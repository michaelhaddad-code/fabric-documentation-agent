"""
Fabric REST API and OneLake client.
All HTTP calls go through here; no business logic.
"""
import base64
import json
import time
from typing import Optional
import requests

from .auth import get_token
from .config import FABRIC_API_BASE, FABRIC_RESOURCE, ONELAKE_BLOB_BASE, STORAGE_RESOURCE

PBI_API_BASE = 'https://api.powerbi.com/v1.0/myorg'


def _fab_headers() -> dict:
    return {'Authorization': f'Bearer {get_token(FABRIC_RESOURCE)}'}


def _pbi_headers() -> dict:
    return {'Authorization': f'Bearer {get_token(FABRIC_RESOURCE)}'}


def _stor_headers() -> dict:
    return {
        'Authorization': f'Bearer {get_token(STORAGE_RESOURCE)}',
        'x-ms-version': '2020-06-12',
    }


def _poll_lro(location: str, max_wait: int = 60) -> Optional[dict]:
    """Poll a Fabric long-running operation URL until it completes."""
    deadline = time.time() + max_wait
    while time.time() < deadline:
        time.sleep(3)
        r = requests.get(location, headers=_fab_headers())
        r.raise_for_status()
        body = r.json()
        status = body.get('status', '')
        if status == 'Succeeded':
            return body
        if status in ('Failed', 'Cancelled'):
            raise RuntimeError(f'LRO {status}: {body.get("error")}')
    return None


# ---------------------------------------------------------------------------
# Workspaces
# ---------------------------------------------------------------------------

def list_workspaces() -> list[dict]:
    r = requests.get(f'{FABRIC_API_BASE}/workspaces', headers=_fab_headers())
    r.raise_for_status()
    return r.json().get('value', [])


def find_workspace(name_or_id: str) -> dict:
    for ws in list_workspaces():
        if ws['id'] == name_or_id or ws['displayName'] == name_or_id:
            return ws
    raise ValueError(f'Workspace not found: {name_or_id!r}')


# ---------------------------------------------------------------------------
# Items
# ---------------------------------------------------------------------------

def list_items(workspace_id: str) -> list[dict]:
    r = requests.get(
        f'{FABRIC_API_BASE}/workspaces/{workspace_id}/items',
        headers=_fab_headers(),
    )
    r.raise_for_status()
    return r.json().get('value', [])


def get_lakehouse(workspace_id: str, lakehouse_id: str) -> dict:
    r = requests.get(
        f'{FABRIC_API_BASE}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}',
        headers=_fab_headers(),
    )
    r.raise_for_status()
    return r.json()


def get_warehouse(workspace_id: str, warehouse_id: str) -> dict:
    r = requests.get(
        f'{FABRIC_API_BASE}/workspaces/{workspace_id}/warehouses/{warehouse_id}',
        headers=_fab_headers(),
    )
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------------------
# Item definitions (source code / pipeline JSON)
# ---------------------------------------------------------------------------

def get_notebook_definition(workspace_id: str, notebook_id: str) -> Optional[list[dict]]:
    """
    Returns list of parts (each with 'path' and 'payload' as base64) or None
    if the API returns an empty definition (common for UI-created notebooks).
    """
    r = requests.post(
        f'{FABRIC_API_BASE}/workspaces/{workspace_id}/notebooks/{notebook_id}/getDefinition',
        headers=_fab_headers(),
    )
    if r.status_code == 200:
        parts = r.json().get('definition', {}).get('parts', [])
        return parts if parts else None
    if r.status_code == 202:
        location = r.headers.get('Location') or r.headers.get('location')
        if not location:
            return None
        result = _poll_lro(location)
        if not result:
            return None
        parts = result.get('definition', {}).get('parts', [])
        return parts if parts else None
    return None


def get_pipeline_definition(workspace_id: str, pipeline_id: str) -> Optional[dict]:
    """Returns the decoded pipeline-content.json dict, or None."""
    r = requests.post(
        f'{FABRIC_API_BASE}/workspaces/{workspace_id}/dataPipelines/{pipeline_id}/getDefinition',
        headers=_fab_headers(),
    )
    if r.status_code not in (200, 202):
        return None
    if r.status_code == 202:
        location = r.headers.get('Location') or r.headers.get('location')
        result = _poll_lro(location) if location else None
        parts = result.get('definition', {}).get('parts', []) if result else []
    else:
        parts = r.json().get('definition', {}).get('parts', [])

    for part in parts:
        if part.get('path') == 'pipeline-content.json':
            raw = base64.b64decode(part['payload']).decode('utf-8')
            return json.loads(raw)
    return None


def get_dataflow_definition(workspace_id: str, item_id: str) -> Optional[str]:
    """Returns decoded M query string (mashup.pq), or None."""
    r = requests.post(
        f'{FABRIC_API_BASE}/workspaces/{workspace_id}/items/{item_id}/getDefinition',
        headers=_fab_headers(),
    )
    if r.status_code == 202:
        location = r.headers.get('Location') or r.headers.get('location')
        result = _poll_lro(location) if location else None
        parts = result.get('definition', {}).get('parts', []) if result else []
    elif r.status_code == 200:
        parts = r.json().get('definition', {}).get('parts', [])
    else:
        return None

    for part in parts:
        if 'mashup' in part.get('path', '').lower() or part.get('path', '').endswith('.pq'):
            return base64.b64decode(part['payload']).decode('utf-8')
    return None


# ---------------------------------------------------------------------------
# OneLake file discovery (bronze Files directory)
# ---------------------------------------------------------------------------

def list_bronze_files(workspace_id: str, lakehouse_id: str) -> list[dict]:
    """
    List files under {lakehouse}/Files/ using the OneLake blob endpoint.
    Returns list of {name, size, last_modified} dicts for non-directory blobs.
    """
    url = (
        f'{ONELAKE_BLOB_BASE}/{workspace_id}'
        f'?restype=container&comp=list'
        f'&prefix={lakehouse_id}/Files/'
        f'&delimiter=%2F'
    )
    r = requests.get(url, headers=_stor_headers())
    if r.status_code != 200:
        return []

    # Response is XML — parse it simply
    import xml.etree.ElementTree as ET
    try:
        root = ET.fromstring(r.text)
    except ET.ParseError:
        return []

    files = []
    ns = ''
    for blob in root.findall(f'{ns}Blobs/{ns}Blob'):
        name_el = blob.find(f'{ns}Name')
        props = blob.find(f'{ns}Properties')
        if name_el is None or props is None:
            continue
        size_el = props.find(f'{ns}Content-Length')
        lm_el = props.find(f'{ns}Last-Modified')
        size = int(size_el.text) if size_el is not None and size_el.text else 0
        if size == 0:
            continue  # skip placeholder blobs
        files.append({
            'name': name_el.text,
            'size': size,
            'last_modified': lm_el.text if lm_el is not None else '',
        })

    # Also recurse one level with a broader prefix (no delimiter) to catch nested files
    if not files:
        url2 = (
            f'{ONELAKE_BLOB_BASE}/{workspace_id}'
            f'?restype=container&comp=list'
            f'&prefix={lakehouse_id}/Files/'
        )
        r2 = requests.get(url2, headers=_stor_headers())
        if r2.status_code == 200:
            try:
                root2 = ET.fromstring(r2.text)
            except ET.ParseError:
                return []
            for blob in root2.findall(f'{ns}Blobs/{ns}Blob'):
                name_el = blob.find(f'{ns}Name')
                props = blob.find(f'{ns}Properties')
                if name_el is None or props is None:
                    continue
                size_el = props.find(f'{ns}Content-Length')
                lm_el = props.find(f'{ns}Last-Modified')
                size = int(size_el.text) if size_el is not None and size_el.text else 0
                if size == 0:
                    continue
                files.append({
                    'name': name_el.text,
                    'size': size,
                    'last_modified': lm_el.text if lm_el is not None else '',
                })

    return files


# ---------------------------------------------------------------------------
# Power BI reports and pages
# ---------------------------------------------------------------------------

def list_reports(workspace_id: str) -> list[dict]:
    """Returns Power BI reports in the workspace, each with id, name, datasetId."""
    r = requests.get(f'{PBI_API_BASE}/groups/{workspace_id}/reports', headers=_pbi_headers())
    if r.status_code != 200:
        return []
    return r.json().get('value', [])


def get_report_pages(workspace_id: str, report_id: str) -> list[dict]:
    """Returns pages for a report: [{name, displayName, order}], sorted by order."""
    r = requests.get(
        f'{PBI_API_BASE}/groups/{workspace_id}/reports/{report_id}/pages',
        headers=_pbi_headers(),
    )
    if r.status_code != 200:
        return []
    pages = r.json().get('value', [])
    return sorted(pages, key=lambda p: p.get('order', 0))


def create_warehouse(workspace_id: str, display_name: str) -> dict:
    """
    Creates a Fabric Warehouse and waits for provisioning to complete.
    Returns the warehouse item dict (id, displayName, type, properties).
    """
    r = requests.post(
        f'{FABRIC_API_BASE}/workspaces/{workspace_id}/warehouses',
        headers=_fab_headers(),
        json={'displayName': display_name},
    )
    if r.status_code == 201:
        return r.json()
    if r.status_code == 202:
        location = r.headers.get('Location') or r.headers.get('location')
        result = _poll_lro(location, max_wait=120) if location else None
        if result:
            # LRO result may contain the item or we need to fetch it
            item_id = result.get('id') or (result.get('createdArtifact') or {}).get('id')
            if item_id:
                r2 = requests.get(
                    f'{FABRIC_API_BASE}/workspaces/{workspace_id}/warehouses/{item_id}',
                    headers=_fab_headers(),
                )
                if r2.status_code == 200:
                    return r2.json()
        # Fall back: find the newly created warehouse by name
        for item in list_items(workspace_id):
            if item.get('type') == 'Warehouse' and item.get('displayName') == display_name:
                r3 = requests.get(
                    f'{FABRIC_API_BASE}/workspaces/{workspace_id}/warehouses/{item["id"]}',
                    headers=_fab_headers(),
                )
                if r3.status_code == 200:
                    return r3.json()
    raise RuntimeError(f'Failed to create warehouse "{display_name}": {r.status_code} {r.text[:200]}')


def get_dataset_source_item_id(workspace_id: str, dataset_id: str) -> Optional[str]:
    """
    Returns the Fabric item ID (warehouse/lakehouse) that backs a semantic model.
    Handles both Direct Lake (AzureDataLakeStorage path) and DirectQuery (Sql database GUID).
    """
    r = requests.get(
        f'{PBI_API_BASE}/groups/{workspace_id}/datasets/{dataset_id}/datasources',
        headers=_pbi_headers(),
    )
    if r.status_code != 200:
        return None
    for ds in r.json().get('value', []):
        ds_type = ds.get('datasourceType', '')
        details = ds.get('connectionDetails', {})
        if ds_type == 'AzureDataLakeStorage':
            # path is /workspace_id/item_id/
            parts = [p for p in details.get('path', '').split('/') if p]
            if len(parts) >= 2:
                return parts[1]
        elif ds_type == 'Sql':
            # database field is the warehouse item ID for Fabric SQL endpoints
            return details.get('database')
    return None
