"""
Download a report's PBIX from the Power BI export API and extract the
page -> physical table mapping by parsing SourceRef.Entity values from
each visual's query definition.  Also parses DAX measure expressions from
the semantic model (DataModelSchema BIM JSON or TMDL files) so that pages
whose visuals only reference measure-group tables (e.g. "Broker Measures")
still get their underlying physical warehouse tables.

Returns {page_internal_id: set[physical_table_name.lower()]} where
physical_table_name matches the semantic model table name (= physical
warehouse table name in Direct Lake / DirectQuery).
"""
import io
import json
import re
import zipfile
from typing import Optional

import requests

from .auth import get_token
from .config import FABRIC_RESOURCE

PBI_API_BASE = 'https://api.powerbi.com/v1.0/myorg'

# Matches  TableName[  or  'Table Name'[  in DAX
_TABLE_COL_RE = re.compile(r"'([^']+)'\s*\[|(\b[A-Za-z_][A-Za-z0-9_ ]*)\[")


def _pbi_headers() -> dict:
    return {'Authorization': f'Bearer {get_token(FABRIC_RESOURCE)}'}


def _extract_entities(obj) -> set[str]:
    """Recursively find all SourceRef.Entity values in a parsed JSON object."""
    entities: set[str] = set()
    if isinstance(obj, dict):
        if 'SourceRef' in obj and isinstance(obj['SourceRef'], dict):
            entity = obj['SourceRef'].get('Entity')
            if entity:
                entities.add(entity)
        for v in obj.values():
            entities |= _extract_entities(v)
    elif isinstance(obj, list):
        for item in obj:
            entities |= _extract_entities(item)
    return entities


def _extract_dax_table_refs(expr: str, known_tables: set[str]) -> set[str]:
    """
    Find physical table names referenced in a DAX expression.

    Handles two patterns:
    - Column references:  TableName[col]  or  'Table Name'[col]
    - Function arguments: COUNTROWS(table), FILTER(table, ...), ALL(table), etc.
    """
    refs: set[str] = set()

    # Pattern 1: TableName[ or 'Table Name'[
    for m in _TABLE_COL_RE.finditer(expr):
        name = (m.group(1) or m.group(2) or '').strip()
        if name.lower() in known_tables:
            refs.add(name.lower())

    # Pattern 2: table name used as a standalone identifier (no [ following)
    expr_lower = expr.lower()
    for tbl in known_tables:
        if re.search(
            r'(?<![a-z0-9_\'"])' + re.escape(tbl) + r'(?![a-z0-9_\'"\[])',
            expr_lower,
        ):
            refs.add(tbl)

    return refs


def _parse_measure_deps(
    zf: zipfile.ZipFile,
    known_tables: set[str],
) -> dict[str, set[str]]:
    """
    Returns {measure_table_name.lower(): set[physical_table_name.lower()]}
    by parsing DAX measure expressions from the semantic model embedded in
    the PBIX zip.

    Tries two locations:
    1. DataModelSchema  — legacy BIM JSON (UTF-16-LE encoded, always present in
       traditional PBIX exports)
    2. SemanticModel/**/*.tmdl  — TMDL measure files (PBIP-style exports)
    """
    deps: dict[str, set[str]] = {}
    names = set(zf.namelist())

    # --- Strategy 1: DataModelSchema (BIM JSON) ---
    if 'DataModelSchema' in names:
        schema = None
        for enc in ('utf-16-le', 'utf-8-sig', 'utf-8'):
            try:
                schema = json.loads(zf.read('DataModelSchema').decode(enc))
                break
            except Exception:
                continue
        if schema:
            model = schema.get('model', schema)
            for table in model.get('tables', []):
                tname = table.get('name', '')
                for measure in table.get('measures', []):
                    expr = measure.get('expression', '')
                    if isinstance(expr, list):
                        expr = '\n'.join(expr)
                    refs = _extract_dax_table_refs(str(expr), known_tables)
                    if refs:
                        deps.setdefault(tname.lower(), set()).update(refs)

    # --- Strategy 2: TMDL measure files (PBIP format) ---
    if not deps:
        for path in names:
            if not (path.endswith('.tmdl') and '/measures/' in path):
                continue
            try:
                parts = path.replace('\\', '/').split('/')
                tidx = next(i for i, p in enumerate(parts) if p == 'tables')
                tname = parts[tidx + 1]
                if tname.lower() in known_tables:
                    continue  # physical table — its own measures handled via direct refs
                content = zf.read(path).decode('utf-8', errors='replace')
                refs = _extract_dax_table_refs(content, known_tables)
                if refs:
                    deps.setdefault(tname.lower(), set()).update(refs)
            except (StopIteration, IndexError):
                continue

    return deps


def parse_pbix_page_tables(
    workspace_id: str,
    report_id: str,
    known_tables: Optional[set[str]] = None,
) -> dict[str, set[str]]:
    """
    Downloads the PBIX for the given report and returns
    {page_internal_id: set[physical_table_name.lower()]}.

    For each page, physical table coverage comes from two sources:
    - Direct refs: SourceRef.Entity values that ARE physical table names
    - Measure-indirect refs: SourceRef.Entity values that are measure-group
      tables (e.g. "Broker Measures") — expanded to their physical dependencies
      via DAX parsing of the semantic model

    known_tables: set of lowercase physical table names from the warehouse.
    Returns empty dict if the export fails or the file is not parseable.
    """
    r = requests.get(
        f'{PBI_API_BASE}/groups/{workspace_id}/reports/{report_id}/Export',
        headers=_pbi_headers(),
        stream=True,
    )
    if r.status_code != 200:
        return {}

    content = b''.join(r.iter_content(8192))
    try:
        zf = zipfile.ZipFile(io.BytesIO(content))
    except zipfile.BadZipFile:
        return {}

    names = set(zf.namelist())
    pages_meta_path = 'Report/definition/pages/pages.json'
    if pages_meta_path not in names:
        zf.close()
        return {}

    pages_meta = json.loads(zf.read(pages_meta_path))
    page_order = pages_meta.get('pageOrder', [])

    # Collect raw SourceRef.Entity values per page (may include measure-group names)
    raw_result: dict[str, set[str]] = {}
    for page_id in page_order:
        page_entities: set[str] = set()
        visual_prefix = f'Report/definition/pages/{page_id}/visuals/'
        for path in names:
            if path.startswith(visual_prefix) and path.endswith('/visual.json'):
                visual = json.loads(zf.read(path))
                page_entities |= _extract_entities(visual)
        raw_result[page_id] = page_entities

    if known_tables is None:
        zf.close()
        return raw_result

    # Parse measure deps to expand measure-group refs → physical tables
    measure_deps = _parse_measure_deps(zf, known_tables)
    zf.close()

    result: dict[str, set[str]] = {}
    for page_id, entities in raw_result.items():
        physical: set[str] = set()
        for entity in entities:
            entity_lower = entity.lower()
            if entity_lower in known_tables:
                physical.add(entity_lower)
            elif entity_lower in measure_deps:
                physical |= measure_deps[entity_lower]
        result[page_id] = physical

    return result
