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
import base64
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


def fetch_sm_measure_deps(
    workspace_id: str,
    dataset_id: str,
    known_tables: set[str],
) -> dict[str, set[str]]:
    """
    Queries the dataset via the Power BI executeQueries API using DAX INFO functions
    to get all measure expressions, then returns
    {measure_table_lower: set[physical_table_lower]}.

    Uses INFO.TABLES() to resolve table IDs to names and INFO.MEASURES() to get
    DAX expressions. Returns empty dict if the API is unavailable.
    """
    from .fabric_api import execute_dataset_query

    # Get table ID -> name mapping
    tbl_rows = execute_dataset_query(
        workspace_id, dataset_id,
        'EVALUATE SELECTCOLUMNS(INFO.TABLES(), "tid", [ID], "tname", [Name])',
    )
    if not tbl_rows:
        return {}
    id_to_name = {row.get('[tid]'): row.get('[tname]', '') for row in tbl_rows}

    # Get measure expressions
    meas_rows = execute_dataset_query(
        workspace_id, dataset_id,
        'EVALUATE SELECTCOLUMNS(INFO.MEASURES(), "tid", [TABLEID], "expr", [EXPRESSION])',
    )
    if not meas_rows:
        return {}

    deps: dict[str, set[str]] = {}
    for row in meas_rows:
        tname = id_to_name.get(row.get('[tid]'), '')
        if not tname or tname.lower() in known_tables:
            continue
        expr = row.get('[expr]') or ''
        refs = _extract_dax_table_refs(str(expr), known_tables)
        if refs:
            deps.setdefault(tname.lower(), set()).update(refs)

    return deps


def parse_sm_measure_deps(
    parts: list[dict],
    known_tables: set[str],
) -> dict[str, set[str]]:
    """
    Parse TMDL definition parts from a semantic model (returned by
    get_semantic_model_definition) and return
    {measure_table_name.lower(): set[physical_table_name.lower()]}.

    Only processes table files that are NOT physical tables (i.e. measure groups).
    """
    deps: dict[str, set[str]] = {}
    for part in parts:
        path = part.get('path', '').replace('\\', '/')
        if not (path.endswith('.tmdl') and '/tables/' in path):
            continue
        # Skip sub-paths like /tables/Foo/measures/Bar.tmdl — the table-level
        # file (definition/tables/Foo.tmdl) contains all measures inline
        path_parts = path.split('/')
        try:
            tidx = next(i for i, p in enumerate(path_parts) if p == 'tables')
        except StopIteration:
            continue
        # Only process direct children of tables/ (not deeper sub-paths)
        if len(path_parts) != tidx + 2:
            continue
        fname = path_parts[tidx + 1]
        tname = fname[:-5] if fname.endswith('.tmdl') else fname
        if tname.lower() in known_tables:
            continue  # physical table — skip
        try:
            content = base64.b64decode(part['payload']).decode('utf-8', errors='replace')
        except Exception:
            continue
        refs = _extract_dax_table_refs(content, known_tables)
        if refs:
            deps[tname.lower()] = refs
    return deps


def _parse_pbip_page_tables(
    parts: list[dict],
    known_tables: Optional[set[str]],
    measure_deps: Optional[dict[str, set[str]]] = None,
) -> dict[str, set[str]]:
    """
    Parse PBIP definition parts returned by get_report_definition and return
    {page_internal_id: set[physical_table_name.lower()]}.

    Handles paths in both PBIP styles:
    - definition/pages/{pageId}/visuals/{visualId}/visual.json
    - Report/definition/pages/{pageId}/visuals/{visualId}/visual.json
    """
    parts_map = {p.get('path', '').replace('\\', '/'): p for p in parts}

    # Find page order from pages.json
    page_order: list[str] = []
    for path in parts_map:
        if path.endswith('pages.json'):
            try:
                content = base64.b64decode(parts_map[path]['payload']).decode('utf-8', errors='replace')
                page_order = json.loads(content).get('pageOrder', [])
            except Exception:
                pass
            break

    # Collect SourceRef.Entity values per page from visual.json files
    raw_result: dict[str, set[str]] = {}
    for path, part in parts_map.items():
        if not path.endswith('/visual.json'):
            continue
        segs = path.split('/')
        try:
            vidx = next(i for i, s in enumerate(segs) if s == 'visuals')
            page_id = segs[vidx - 1]
        except (StopIteration, IndexError):
            continue
        try:
            content = base64.b64decode(part['payload']).decode('utf-8', errors='replace')
            raw_result.setdefault(page_id, set()).update(_extract_entities(json.loads(content)))
        except Exception:
            continue

    if not raw_result:
        return {}

    if known_tables is None:
        return raw_result

    # Derive measure deps from SM TMDL parts bundled in the definition
    if measure_deps is None:
        measure_deps = parse_sm_measure_deps(parts, known_tables)

    result: dict[str, set[str]] = {}
    for page_id in (page_order if page_order else raw_result.keys()):
        entities = raw_result.get(page_id, set())
        physical: set[str] = set()
        for entity in entities:
            el = entity.lower()
            if el in known_tables:
                physical.add(el)
            elif el in measure_deps:
                physical |= measure_deps[el]
        result[page_id] = physical

    return result


def parse_pbix_page_tables(
    workspace_id: str,
    report_id: str,
    known_tables: Optional[set[str]] = None,
    measure_deps: Optional[dict[str, set[str]]] = None,
) -> dict[str, set[str]]:
    """
    Returns {page_internal_id: set[physical_table_name.lower()]} for the given report.

    Strategy:
    1. Try Fabric getDefinition (PBIP format) — works for PBIP-native reports and
       includes SM TMDL files so measure-group pages resolve to physical tables.
    2. Fall back to the Power BI Export API (PBIX) — for PBIX-based reports; uses
       DataModelSchema or the pre-computed measure_deps for measure resolution.

    measure_deps: pre-computed {measure_table_lower: set[physical_table_lower]}.
      Passed as fallback; if None the function derives deps from the definition itself.
    known_tables: lowercase physical table names from the warehouse.
    Returns empty dict if both strategies fail.
    """
    from .fabric_api import get_report_definition

    # --- Strategy 1: PBIP via getDefinition ---
    parts = get_report_definition(workspace_id, report_id)
    if parts:
        tmdl_count = sum(1 for p in parts if p.get('path', '').endswith('.tmdl'))
        visual_count = sum(1 for p in parts if p.get('path', '').endswith('/visual.json'))
        print(f'    [PBIP] {len(parts)} parts ({visual_count} visual.json, {tmdl_count} .tmdl)')
        result = _parse_pbip_page_tables(parts, known_tables, measure_deps)
        if result:
            return result
        print(f'    [PBIP] no pages found in parts — falling back to PBIX export')
    else:
        print(f'    [PBIP] getDefinition returned no parts — using PBIX export')

    # --- Strategy 2: PBIX Export fallback ---
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

    # --- New PBIP-style PBIX (Report/definition/pages/pages.json) ---
    pages_meta_path = 'Report/definition/pages/pages.json'
    if pages_meta_path in names:
        pages_meta = json.loads(zf.read(pages_meta_path))
        page_order = pages_meta.get('pageOrder', [])

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

        if measure_deps is None:
            measure_deps = _parse_measure_deps(zf, known_tables)
        zf.close()

        result: dict[str, set[str]] = {}
        for page_id, entities in raw_result.items():
            physical: set[str] = set()
            for entity in entities:
                el = entity.lower()
                if el in known_tables:
                    physical.add(el)
                elif el in measure_deps:
                    physical |= measure_deps[el]
            result[page_id] = physical
        return result

    # --- Legacy PBIX format (Report/Layout — pre-2024 single-file layout) ---
    if 'Report/Layout' in names:
        try:
            layout = json.loads(zf.read('Report/Layout').decode('utf-16-le'))
        except Exception:
            zf.close()
            return {}
        zf.close()

        raw_result = {}
        for section in layout.get('sections', []):
            page_id = section.get('name', '')
            if not page_id:
                continue
            entities: set[str] = set()
            for vc in section.get('visualContainers', []):
                try:
                    cfg = json.loads(vc.get('config', '{}'))
                    frm = cfg.get('singleVisual', {}).get('prototypeQuery', {}).get('From', [])
                    for item in frm:
                        e = item.get('Entity', '')
                        if e:
                            entities.add(e)
                except Exception:
                    pass
            raw_result[page_id] = entities

        if known_tables is None:
            return raw_result

        result = {}
        md = measure_deps or {}
        for page_id, entities in raw_result.items():
            physical: set[str] = set()
            for entity in entities:
                el = entity.lower()
                if el in known_tables:
                    physical.add(el)
                elif el in md:
                    physical |= md[el]
            result[page_id] = physical
        return result

    zf.close()
    return {}
