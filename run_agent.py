"""
Fabric Documentation Agent
Usage:
    python run_agent.py --workspace "XP3R-R&D"
    python run_agent.py --workspace "XP3R-R&D" --dry-run
    python run_agent.py --workspace "XP3R-R&D" --output my_output.xlsx
    python run_agent.py --workspace "XP3R-R&D" --notebooks ./notebooks
"""
import argparse
import pickle
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import requests

from agent.config import (
    TEMPLATE_PATH, NOTEBOOKS_DIR, IN_SCOPE_TYPES, ARTIFACT_TAB_TYPES,
    SQL_ENDPOINT_TYPES, detect_layer,
)
from agent.models import (
    ArtifactInfo, LakehouseInfo, SqlEndpoint,
    TableInfo, ColumnInfo, DataSourceRow, LineageEdge,
)
from agent.auth import get_token
from agent.fabric_api import (
    find_workspace, list_items, get_lakehouse, get_warehouse,
    get_pipeline_definition, list_bronze_files,
    list_reports, get_report_pages, get_dataset_source_item_id,
    create_warehouse, get_semantic_model_definition, get_report_definition,
)
from agent.pbix_parser import (
    parse_pbix_page_tables, parse_sm_measure_deps, fetch_sm_measure_deps,
    fetch_sm_measure_deps_xmla, build_pbip_zip_index, parse_pbip_zip_page_tables,
    parse_pbip_zip_visuals,
)
from agent.sql_api import SQLClient
from agent.lineage import build_lineage, parse_pipeline, print_lineage_summary
from agent.writer import write_output
from agent.governance_writer import populate_governance, provision_governance_warehouse, read_measure_deps


MEASURE_DEPS_NOTEBOOK_ID = '85f484cb-056e-4662-94bb-80bee0316840'
FABRIC_API = 'https://api.fabric.microsoft.com/v1'


def trigger_measure_deps_notebook(workspace_id: str) -> bool:
    """
    Trigger nb_extract_measure_deps in Fabric and wait for it to complete.
    Returns True on success, False on failure (agent continues either way).
    """
    print('\nTriggering nb_extract_measure_deps notebook...')
    try:
        token = get_token('https://api.fabric.microsoft.com')
        hdrs = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

        r = requests.post(
            f'{FABRIC_API}/workspaces/{workspace_id}/items/{MEASURE_DEPS_NOTEBOOK_ID}'
            '/jobs/instances?jobType=RunNotebook',
            json={}, headers=hdrs,
        )
        if r.status_code != 202:
            print(f'  WARNING: notebook trigger returned {r.status_code} — continuing with existing measure_deps')
            return False

        instance_id = r.headers.get('Location', '').rstrip('/').split('/')[-1]
        if not instance_id:
            print('  WARNING: could not get job instance ID — continuing with existing measure_deps')
            return False

        print(f'  Job instance: {instance_id} — polling...')
        poll_url = (
            f'{FABRIC_API}/workspaces/{workspace_id}/items/{MEASURE_DEPS_NOTEBOOK_ID}'
            f'/jobs/instances/{instance_id}'
        )
        for i in range(40):  # max 10 minutes
            time.sleep(15)
            token = get_token('https://api.fabric.microsoft.com')
            hdrs['Authorization'] = f'Bearer {token}'
            pr = requests.get(poll_url, headers=hdrs)
            if pr.status_code != 200:
                continue
            status = pr.json().get('status', '').lower()
            elapsed = (i + 1) * 15
            print(f'  {elapsed}s: {status}')
            if status == 'completed':
                print('  Notebook completed — measure_deps refreshed.')
                return True
            if status in ('failed', 'cancelled', 'deduped'):
                msg = pr.json().get('failureReason') or pr.json().get('error') or ''
                print(f'  WARNING: notebook {status}: {msg}')
                print('  Continuing with existing measure_deps data.')
                return False

        print('  WARNING: notebook timed out — continuing with existing measure_deps')
        return False

    except Exception as e:
        print(f'  WARNING: notebook trigger failed ({e}) — continuing with existing measure_deps')
        return False


def parse_args():
    p = argparse.ArgumentParser(
        description='Document a Fabric workspace into wh_governance and optionally an Excel file.'
    )
    p.add_argument('--workspace', required=True, help='Workspace name or ID')
    p.add_argument('--dry-run', action='store_true', help='Print summary; do not write anything')
    p.add_argument('--excel', action='store_true', help='Also write an Excel .xlsx documentation file')
    p.add_argument('--output', help='Output .xlsx path (implies --excel; default: fabric_doc_<workspace>_<date>.xlsx)')
    p.add_argument('--notebooks', default=str(NOTEBOOKS_DIR), help='Directory for local .ipynb fallback files')
    p.add_argument(
        '--layers', default=None,
        help='Comma-separated list of layers to include in Tables/Columns/Lineage '
             '(e.g. bronze,silver,gold). Unknown-layer items still appear in Artifacts tab. '
             'Default: include all layers.',
    )
    p.add_argument(
        '--pbip-zip', default=None,
        help='Path to a PBIP ZIP exported from PBI Desktop (e.g. PBIP_Reports_for_Michael.zip). '
             'Reports found in the ZIP are parsed locally instead of downloading PBIX from Fabric.',
    )
    p.add_argument(
        '--pbip-zip-only', action='store_true',
        help='Only process reports whose semantic model ID appears in the PBIP ZIP. '
             'Reports not in the ZIP are skipped entirely. Requires --pbip-zip.',
    )
    p.add_argument(
        '--refresh-measure-deps', action='store_true',
        help='Trigger the nb_extract_measure_deps Fabric notebook before reading measure_deps. '
             'By default the notebook is NOT triggered — run it manually in the Fabric UI instead.',
    )
    p.add_argument(
        '--relationships-only', action='store_true',
        help='Skip the objects insert (objects table already populated) and only rebuild '
             'data_lineage.relationships. Use to resume after a mid-run token expiry failure.',
    )
    return p.parse_args()


def make_artifact(item: dict, workspace_name: str) -> ArtifactInfo:
    props = item.get('properties', {})
    created_by = ''
    if isinstance(props.get('createdBy'), dict):
        created_by = props['createdBy'].get('userPrincipalName', '') or props['createdBy'].get('displayName', '')
    last_modified = props.get('lastModifiedAt', props.get('modifiedDateTime', ''))
    return ArtifactInfo(
        id=item['id'],
        display_name=item['displayName'],
        type=item['type'],
        workspace_id=item['workspaceId'],
        workspace_name=workspace_name,
        description=item.get('description', ''),
        last_modified=last_modified,
        created_by=created_by,
        layer=detect_layer(item['displayName'], item.get('description', '')),
    )


def make_lakehouse(item: dict, detail: dict, workspace_name: str) -> LakehouseInfo:
    art = make_artifact(item, workspace_name)
    props = detail.get('properties', {})
    sql_ep_props = props.get('sqlEndpointProperties', {})
    conn_str = sql_ep_props.get('connectionString', '') or props.get('connectionString', '')
    sql_ep = SqlEndpoint(
        connection_string=conn_str,
        endpoint_id=sql_ep_props.get('id', ''),
    ) if conn_str else None
    lh = LakehouseInfo(
        **{k: v for k, v in art.__dict__.items()},
        sql_endpoint=sql_ep,
        tables_onelake_path=props.get('oneLakeTablesPath', ''),
        files_onelake_path=props.get('oneLakeFilesPath', ''),
    )
    return lh


def collect_workspace(workspace_id: str, workspace_name: str, notebooks_dir: Path, layer_filter: set[str] | None = None):
    """
    Collect all data from the workspace. Returns structured objects plus the
    wh_governance SQL endpoint connection string (or None if not found).
    wh_governance is excluded from the documentation artifacts.
    """

    print(f'\n[1/5] Listing items in workspace "{workspace_name}"...')
    raw_items = list_items(workspace_id)
    in_scope = [i for i in raw_items if i.get('type') in IN_SCOPE_TYPES]
    print(f'      Found {len(in_scope)} in-scope items ({len(raw_items)} total)')

    # Add workspaceId to each item (API omits it from list responses)
    for item in in_scope:
        item.setdefault('workspaceId', workspace_id)

    lakehouses: list[LakehouseInfo] = []
    all_artifacts: list[ArtifactInfo] = []
    raw_notebooks: list[dict] = []
    raw_pipelines: list[dict] = []
    gov_server: str | None = None

    print('\n[2/5] Fetching artifact details...')
    for item in in_scope:
        itype = item['type']
        art = make_artifact(item, workspace_name)

        if itype == 'Lakehouse':
            detail = get_lakehouse(workspace_id, item['id'])
            lh = make_lakehouse(item, detail, workspace_name)
            lakehouses.append(lh)
            if itype in ARTIFACT_TAB_TYPES:
                all_artifacts.append(lh)
            print(f'      Lakehouse: {lh.display_name}  layer={lh.layer}  sql_ep={bool(lh.sql_endpoint)}')

        elif itype == 'Warehouse' and art.display_name == 'wh_governance':
            # Capture the governance warehouse endpoint but exclude it from documentation
            detail = get_warehouse(workspace_id, item['id'])
            lh = make_lakehouse(item, detail, workspace_name)
            if lh.sql_endpoint:
                gov_server = lh.sql_endpoint.connection_string
            print(f'      Warehouse: {lh.display_name}  [governance target, excluded from docs]')
            continue

        elif itype == 'Warehouse':
            # Warehouses have similar structure to lakehouses but different endpoint
            detail = get_warehouse(workspace_id, item['id'])
            lh = make_lakehouse(item, detail, workspace_name)
            lakehouses.append(lh)
            all_artifacts.append(lh)
            print(f'      Warehouse: {lh.display_name}  layer={lh.layer}')

        elif itype == 'Notebook':
            raw_notebooks.append(item)
            all_artifacts.append(art)
            print(f'      Notebook:  {art.display_name}')

        elif itype in ('DataflowGen2', 'Dataflow'):
            all_artifacts.append(art)
            print(f'      Dataflow:  {art.display_name}')

        elif itype == 'DataPipeline':
            raw_pipelines.append(item)
            # Pipelines don't appear in Artifacts tab

    print('\n[3/5] Querying tables, schema, and stats...')
    all_tables: list[TableInfo] = []
    all_columns: list[ColumnInfo] = []

    TABLE_WORKERS = 8  # parallel connections per lakehouse/warehouse

    def _fetch_table(conn_str: str, lh_display: str, lh_id: str, lh_layer: str,
                     lh_desc: str, traw: dict) -> tuple:
        """Query one table's columns, row count, stats, and last-updated. Own connection."""
        schema = traw['schema']
        tname  = traw['table_name']
        c = SQLClient(conn_str, lh_display)
        try:
            cols         = c.get_columns(schema, tname)
            col_names    = [col['col_name'] for col in cols]
            row_count    = c.get_row_count(schema, tname)
            stats        = c.get_column_stats(schema, tname, col_names)
            last_updated = c.get_last_updated(schema, tname, col_names)
        finally:
            c.close()
        tbl = TableInfo(
            table_name=tname, schema_name=schema,
            lh_name=lh_display, lh_id=lh_id, layer=lh_layer,
            workspace_name=workspace_name, description=lh_desc,
            row_count=row_count, last_updated=last_updated,
        )
        col_objs = []
        for col in cols:
            s = stats.get(col['col_name'], {})
            col_objs.append(ColumnInfo(
                lh_name=lh_display, table_name=tname,
                col_name=col['col_name'], datatype=col['datatype'],
                is_nullable=col['is_nullable'],
                sample_value=s.get('sample', ''),
                pct_null=s.get('pct_null', ''),
            ))
        return tbl, col_objs

    for lh in lakehouses:
        if layer_filter and lh.layer not in layer_filter:
            print(f'      {lh.display_name}: skipped (layer={lh.layer}, not in --layers filter)')
            lh._tables = []
            lh._files = []
            continue

        if not lh.sql_endpoint:
            print(f'      {lh.display_name}: no SQL endpoint, skipping schema query')
            lh._tables = []
            continue

        conn_str = lh.sql_endpoint.connection_string
        list_client = SQLClient(conn_str, lh.display_name)
        try:
            raw_tables = list_client.get_tables()
        except Exception as e:
            print(f'      {lh.display_name}: SQL connection failed — {e}')
            lh._tables = []
            continue
        finally:
            list_client.close()

        lh._tables = []
        if not raw_tables:
            print(f'      {lh.display_name}: no registered tables (may be Files-only bronze layer)')
            continue

        print(f'      {lh.display_name}: {len(raw_tables)} table(s) — querying in parallel (workers={TABLE_WORKERS})')

        with ThreadPoolExecutor(max_workers=TABLE_WORKERS) as pool:
            future_map = {
                pool.submit(
                    _fetch_table, conn_str,
                    lh.display_name, lh.id, lh.layer, lh.description, traw
                ): traw
                for traw in raw_tables
            }
            for future in as_completed(future_map):
                traw = future_map[future]
                schema = traw['schema']
                tname  = traw['table_name']
                try:
                    tbl, col_objs = future.result()
                    lh._tables.append(tbl)
                    all_tables.append(tbl)
                    all_columns.extend(col_objs)
                    print(f'        {schema}.{tname}: {tbl.row_count} rows, {len(col_objs)} cols, last_updated={tbl.last_updated or "n/a"}')
                except Exception as e:
                    print(f'        {schema}.{tname}: ERROR — {e}  (skipping)')

    # List bronze files for Data Sources
    for lh in lakehouses:
        if lh.layer == 'Bronze':
            print(f'      Listing bronze files: {lh.display_name}...')
            lh._files = list_bronze_files(workspace_id, lh.id)
            print(f'        Found {len(lh._files)} file(s)')
        else:
            lh._files = []

    print('\n[4/5] Parsing pipeline definitions...')
    pipelines = []
    for raw_p in raw_pipelines:
        print(f'      Pipeline: {raw_p["displayName"]}')
        pdef = get_pipeline_definition(workspace_id, raw_p['id'])
        if pdef:
            p_art = make_artifact(raw_p, workspace_name)
            pip = parse_pipeline(p_art.__dict__, pdef)
            pipelines.append(pip)
            for act in pip.activities:
                nb_name = next(
                    (i['displayName'] for i in raw_notebooks if i['id'] == act.notebook_id),
                    act.notebook_id,
                )
                print(f'        Activity "{act.name}" -> notebook: {nb_name}  depends_on={act.depends_on}')
        else:
            print(f'        Could not retrieve definition')

    print('\n[5/5] Assembling lineage...')
    all_items_as_artifacts = all_artifacts  # includes notebooks and dataflows
    gold_silver, silver_bronze, data_sources = build_lineage(
        workspace_id=workspace_id,
        lakehouses=lakehouses,
        all_items=all_items_as_artifacts,
        pipelines=pipelines,
        notebooks_dir=notebooks_dir,
    )

    # Back-fill source_artifact on TableInfo from lineage edges
    for edge in gold_silver:
        for tbl in all_tables:
            if tbl.table_name.lower() == edge.output_table.lower():
                tbl.source_artifact = edge.notebook_name
    for edge in silver_bronze:
        for tbl in all_tables:
            if tbl.table_name.lower() == edge.output_table.lower():
                tbl.source_artifact = edge.notebook_name

    # Sort tables: Bronze → Silver → Gold (for readability)
    layer_order = {'Bronze': 0, 'Silver': 1, 'Gold': 2, 'Unknown': 3}
    all_tables.sort(key=lambda t: (layer_order.get(t.layer, 9), t.lh_name, t.table_name))

    # Filter artifacts tab to document-relevant types
    doc_artifacts = [a for a in all_artifacts if a.type in ARTIFACT_TAB_TYPES]

    return doc_artifacts, all_tables, all_columns, gold_silver, silver_bronze, data_sources, gov_server


def main():
    args = parse_args()
    notebooks_dir = Path(args.notebooks)
    write_excel = args.excel or bool(args.output)

    layer_filter = (
        {l.strip().capitalize() for l in args.layers.split(',')}
        if args.layers else None
    )

    pbip_zip             = Path(args.pbip_zip) if args.pbip_zip else None
    pbip_zip_only        = args.pbip_zip_only
    refresh_measure_deps = args.refresh_measure_deps
    relationships_only   = args.relationships_only

    print(f'Fabric Documentation Agent')
    print(f'Workspace : {args.workspace}')
    print(f'Dry run   : {args.dry_run}')
    print(f'Excel     : {write_excel}')
    print(f'Notebooks : {notebooks_dir}')
    print(f'Layers    : {", ".join(sorted(layer_filter)) if layer_filter else "all"}')
    print(f'PBIP ZIP  : {pbip_zip or "none"}')

    # Pre-flight auth check
    print('\nChecking auth...')
    try:
        get_token('https://api.fabric.microsoft.com')
        print('  Fabric API token: OK')
    except Exception as e:
        print(f'  ERROR: {e}')
        sys.exit(1)

    ws = find_workspace(args.workspace)
    workspace_id = ws['id']
    workspace_name = ws['displayName']
    print(f'  Workspace ID: {workspace_id}')

    safe_name = workspace_name.replace(' ', '_').replace('/', '_')
    checkpoint_path = Path(f'.cache_{safe_name}.pkl')

    if checkpoint_path.exists():
        print(f'\nLoading cached collection from {checkpoint_path}...')
        with open(checkpoint_path, 'rb') as f:
            cached = pickle.load(f)
        # Support old cache format (6-tuple) and new format (7-tuple with gov_server)
        if len(cached) == 7:
            artifacts, tables, columns, gold_silver, silver_bronze, data_sources, gov_server = cached
        else:
            artifacts, tables, columns, gold_silver, silver_bronze, data_sources = cached
            gov_server = None
        print('  Loaded. Skipping API collection.')
    else:
        artifacts, tables, columns, gold_silver, silver_bronze, data_sources, gov_server = collect_workspace(
            workspace_id, workspace_name, notebooks_dir, layer_filter=layer_filter,
        )
        with open(checkpoint_path, 'wb') as f:
            pickle.dump(
                (artifacts, tables, columns, gold_silver, silver_bronze, data_sources, gov_server), f
            )
        print(f'\nCollection cached to {checkpoint_path}')

    print(f'\nCollection complete: {len(artifacts)} artifacts, {len(tables)} tables, {len(columns)} columns')
    print_lineage_summary(gold_silver, silver_bronze)

    if args.dry_run:
        print('\n[dry-run] No writes.')
        print(f'  Artifacts   : {len(artifacts)}')
        print(f'  Tables      : {len(tables)}')
        print(f'  Columns     : {len(columns)}')
        print(f'  G->S edges  : {len(gold_silver)}')
        print(f'  S->B edges  : {len(silver_bronze)}')
        print(f'  Data Sources: {len(data_sources)}')
        print(f'  wh_governance server: {gov_server or "not found — would be created"}')
        return

    # ------------------------------------------------------------------
    # Excel output (optional)
    # ------------------------------------------------------------------
    if write_excel:
        if args.output:
            output_path = Path(args.output)
        else:
            date_str = datetime.now().strftime('%Y%m%d_%H%M')
            output_path = Path(f'fabric_doc_{safe_name}_{date_str}.xlsx')

        write_output(
            template_path=TEMPLATE_PATH,
            output_path=output_path,
            artifacts=artifacts,
            tables=tables,
            columns=columns,
            gold_silver=gold_silver,
            silver_bronze=silver_bronze,
            data_sources=data_sources,
        )

    # ------------------------------------------------------------------
    # wh_governance — create if missing, provision schema/tables, populate
    # ------------------------------------------------------------------
    if gov_server is None:
        print(f'\nwh_governance not found in cache — creating or locating...')
        try:
            wh_detail = create_warehouse(workspace_id, 'wh_governance')
            print('  Created new wh_governance.')
        except RuntimeError as e:
            if '409' not in str(e) and 'AlreadyInUse' not in str(e):
                raise
            print('  Already exists in workspace — finding connection string...')
            raw_items = list_items(workspace_id)
            gov_item = next(
                (i for i in raw_items
                 if i.get('type') == 'Warehouse' and i.get('displayName') == 'wh_governance'),
                None,
            )
            if not gov_item:
                print('  ERROR: could not find wh_governance in workspace')
                sys.exit(1)
            wh_detail = get_warehouse(workspace_id, gov_item['id'])

        props = wh_detail.get('properties', {})
        gov_server = (
            props.get('connectionString')
            or props.get('sqlEndpointProperties', {}).get('connectionString')
        )
        if not gov_server:
            print('  ERROR: could not retrieve connection string for wh_governance')
            sys.exit(1)
        print(f'  Server: {gov_server}')

        # Update cache so future runs skip this lookup
        with open(checkpoint_path, 'wb') as f:
            pickle.dump(
                (artifacts, tables, columns, gold_silver, silver_bronze, data_sources, gov_server), f
            )
        print('  Cache updated with gov_server.')

    print('\nProvisioning wh_governance schema and tables...')
    provision_governance_warehouse(gov_server)

    print('\nCollecting reports and pages...')
    reports_with_pages = []
    report_warehouse_map: dict[str, str] = {}
    seen_datasets: set[str] = set()
    dataset_names: dict[str, str] = {}  # ds_id -> display name (for XMLA connection)

    # For DirectQuery warehouses the datasources API returns the display name as 'database',
    # not the item GUID. Build a name→id lookup so we can resolve either form.
    known_lh_ids = {tbl.lh_id for tbl in tables}
    lh_name_to_id = {tbl.lh_name.lower(): tbl.lh_id for tbl in tables}

    for rpt in list_reports(workspace_id):
        pages = get_report_pages(workspace_id, rpt['id'])
        reports_with_pages.append({**rpt, 'pages': pages})
        ds_id = rpt.get('datasetId', '')
        ds_name = rpt.get('datasetName', '')
        if ds_id:
            dataset_names[ds_id] = ds_name
        if ds_id and ds_id not in seen_datasets:
            seen_datasets.add(ds_id)
            item_id = get_dataset_source_item_id(workspace_id, ds_id)
            if item_id:
                if item_id not in known_lh_ids:
                    item_id = lh_name_to_id.get(item_id.lower(), item_id)
                report_warehouse_map[ds_id] = item_id
        print(f'  {rpt["name"]}: {len(pages)} pages, source={report_warehouse_map.get(ds_id, "unknown")}')

    print('\nFetching semantic model measure expressions...')
    known_tables = {t.table_name.lower() for t in tables}

    # Read measure_deps from wh_governance (populated manually via nb_extract_measure_deps)
    # To force a notebook refresh pass --refresh-measure-deps
    if refresh_measure_deps:
        trigger_measure_deps_notebook(workspace_id)
    sm_measure_deps = read_measure_deps(gov_server, known_tables)
    if sm_measure_deps:
        print(f'  Loaded {len(sm_measure_deps)} measure group(s) from wh_governance.data_lineage.measure_deps')
    else:
        print('  data_lineage.measure_deps is empty — falling back to API methods')
        for ds_id in seen_datasets:
            ds_name = dataset_names.get(ds_id, ds_id[:8])
            # Strategy 1: XMLA via MSOLAP
            deps = fetch_sm_measure_deps_xmla(workspace_name, dataset_names.get(ds_id, ''), known_tables)
            if deps:
                sm_measure_deps.update(deps)
                print(f'  {ds_name}: {len(deps)} measure group(s) via XMLA')
                continue
            # Strategy 2: executeQueries REST API
            deps = fetch_sm_measure_deps(workspace_id, ds_id, known_tables)
            if deps:
                sm_measure_deps.update(deps)
                print(f'  {ds_name}: {len(deps)} measure group(s) via executeQueries')
                continue
            # Strategy 3: TMDL definition parts
            try:
                sm_parts = get_semantic_model_definition(workspace_id, ds_id)
            except Exception as e:
                print(f'  {ds_name}: getDefinition failed ({e}) — skipping')
                continue
            if sm_parts:
                deps = parse_sm_measure_deps(sm_parts, known_tables)
                sm_measure_deps.update(deps)
                print(f'  {ds_name}: {len(deps)} measure group(s) from TMDL')
            else:
                print(f'  {ds_name}: no measure data available')

    # Build PBIP ZIP index (sm_id -> report folder inside ZIP) if provided
    pbip_zip_index: dict[str, tuple[str, str]] = {}
    if pbip_zip and pbip_zip.exists():
        pbip_zip_index = build_pbip_zip_index(str(pbip_zip))
        print(f'\nPBIP ZIP index: {len(pbip_zip_index)} report(s) found')
        for sm_id, (zfolder, rfolder) in pbip_zip_index.items():
            print(f'  sm={sm_id[:8]}... -> {rfolder}')
    elif pbip_zip:
        print(f'\nWARNING: --pbip-zip path not found: {pbip_zip}')

    print('\nParsing PBIX files for page->table mapping...')
    report_page_tables:  dict[str, dict[str, set[str]]]  = {}
    report_page_visuals: dict[str, dict[str, list[dict]]] = {}
    for rpt in reports_with_pages:
        ds_id = rpt.get('datasetId', '').lower()
        # --pbip-zip-only: skip reports whose SM is not in the ZIP
        if pbip_zip_only and ds_id not in pbip_zip_index:
            print(f'  {rpt["name"]}: skipped (not in PBIP ZIP)')
            continue
        # Strategy 0: local PBIP ZIP (highest fidelity, no download needed)
        if ds_id in pbip_zip_index:
            _, report_folder = pbip_zip_index[ds_id]
            page_map = parse_pbip_zip_page_tables(
                str(pbip_zip), report_folder,
                known_tables=known_tables,
                measure_deps=sm_measure_deps or None,
            )
            if page_map:
                # Verify the ZIP's page IDs actually match this report's pages.
                # Two reports can share the same SM ID (e.g. dashboard_nations_adjusted
                # and Warehouse_Dashboard) — only accept the map when IDs overlap.
                report_page_ids = {p['name'] for p in rpt.get('pages', [])}
                if report_page_ids & set(page_map.keys()):
                    report_page_tables[rpt['id']] = page_map
                    vis_map = parse_pbip_zip_visuals(
                        str(pbip_zip), report_folder,
                        known_tables=known_tables,
                        measure_deps=sm_measure_deps or None,
                    )
                    if vis_map:
                        report_page_visuals[rpt['id']] = vis_map
                    total_refs = sum(len(v) for v in page_map.values())
                    vis_count  = sum(len(v) for v in vis_map.values())
                    print(f'  {rpt["name"]}: {len(page_map)} pages, {total_refs} table refs, {vis_count} visuals (local PBIP ZIP)')
                    continue
                print(f'  {rpt["name"]}: PBIP ZIP page IDs do not match this report — skipping')
                continue
            else:
                print(f'  {rpt["name"]}: local PBIP ZIP parse returned no pages — falling back to API')

        if pbip_zip_only:
            print(f'  {rpt["name"]}: skipped (--pbip-zip-only, no matching ZIP page map)')
            continue

        page_map = parse_pbix_page_tables(
            workspace_id, rpt['id'],
            known_tables=known_tables,
            measure_deps=sm_measure_deps or None,
        )
        if page_map:
            report_page_tables[rpt['id']] = page_map
            total_refs = sum(len(v) for v in page_map.values())
            print(f'  {rpt["name"]}: {len(page_map)} pages, {total_refs} table refs parsed')
        else:
            print(f'  {rpt["name"]}: PBIX export failed, falling back to report-level')

    populate_governance(
        server=gov_server,
        workspace_id=workspace_id,
        workspace_name=workspace_name,
        artifacts=artifacts,
        tables=tables,
        columns=columns,
        gold_silver=gold_silver,
        silver_bronze=silver_bronze,
        reports=reports_with_pages,
        report_warehouse_map=report_warehouse_map,
        report_page_tables=report_page_tables,
        report_page_visuals=report_page_visuals,
        relationships_only=relationships_only,
    )


if __name__ == '__main__':
    main()
