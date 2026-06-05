"""
Fabric Documentation Agent
Usage:
    python run_agent.py --workspace "XP3R-R&D"
    python run_agent.py --workspace "XP3R-R&D" --dry-run
    python run_agent.py --workspace "XP3R-R&D" --output my_output.xlsx
    python run_agent.py --workspace "XP3R-R&D" --notebooks ./notebooks
"""
import argparse
import sys
from datetime import datetime
from pathlib import Path

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
)
from agent.sql_api import SQLClient
from agent.lineage import build_lineage, parse_pipeline, print_lineage_summary
from agent.writer import write_output


def parse_args():
    p = argparse.ArgumentParser(description='Document a Fabric workspace into an Excel spreadsheet.')
    p.add_argument('--workspace', required=True, help='Workspace name or ID')
    p.add_argument('--dry-run', action='store_true', help='Print lineage summary; do not write Excel')
    p.add_argument('--output', help='Output .xlsx path (default: fabric_doc_<workspace>_<date>.xlsx)')
    p.add_argument('--notebooks', default=str(NOTEBOOKS_DIR), help='Directory for local .ipynb fallback files')
    p.add_argument(
        '--layers', default=None,
        help='Comma-separated list of layers to include in Tables/Columns/Lineage '
             '(e.g. bronze,silver,gold). Unknown-layer items still appear in Artifacts tab. '
             'Default: include all layers.',
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
    sql_ep = SqlEndpoint(
        connection_string=sql_ep_props.get('connectionString', ''),
        endpoint_id=sql_ep_props.get('id', ''),
    ) if sql_ep_props.get('connectionString') else None
    lh = LakehouseInfo(
        **{k: v for k, v in art.__dict__.items()},
        sql_endpoint=sql_ep,
        tables_onelake_path=props.get('oneLakeTablesPath', ''),
        files_onelake_path=props.get('oneLakeFilesPath', ''),
    )
    return lh


def collect_workspace(workspace_id: str, workspace_name: str, notebooks_dir: Path, layer_filter: set[str] | None = None):
    """Collect all data from the workspace. Returns structured objects."""

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

        elif itype == 'DataflowGen2':
            all_artifacts.append(art)
            print(f'      Dataflow:  {art.display_name}')

        elif itype == 'DataPipeline':
            raw_pipelines.append(item)
            # Pipelines don't appear in Artifacts tab

    print('\n[3/5] Querying tables, schema, and stats...')
    all_tables: list[TableInfo] = []
    all_columns: list[ColumnInfo] = []

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

        client = SQLClient(lh.sql_endpoint.connection_string, lh.display_name)
        try:
            raw_tables = client.get_tables()
        except Exception as e:
            print(f'      {lh.display_name}: SQL connection failed — {e}')
            lh._tables = []
            continue

        lh._tables = []
        if not raw_tables:
            print(f'      {lh.display_name}: no registered tables (may be Files-only bronze layer)')
        else:
            print(f'      {lh.display_name}: {len(raw_tables)} table(s)')

        for traw in raw_tables:
            schema = traw['schema']
            tname = traw['table_name']

            cols = client.get_columns(schema, tname)
            col_names = [c['col_name'] for c in cols]
            row_count = client.get_row_count(schema, tname)
            stats = client.get_column_stats(schema, tname, col_names)
            last_updated = client.get_last_updated(schema, tname, col_names)

            tbl = TableInfo(
                table_name=tname,
                schema_name=schema,
                lh_name=lh.display_name,
                lh_id=lh.id,
                layer=lh.layer,
                workspace_name=workspace_name,
                description=lh.description,   # table-level desc not available via REST; use LH desc as fallback
                row_count=row_count,
                last_updated=last_updated,
            )
            lh._tables.append(tbl)
            all_tables.append(tbl)

            for c in cols:
                cname = c['col_name']
                s = stats.get(cname, {})
                all_columns.append(ColumnInfo(
                    lh_name=lh.display_name,
                    table_name=tname,
                    col_name=cname,
                    datatype=c['datatype'],
                    is_nullable=c['is_nullable'],
                    sample_value=s.get('sample', ''),
                    pct_null=s.get('pct_null', ''),
                ))

            print(f'        {schema}.{tname}: {row_count} rows, {len(cols)} cols, last_updated={last_updated or "n/a"}')

        client.close()

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

    return doc_artifacts, all_tables, all_columns, gold_silver, silver_bronze, data_sources


def main():
    args = parse_args()
    notebooks_dir = Path(args.notebooks)

    layer_filter = (
        {l.strip().capitalize() for l in args.layers.split(',')}
        if args.layers else None
    )

    print(f'Fabric Documentation Agent')
    print(f'Workspace : {args.workspace}')
    print(f'Dry run   : {args.dry_run}')
    print(f'Notebooks : {notebooks_dir}')
    print(f'Layers    : {", ".join(sorted(layer_filter)) if layer_filter else "all"}')

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

    artifacts, tables, columns, gold_silver, silver_bronze, data_sources = collect_workspace(
        workspace_id, workspace_name, notebooks_dir, layer_filter=layer_filter,
    )

    print_lineage_summary(gold_silver, silver_bronze)

    if args.dry_run:
        print('\n[dry-run] Skipping Excel output.')
        print(f'  Artifacts  : {len(artifacts)}')
        print(f'  Tables     : {len(tables)}')
        print(f'  Columns    : {len(columns)}')
        print(f'  G->S rows   : {len(gold_silver)}')
        print(f'  S->B rows   : {len(silver_bronze)}')
        print(f'  Data Sources: {len(data_sources)}')
        return

    if args.output:
        output_path = Path(args.output)
    else:
        date_str = datetime.now().strftime('%Y%m%d_%H%M')
        safe_name = workspace_name.replace(' ', '_').replace('/', '_')
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


if __name__ == '__main__':
    main()
