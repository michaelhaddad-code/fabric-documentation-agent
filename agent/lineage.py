"""
Lineage assembly. No external calls — pure logic over collected data.

Strategy (in priority order):
1. PARSED     — notebook source available (getDefinition or local file); patterns matched
2. INFERRED   — pipeline activity order + table layer inventory; no source needed
3. NEEDS REVIEW — source unavailable and inference is ambiguous
"""
from pathlib import Path

from .config import NOTEBOOKS_DIR
from .fabric_api import get_notebook_definition, get_dataflow_definition
from .models import (
    ArtifactInfo, LakehouseInfo, NotebookSource, PipelineActivity,
    PipelineInfo, LineageEdge, DataSourceRow,
    PARSED, INFERRED, NEEDS_REVIEW,
)
from .notebook_parser import parse_ipynb, parse_source, find_local_notebook


# ---------------------------------------------------------------------------
# Parse pipeline JSON → PipelineInfo
# ---------------------------------------------------------------------------

def parse_pipeline(item: dict, pipeline_json: dict) -> PipelineInfo:
    activities = []
    for act in pipeline_json.get('properties', {}).get('activities', []):
        tp = act.get('typeProperties', {})
        nb_id = tp.get('notebookId') or tp.get('artifactId') or ''
        depends = [d['activity'] for d in act.get('dependsOn', [])]
        activities.append(PipelineActivity(
            name=act.get('name', ''),
            notebook_id=nb_id,
            depends_on=depends,
        ))
    return PipelineInfo(
        id=item['id'],
        display_name=item['display_name'],
        description=item.get('description', ''),
        activities=activities,
    )


# ---------------------------------------------------------------------------
# Resolve notebook source code
# ---------------------------------------------------------------------------

def _resolve_notebook(
    nb_item: ArtifactInfo,
    workspace_id: str,
    notebooks_dir: Path,
) -> NotebookSource:
    ns = NotebookSource(
        notebook_id=nb_item.id,
        display_name=nb_item.display_name,
        source_method='none',
    )

    # 1. Try getDefinition
    parts = get_notebook_definition(workspace_id, nb_item.id)
    if parts:
        for part in parts:
            path = part.get('path', '')
            if path.endswith('.ipynb') or path.endswith('.py'):
                import base64
                raw = base64.b64decode(part['payload']).decode('utf-8')
                result = parse_source(raw)
                ns.reads = result['reads']
                ns.writes = result['writes']
                ns.source_method = 'getDefinition'
                return ns

    # 2. Try local file
    local = find_local_notebook(nb_item.display_name, notebooks_dir)
    if local:
        result = parse_ipynb(local)
        ns.reads = result['reads']
        ns.writes = result['writes']
        ns.source_method = 'local_file'

    return ns


def _resolve_dataflow(
    item: ArtifactInfo,
    workspace_id: str,
) -> NotebookSource:
    """Dataflows use M query; treat similarly to notebooks for lineage."""
    ns = NotebookSource(
        notebook_id=item.id,
        display_name=item.display_name,
        source_method='none',
    )
    m_query = get_dataflow_definition(workspace_id, item.id)
    if m_query:
        # Very simple extraction: find table references in M source references
        import re
        # Source(name="...") or #"..." patterns in M
        refs = re.findall(r'#?["\']([a-zA-Z_][a-zA-Z0-9_ ]+)["\']', m_query)
        ns.reads = list({r.lower().replace(' ', '_') for r in refs if len(r) > 2})
        ns.source_method = 'getDefinition'
    return ns


# ---------------------------------------------------------------------------
# Build table → layer map from collected lakehouses
# ---------------------------------------------------------------------------

def _table_layer_map(lakehouses: list[LakehouseInfo]) -> dict[str, tuple[str, str]]:
    """Returns {table_name_lower: (lh_name, layer)}."""
    mapping: dict[str, tuple[str, str]] = {}
    for lh in lakehouses:
        for tbl in getattr(lh, '_tables', []):
            mapping[tbl.table_name.lower()] = (lh.display_name, lh.layer)
    return mapping


# ---------------------------------------------------------------------------
# Main lineage builder
# ---------------------------------------------------------------------------

def build_lineage(
    workspace_id: str,
    lakehouses: list[LakehouseInfo],
    all_items: list[ArtifactInfo],
    pipelines: list[PipelineInfo],
    notebooks_dir: Path = NOTEBOOKS_DIR,
) -> tuple[list[LineageEdge], list[LineageEdge], list[DataSourceRow]]:
    """
    Returns:
        gold_silver_edges  — list of LineageEdge (gold table ← notebook ← silver tables)
        silver_bronze_edges — list of LineageEdge (silver table ← notebook ← bronze sources)
        data_sources       — list of DataSourceRow for the Data Sources tab
    """
    # Index artifacts by id
    nb_items = {i.id: i for i in all_items if i.type == 'Notebook'}
    df_items = {i.id: i for i in all_items if i.type == 'DataflowGen2'}

    # Resolve notebook/dataflow sources (reads/writes)
    sources: dict[str, NotebookSource] = {}
    for nb in nb_items.values():
        print(f'  Resolving notebook: {nb.display_name}')
        ns = _resolve_notebook(nb, workspace_id, notebooks_dir)
        sources[nb.id] = ns
        print(f'    source_method={ns.source_method}  reads={ns.reads}  writes={ns.writes}')
    for df in df_items.values():
        print(f'  Resolving dataflow: {df.display_name}')
        ns = _resolve_dataflow(df, workspace_id)
        sources[df.id] = ns
        print(f'    source_method={ns.source_method}  reads={ns.reads}  writes={ns.writes}')

    # Layer-indexed table sets
    bronze_lhs = [lh for lh in lakehouses if lh.layer == 'Bronze']
    silver_lhs = [lh for lh in lakehouses if lh.layer == 'Silver']
    gold_lhs   = [lh for lh in lakehouses if lh.layer == 'Gold']

    def tables_for(lh_list: list[LakehouseInfo]) -> dict[str, str]:
        """Returns {table_name_lower: lh_display_name}."""
        out = {}
        for lh in lh_list:
            for tbl in getattr(lh, '_tables', []):
                out[tbl.table_name.lower()] = lh.display_name
        return out

    bronze_tables = tables_for(bronze_lhs)
    silver_tables = tables_for(silver_lhs)
    gold_tables   = tables_for(gold_lhs)

    # Build execution order from pipelines (topological sort by dependsOn)
    def ordered_activities(pipeline: PipelineInfo) -> list[PipelineActivity]:
        """Kahn's sort on activity dependency graph."""
        by_name = {a.name: a for a in pipeline.activities}
        in_degree = {a.name: 0 for a in pipeline.activities}
        for a in pipeline.activities:
            for dep in a.depends_on:
                in_degree[a.name] = in_degree.get(a.name, 0) + 1
        queue = [a for a in pipeline.activities if in_degree[a.name] == 0]
        ordered = []
        while queue:
            node = queue.pop(0)
            ordered.append(node)
            for a in pipeline.activities:
                if node.name in a.depends_on:
                    in_degree[a.name] -= 1
                    if in_degree[a.name] == 0:
                        queue.append(a)
        return ordered

    # Map: notebook_id → (writes_to_layers, reads_from_layers) based on pipeline position
    # Pipeline order tells us: first notebook is Bronze→Silver, last is Silver→Gold (for medallion)
    nb_write_layer: dict[str, str] = {}
    nb_read_layer: dict[str, str] = {}

    _LAYER_PREV = {'Bronze': 'External', 'Silver': 'Bronze', 'Gold': 'Silver'}

    def _layer_from_name(name: str) -> str | None:
        """Return the output layer if the name contains a clear layer keyword."""
        n = name.lower()
        for layer in ['gold', 'silver', 'bronze']:  # gold first (most specific)
            if layer in n:
                return layer.capitalize()
        return None

    for pipeline in pipelines:
        ordered = ordered_activities(pipeline)
        nb_acts = [a for a in ordered if a.notebook_id]
        n = len(nb_acts)
        for idx, act in enumerate(nb_acts):
            nid = act.notebook_id

            # Priority 1: activity name contains a layer keyword ("Bronze", "Silver", "Gold")
            write_layer = _layer_from_name(act.name)
            if write_layer:
                nb_write_layer[nid] = write_layer
                nb_read_layer[nid] = _LAYER_PREV[write_layer]
                continue

            # Priority 2: positional fallback (only when name gives no signal)
            if n == 1:
                if gold_tables and silver_tables:
                    nb_read_layer[nid] = 'Silver'
                    nb_write_layer[nid] = 'Gold'
                elif silver_tables:
                    nb_read_layer[nid] = 'Bronze'
                    nb_write_layer[nid] = 'Silver'
            elif n == 2:
                if idx == 0:
                    nb_read_layer[nid] = 'Bronze'
                    nb_write_layer[nid] = 'Silver'
                else:
                    nb_read_layer[nid] = 'Silver'
                    nb_write_layer[nid] = 'Gold'
            else:
                if idx == 0:
                    nb_read_layer[nid] = 'Bronze'
                    nb_write_layer[nid] = 'Silver'
                elif idx == n - 1:
                    nb_read_layer[nid] = 'Silver'
                    nb_write_layer[nid] = 'Gold'

    # Fix 1: name-based fallback for notebooks not in any pipeline.
    # Only fires when that layer transition isn't already covered by a pipeline notebook,
    # preventing duplicates when both a pipeline step and an orphaned notebook write to Gold.
    _NAME_TRANSITIONS = [
        (['silver_to_gold', 'silver2gold', 'silvertogold'], 'Silver', 'Gold'),
        (['bronze_to_silver', 'bronze2silver', 'bronzetosilver'], 'Bronze', 'Silver'),
        (['bronze_to_gold', 'bronze2gold'], 'Bronze', 'Gold'),
    ]
    pipeline_write_layers = set(nb_write_layer.values())
    for nb_id, nb_item in nb_items.items():
        if nb_id in nb_write_layer:
            continue
        name = nb_item.display_name.lower()
        for keywords, read_layer, write_layer in _NAME_TRANSITIONS:
            if any(kw in name for kw in keywords):
                if write_layer in pipeline_write_layers:
                    print(f'  Name-inferred skipped: {nb_item.display_name} -> {write_layer} already covered by pipeline')
                else:
                    nb_read_layer[nb_id] = read_layer
                    nb_write_layer[nb_id] = write_layer
                    print(f'  Name-inferred: {nb_item.display_name} -> reads {read_layer}, writes {write_layer}')
                break

    def _get_artifact_name(artifact_id: str) -> str:
        item = nb_items.get(artifact_id) or df_items.get(artifact_id)
        return item.display_name if item else artifact_id

    # ---------------------------------------------------------------------------
    # Gold → Silver edges
    # ---------------------------------------------------------------------------
    gold_silver: list[LineageEdge] = []

    for lh in gold_lhs:
        for tbl in getattr(lh, '_tables', []):
            tname = tbl.table_name.lower()

            # Find notebook(s) that write this gold table
            writing_nbs = [
                nid for nid, ns in sources.items()
                if tname in ns.writes
            ]
            # Fallback: notebook whose write_layer is Gold (pipeline inference)
            if not writing_nbs:
                writing_nbs = [
                    nid for nid, wl in nb_write_layer.items()
                    if wl == 'Gold'
                ]
                confidence = INFERRED
                notes = 'INFERRED: no source code available; assigned from pipeline position'
            else:
                confidence = PARSED
                notes = f'PARSED: source_method={sources[writing_nbs[0]].source_method}'

            for nid in writing_nbs:
                nb_name = _get_artifact_name(nid)
                ns = sources.get(nid)

                # Resolve silver inputs
                if ns and ns.source_method != 'none' and ns.reads:
                    silver_inputs = [r for r in ns.reads if r in silver_tables]
                else:
                    silver_inputs = list(silver_tables.keys())
                    confidence = INFERRED
                    notes = 'INFERRED: no source code; used all silver tables as inputs'

                silver_lh_name = silver_tables.get(silver_inputs[0], '') if silver_inputs else ''

                gold_silver.append(LineageEdge(
                    output_lh=lh.display_name,
                    output_table=tbl.table_name,
                    notebook_name=nb_name,
                    input_refs=silver_inputs,
                    confidence=confidence,
                    notes=notes,
                ))

    # ---------------------------------------------------------------------------
    # Silver → Bronze edges
    # ---------------------------------------------------------------------------
    silver_bronze: list[LineageEdge] = []
    data_sources: list[DataSourceRow] = []

    for lh in silver_lhs:
        for tbl in getattr(lh, '_tables', []):
            tname = tbl.table_name.lower()

            writing_nbs = [
                nid for nid, ns in sources.items()
                if tname in ns.writes
            ]
            if not writing_nbs:
                writing_nbs = [
                    nid for nid, wl in nb_write_layer.items()
                    if wl == 'Silver'
                ]
                confidence = INFERRED
                notes = 'INFERRED: no source code available; assigned from pipeline position'
            else:
                confidence = PARSED
                notes = f'PARSED: source_method={sources[writing_nbs[0]].source_method}'

            for nid in writing_nbs:
                nb_name = _get_artifact_name(nid)
                ns = sources.get(nid)

                # For bronze, we may have either table names (rare) or file references
                if ns and ns.source_method != 'none' and ns.reads:
                    bronze_inputs = ns.reads
                else:
                    # Fix 2: prefer matching bronze Delta tables over Files/ path
                    if bronze_tables and tname in bronze_tables:
                        # Exact name match: silver.timesheets <- bronze.timesheets
                        bronze_inputs = [tname]
                        confidence = INFERRED
                        notes = 'INFERRED: matched silver table name to same-named bronze Delta table'
                    elif bronze_tables:
                        # Bronze has tables but no exact name match — list all as candidates
                        bronze_inputs = sorted(bronze_tables.keys())
                        confidence = NEEDS_REVIEW
                        notes = 'NEEDS REVIEW: no exact name match between silver and bronze tables'
                    else:
                        # Bronze has no registered tables — fall back to Files/ path
                        bronze_file_refs = []
                        for bronze_lh in bronze_lhs:
                            files = getattr(bronze_lh, '_files', [])
                            if files:
                                for f in files:
                                    rel = f['name'].split('/', 1)[-1] if '/' in f['name'] else f['name']
                                    bronze_file_refs.append(f'{bronze_lh.display_name}/{rel}')
                            else:
                                bronze_file_refs.append(f'{bronze_lh.display_name}/Files/ (file source)')
                        bronze_inputs = bronze_file_refs or [f'{bl.display_name}/Files/' for bl in bronze_lhs]
                        confidence = INFERRED
                        notes = 'INFERRED: no source code; bronze layer has no registered tables; file path used'

                silver_bronze.append(LineageEdge(
                    output_lh=lh.display_name,
                    output_table=tbl.table_name,
                    notebook_name=nb_name,
                    input_refs=bronze_inputs,
                    confidence=confidence,
                    notes=notes,
                ))

    # ---------------------------------------------------------------------------
    # Data Sources (what feeds bronze)
    # ---------------------------------------------------------------------------
    for bronze_lh in bronze_lhs:
        files = getattr(bronze_lh, '_files', [])
        if files:
            for f in files:
                rel_path = f['name'].split('/', 1)[-1] if '/' in f['name'] else f['name']
                fname = rel_path.split('/')[-1]
                ext = fname.rsplit('.', 1)[-1].lower() if '.' in fname else ''
                src_type = 'File'
                ingestion = 'Manual upload to OneLake'
                onelake_path = f"onelake.dfs.fabric.microsoft.com/{bronze_lh.workspace_name}/{bronze_lh.display_name}/Files/{rel_path}"
                data_sources.append(DataSourceRow(
                    source_name=fname,
                    source_type=src_type,
                    connection_path=onelake_path,
                    target_bronze=rel_path,
                    ingestion_method=ingestion,
                    notes=f'Discovered via OneLake blob listing ({f["size"]} bytes)',
                ))
        else:
            # No files found — stub row
            data_sources.append(DataSourceRow(
                source_name=f'{bronze_lh.display_name} (files)',
                source_type='File',
                connection_path=bronze_lh.files_onelake_path,
                target_bronze='(see Files section)',
                ingestion_method='NEEDS REVIEW',
                notes='OneLake file listing returned no results; populate manually',
            ))

    return gold_silver, silver_bronze, data_sources


# ---------------------------------------------------------------------------
# Console summary (for eyeballing)
# ---------------------------------------------------------------------------

def print_lineage_summary(
    gold_silver: list[LineageEdge],
    silver_bronze: list[LineageEdge],
) -> None:
    print('\n' + '=' * 60)
    print('LINEAGE SUMMARY - verify against ground truth')
    print('=' * 60)

    print('\n  GOLD <- NOTEBOOK <- SILVER')
    print('  ' + '-' * 50)
    for e in gold_silver:
        inputs = ', '.join(e.input_refs) or '(unknown)'
        print(f'  [{e.confidence}] {e.output_table} ({e.output_lh})')
        print(f'       <- {e.notebook_name}')
        print(f'       <- {inputs}')
        if e.notes:
            print(f'       note: {e.notes}')

    print('\n  SILVER <- NOTEBOOK <- BRONZE')
    print('  ' + '-' * 50)
    for e in silver_bronze:
        inputs = ', '.join(e.input_refs) or '(unknown)'
        print(f'  [{e.confidence}] {e.output_table} ({e.output_lh})')
        print(f'       <- {e.notebook_name}')
        print(f'       <- {inputs}')
        if e.notes:
            print(f'       note: {e.notes}')

    print()
