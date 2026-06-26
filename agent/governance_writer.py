"""
Populate wh_governance.data_lineage.objects and data_lineage.relationships.

Objects hierarchy:
  Workspace
  ├── Artifacts (Lakehouse/Warehouse/Notebook/Dataflow) — referenced by relationships
  ├── Report (parent=Workspace)
  │   └── Page (parent=Report)
  │       └── Visual (parent=Page, when PBIP ZIP available)
  Relationships store page→table and visual→table edges (many-to-many).
  Tables are always parented to their Lakehouse/Warehouse artifact.
  │       └── Column
  └── Tables with no connected report (parent=their Artifact)
      └── Column

Uses fast_executemany=True + chunked batches for speed — avoids TCP timeouts
that occur when sending 7000+ individual inserts over a single connection.
"""
import hashlib
import json
import struct
from datetime import datetime, timezone

import pyodbc

from .auth import get_token
from .config import SQL_RESOURCE
from .models import ArtifactInfo, TableInfo, ColumnInfo, LineageEdge

_CONFIDENCE_MAP = {'PARSED': 1.0, 'INFERRED': 0.5, 'NEEDS REVIEW': 0.25}
_DETECTION_MAP  = {'PARSED': 'static_parse', 'INFERRED': 'inferred', 'NEEDS REVIEW': 'manual'}

SQL_COPT_SS_ACCESS_TOKEN = 1256
CHUNK_SIZE = 200


def _sha256(*parts: str) -> str:
    return hashlib.sha256('|'.join(parts).encode()).hexdigest()[:64]


def _now() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')


def _connect(server: str) -> pyodbc.Connection:
    token = get_token(SQL_RESOURCE)
    token_bytes = token.encode('utf-16-le')
    token_struct = struct.pack('=i', len(token_bytes)) + token_bytes
    conn_str = (
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'SERVER={server};DATABASE=wh_governance;'
        f'Encrypt=yes;Connection Timeout=60;'
    )
    conn = pyodbc.connect(
        conn_str,
        attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct},
        timeout=60,
        autocommit=True,
    )
    conn.timeout = 600
    return conn


OBJ_SQL = '''INSERT INTO data_lineage.objects (
    objects_id, objects_type, layer, workspace_name, workspace_id,
    item_name, fq_name, schema_name, parent_objects_id,
    source_system, owner_email, business_domain, sensitivity_label,
    is_pii, is_active, properties_json,
    first_seen_datetime, last_seen_datetime, created_datetime, updated_datetime
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''

REL_SQL = '''INSERT INTO data_lineage.relationships (
    relationships_id, source_objects_id, target_objects_id,
    relationships_type, transformation_type, transformation_expr,
    code_reference, run_id, confidence_score, detection_method,
    is_active, first_seen_datetime, last_seen_datetime,
    created_datetime, updated_datetime
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''


SCHEMA_DDL = 'CREATE SCHEMA data_lineage'

MEASURE_DEPS_DDL = '''
CREATE TABLE data_lineage.measure_deps (
    sm_name             VARCHAR(256)    NOT NULL,
    measure_group       VARCHAR(256)    NOT NULL,
    physical_table      VARCHAR(256)    NOT NULL,
    updated_datetime    DATETIME2(3)    NOT NULL
)'''

OBJECTS_DDL = '''
CREATE TABLE data_lineage.objects (
    objects_id          NVARCHAR(64)    NOT NULL,
    objects_type        NVARCHAR(100)   NOT NULL,
    layer               NVARCHAR(50)    NULL,
    workspace_name      NVARCHAR(500)   NOT NULL,
    workspace_id        NVARCHAR(100)   NOT NULL,
    item_name           NVARCHAR(500)   NOT NULL,
    fq_name             NVARCHAR(2000)  NOT NULL,
    schema_name         NVARCHAR(200)   NULL,
    parent_objects_id   NVARCHAR(64)    NULL,
    source_system       NVARCHAR(200)   NULL,
    owner_email         NVARCHAR(500)   NULL,
    business_domain     NVARCHAR(200)   NULL,
    sensitivity_label   NVARCHAR(200)   NULL,
    is_pii              INT             NOT NULL,
    is_active           INT             NOT NULL,
    properties_json     NVARCHAR(MAX)   NULL,
    first_seen_datetime DATETIME2       NULL,
    last_seen_datetime  DATETIME2       NULL,
    created_datetime    DATETIME2       NULL,
    updated_datetime    DATETIME2       NULL
)'''

RELATIONSHIPS_DDL = '''
CREATE TABLE data_lineage.relationships (
    relationships_id    NVARCHAR(64)    NOT NULL,
    source_objects_id   NVARCHAR(64)    NOT NULL,
    target_objects_id   NVARCHAR(64)    NOT NULL,
    relationships_type  NVARCHAR(100)   NOT NULL,
    transformation_type NVARCHAR(100)   NULL,
    transformation_expr NVARCHAR(MAX)   NULL,
    code_reference      NVARCHAR(500)   NULL,
    run_id              NVARCHAR(200)   NULL,
    confidence_score    DECIMAL(5,4)    NULL,
    detection_method    NVARCHAR(100)   NULL,
    is_active           INT             NOT NULL,
    first_seen_datetime DATETIME2       NULL,
    last_seen_datetime  DATETIME2       NULL,
    created_datetime    DATETIME2       NULL,
    updated_datetime    DATETIME2       NULL
)'''


def provision_governance_warehouse(server: str):
    """
    Creates the data_lineage schema and objects/relationships tables if they
    don't already exist. Safe to call on every run.
    """
    conn = _connect(server)
    cur = conn.cursor()

    # Schema
    cur.execute(
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'data_lineage'"
    )
    if cur.fetchone()[0] == 0:
        cur.execute(SCHEMA_DDL)
        print('  Created schema: data_lineage')
    else:
        print('  Schema data_lineage already exists')

    # Objects table
    cur.execute(
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_SCHEMA = 'data_lineage' AND TABLE_NAME = 'objects'"
    )
    if cur.fetchone()[0] == 0:
        cur.execute(OBJECTS_DDL)
        print('  Created table: data_lineage.objects')
    else:
        print('  Table data_lineage.objects already exists')

    # Relationships table
    cur.execute(
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_SCHEMA = 'data_lineage' AND TABLE_NAME = 'relationships'"
    )
    if cur.fetchone()[0] == 0:
        cur.execute(RELATIONSHIPS_DDL)
        print('  Created table: data_lineage.relationships')
    else:
        print('  Table data_lineage.relationships already exists')

    # Measure deps table (populated by the nb_extract_measure_deps Fabric notebook)
    cur.execute(
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_SCHEMA = 'data_lineage' AND TABLE_NAME = 'measure_deps'"
    )
    if cur.fetchone()[0] == 0:
        cur.execute(MEASURE_DEPS_DDL)
        print('  Created table: data_lineage.measure_deps')
    else:
        print('  Table data_lineage.measure_deps already exists')

    conn.close()


def read_measure_deps(server: str, known_tables: set[str]) -> dict[str, set[str]]:
    """
    Read measure group → physical table mappings written by the
    nb_extract_measure_deps Fabric notebook.
    Returns {measure_group_name.lower(): set[physical_table_name.lower()]}.
    Only includes physical_table values that exist in known_tables.
    """
    conn = _connect(server)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT measure_group, physical_table FROM data_lineage.measure_deps"
        )
        deps: dict[str, set[str]] = {}
        for mg, pt in cur.fetchall():
            if pt.lower() in known_tables:
                deps.setdefault(mg.lower(), set()).add(pt.lower())
        return deps
    except Exception:
        return {}
    finally:
        conn.close()


def _bulk_insert(server: str, sql: str, rows: list[tuple], label: str):
    """Insert rows in chunks, reconnecting and retrying on TCP failures."""
    get_token(SQL_RESOURCE, force_refresh=True)  # ensure fresh token before opening connection
    conn = _connect(server)
    cur = conn.cursor()
    cur.fast_executemany = True
    i = 0
    while i < len(rows):
        chunk = rows[i:i + CHUNK_SIZE]
        for attempt in range(3):
            try:
                cur.executemany(sql, chunk)
                break
            except pyodbc.Error as e:
                if attempt == 2:
                    raise
                print(f'  {label}: connection error at row {i}, reconnecting... ({e})')
                try:
                    conn.close()
                except Exception:
                    pass
                conn = _connect(server)
                cur = conn.cursor()
                cur.fast_executemany = True
        i += CHUNK_SIZE
        print(f'  {label}: {min(i, len(rows))}/{len(rows)}')
    conn.close()


def populate_governance(
    server: str,
    workspace_id: str,
    workspace_name: str,
    artifacts: list[ArtifactInfo],
    tables: list[TableInfo],
    columns: list[ColumnInfo],
    gold_silver: list[LineageEdge],
    silver_bronze: list[LineageEdge],
    reports: list[dict],
    report_warehouse_map: dict[str, str],  # dataset_id -> warehouse/lakehouse item_id
    report_page_tables: dict[str, dict[str, set[str]]] | None = None,
    report_page_visuals: dict[str, dict[str, list[dict]]] | None = None,
    relationships_only: bool = False,
):
    """
    reports: list of dicts from list_reports + get_report_pages, each shaped as:
      {id, name, datasetId, pages: [{name, displayName, order}, ...]}
    report_warehouse_map: dataset_id -> Fabric item_id of the connected warehouse/lakehouse
    report_page_tables: report_id -> {page_internal_id -> set[physical_table_name]}
    report_page_visuals: report_id -> {page_internal_id -> [{'name', 'type', 'physical_tables'}]}
      When provided, Visual objects are written under each Page and visual→table /
      page→table edges are written to data_lineage.relationships.
      Tables are always parented to their Lakehouse/Warehouse artifact.
    """
    print('\nConnecting to wh_governance...')
    now = _now()

    print('  Clearing existing rows...')
    _conn = _connect(server)
    _cur = _conn.cursor()
    _cur.execute('DELETE FROM data_lineage.relationships')
    if not relationships_only:
        _cur.execute('DELETE FROM data_lineage.objects')
    _conn.close()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    obj_rows: list[tuple] = []

    def _obj(objects_id, objects_type, item_name, fq_name, *,
             layer=None, schema_name=None, parent_objects_id=None,
             owner_email=None, properties_json=None):
        return (
            objects_id, objects_type, layer, workspace_name, workspace_id,
            item_name, fq_name, schema_name, parent_objects_id,
            'Microsoft Fabric', owner_email, None, None,
            0, 1, properties_json,
            now, now, now, now,
        )

    # ------------------------------------------------------------------
    # Workspace
    # ------------------------------------------------------------------
    obj_rows.append(_obj(workspace_id, 'Workspace', workspace_name, workspace_name))

    # ------------------------------------------------------------------
    # Artifacts (Lakehouse/Warehouse/Notebook/Dataflow)
    # Referenced as source/target in the relationships section.
    # ------------------------------------------------------------------
    artifact_id_map = {}  # display_name -> id
    for art in artifacts:
        obj_rows.append(_obj(
            art.id, art.type, art.display_name,
            f'{workspace_name}.{art.display_name}',
            layer=art.layer, parent_objects_id=workspace_id,
            owner_email=art.created_by or None,
        ))
        artifact_id_map[art.display_name] = art.id

    # ------------------------------------------------------------------
    # Build per-warehouse table lookup
    # ------------------------------------------------------------------
    lh_id_to_tables: dict[str, list[TableInfo]] = {}
    for tbl in tables:
        lh_id_to_tables.setdefault(tbl.lh_id, []).append(tbl)

    # ------------------------------------------------------------------
    # Report → Page → Table → Column hierarchy
    # Tables with no connected report fall back to their artifact as parent.
    # Reports are processed PBIX-failed first, PBIX-available last so that
    # exact page assignments win over report-level fallbacks when two reports
    # share the same source warehouse/lakehouse.
    # ------------------------------------------------------------------
    table_id_map: dict[tuple[str, str], str] = {}   # (lh_name.lower, table.lower) -> objects_id

    reports_ordered = sorted(
        reports,
        key=lambda r: 1 if (report_page_tables or {}).get(r['id']) else 0,
    )

    for report in reports_ordered:
        report_id     = report['id']
        report_name   = report['name']
        dataset_id    = report.get('datasetId', '')
        pages         = report.get('pages', [])
        report_owner  = report.get('createdBy') or report.get('modifiedBy') or None

        obj_rows.append(_obj(
            report_id, 'Report', report_name,
            f'{workspace_name}.{report_name}',
            parent_objects_id=workspace_id,
            owner_email=report_owner,
        ))

        source_lh_id = report_warehouse_map.get(dataset_id)
        report_tables = lh_id_to_tables.get(source_lh_id, []) if source_lh_id else []

        pbix_pages   = (report_page_tables  or {}).get(report_id, {})
        rpt_visuals  = (report_page_visuals or {}).get(report_id, {})
        page_obj_ids: dict[str, str] = {}  # page_internal_id -> objects_id

        for page in pages:
            page_internal = page['name']
            page_display  = page['displayName']
            page_obj_id   = _sha256(report_id, page_internal)
            page_obj_ids[page_internal] = page_obj_id
            raw_vis       = page.get('visibility')
            page_props    = json.dumps({
                'order':      page.get('order', 0),
                'visibility': 'hidden' if raw_vis == 'HiddenInPublic' else 'visible',
            })

            obj_rows.append(_obj(
                page_obj_id, 'Page', page_display,
                f'{workspace_name}.{report_name}.{page_display}',
                parent_objects_id=report_id,
                owner_email=report_owner,
                properties_json=page_props,
            ))

            # Visual objects under this page
            for vis in rpt_visuals.get(page_internal, []):
                vis_obj_id = _sha256(page_obj_id, vis['name'], vis['type'])
                vis_props  = json.dumps({'visual_type': vis['type']})
                obj_rows.append(_obj(
                    vis_obj_id, 'Visual', vis['name'],
                    f'{workspace_name}.{report_name}.{page_display}.{vis["name"]}',
                    parent_objects_id=page_obj_id,
                    owner_email=report_owner,
                    properties_json=vis_props,
                ))

        for tbl in report_tables:
            tbl_obj_id = _sha256(workspace_id, tbl.lh_id, tbl.schema_name, tbl.table_name)
            table_id_map[(tbl.lh_name.lower(), tbl.table_name.lower())] = tbl_obj_id
            # Tables are always parented to their lakehouse — relationships carry page/visual links

    # ------------------------------------------------------------------
    # Table objects (and columns)
    # ------------------------------------------------------------------
    col_count = 0

    # Build column lookup: (lh_name.lower, table.lower) -> [ColumnInfo]
    col_lookup: dict[tuple[str, str], list[ColumnInfo]] = {}
    for col in columns:
        col_lookup.setdefault((col.lh_name.lower(), col.table_name.lower()), []).append(col)

    for tbl in tables:
        tbl_obj_id = _sha256(workspace_id, tbl.lh_id, tbl.schema_name, tbl.table_name)
        table_id_map.setdefault((tbl.lh_name.lower(), tbl.table_name.lower()), tbl_obj_id)

        # Tables are always parented to their artifact (warehouse/lakehouse)
        parent = tbl.lh_id

        props = json.dumps({'row_count': tbl.row_count, 'last_updated': tbl.last_updated})
        obj_rows.append(_obj(
            tbl_obj_id, 'Table', tbl.table_name,
            f'{workspace_name}.{tbl.lh_name}.{tbl.schema_name}.{tbl.table_name}',
            layer=tbl.layer, schema_name=tbl.schema_name,
            parent_objects_id=parent, properties_json=props,
        ))

        for col in col_lookup.get((tbl.lh_name.lower(), tbl.table_name.lower()), []):
            col_obj_id = _sha256(workspace_id, col.lh_name, col.table_name, col.col_name)
            col_props = json.dumps({
                'datatype': col.datatype,
                'is_nullable': col.is_nullable,
                'pct_null': col.pct_null,
                'sample_value': str(col.sample_value)[:500] if col.sample_value else None,
            })
            obj_rows.append(_obj(
                col_obj_id, 'Column', col.col_name,
                f'{workspace_name}.{col.lh_name}.{col.table_name}.{col.col_name}',
                parent_objects_id=tbl_obj_id, properties_json=col_props,
            ))
            col_count += 1

    vis_count = sum(
        len(vlist)
        for rpt_vis in (report_page_visuals or {}).values()
        for vlist in rpt_vis.values()
    )
    if relationships_only:
        print(
            f'  Skipping objects insert (--relationships-only): '
            f'{len(obj_rows)} rows already in warehouse.'
        )
    else:
        print(
            f'  Inserting {len(obj_rows)} object rows '
            f'(1 workspace + {len(artifacts)} artifacts + {len(reports)} reports + '
            f'{sum(len(r.get("pages", [])) for r in reports)} pages + '
            f'{vis_count} visuals + {len(tables)} tables + {col_count} columns)...'
        )
        _bulk_insert(server, OBJ_SQL, obj_rows, 'objects')

    # ------------------------------------------------------------------
    # Relationship rows (notebook/dataflow lineage — unchanged)
    # ------------------------------------------------------------------
    rel_rows: list[tuple] = []
    seen_rels: set[str] = set()

    def _add_edges(edges: list[LineageEdge]):
        for edge in edges:
            target_obj_id = table_id_map.get(
                (edge.output_lh.lower(), edge.output_table.lower())
            )
            if not target_obj_id:
                continue

            confidence = _CONFIDENCE_MAP.get(edge.confidence, 0.5)
            detection  = _DETECTION_MAP.get(edge.confidence, 'inferred')
            nb_obj_id  = artifact_id_map.get(edge.notebook_name, edge.notebook_name)

            if edge.confidence == 'NEEDS REVIEW':
                rel_id = _sha256(target_obj_id, 'needs_review', edge.notebook_name)
                if rel_id not in seen_rels:
                    seen_rels.add(rel_id)
                    rel_rows.append((
                        rel_id, nb_obj_id, target_obj_id,
                        'derives_from', None, 'NEEDS REVIEW — candidates not resolved',
                        edge.notebook_name, None, confidence, detection,
                        1, now, now, now, now,
                    ))
                continue

            for src_ref in edge.input_refs:
                src_obj_id = next(
                    (oid for (_, tname), oid in table_id_map.items() if tname == src_ref.lower()),
                    _sha256('unresolved', src_ref),
                )
                rel_id = _sha256(src_obj_id, target_obj_id, 'derives_from')
                if rel_id not in seen_rels:
                    seen_rels.add(rel_id)
                    rel_rows.append((
                        rel_id, src_obj_id, target_obj_id,
                        'derives_from', None, None,
                        edge.notebook_name, None, confidence, detection,
                        1, now, now, now, now,
                    ))

    _add_edges(gold_silver)
    _add_edges(silver_bronze)

    # page→table and visual→table relationships (many-to-many, from PBIP visual parse)
    if report_page_visuals:
        # Build a flat name→obj_id lookup for physical tables (any lakehouse)
        tbl_name_to_id: dict[str, str] = {tname: oid for (_, tname), oid in table_id_map.items()}

        for report in reports:
            report_id  = report['id']
            pages      = report.get('pages', [])
            rpt_visuals = report_page_visuals.get(report_id, {})
            if not rpt_visuals:
                continue

            # Rebuild page_obj_ids for this report
            page_obj_ids = {p['name']: _sha256(report_id, p['name']) for p in pages}

            for page_internal, vis_list in rpt_visuals.items():
                page_obj_id = page_obj_ids.get(page_internal)
                if not page_obj_id:
                    continue

                page_tables_seen: set[str] = set()

                for vis in vis_list:
                    vis_obj_id = _sha256(page_obj_id, vis['name'], vis['type'])

                    for tbl_name in vis['physical_tables']:
                        tbl_obj_id = tbl_name_to_id.get(tbl_name)
                        if not tbl_obj_id:
                            continue

                        # visual → table
                        rel_id = _sha256(vis_obj_id, tbl_obj_id, 'pbip_visual')
                        if rel_id not in seen_rels:
                            seen_rels.add(rel_id)
                            rel_rows.append((
                                rel_id, vis_obj_id, tbl_obj_id,
                                'uses', None, None, None, None,
                                1.0, 'pbip_visual',
                                1, now, now, now, now,
                            ))

                        # page → table (deduplicated per page)
                        if tbl_name not in page_tables_seen:
                            page_tables_seen.add(tbl_name)
                            rel_id2 = _sha256(page_obj_id, tbl_obj_id, 'pbip_page')
                            if rel_id2 not in seen_rels:
                                seen_rels.add(rel_id2)
                                rel_rows.append((
                                    rel_id2, page_obj_id, tbl_obj_id,
                                    'uses', None, None, None, None,
                                    1.0, 'pbip_page',
                                    1, now, now, now, now,
                                ))

    print(f'  Inserting {len(rel_rows)} relationship rows...')
    _bulk_insert(server, REL_SQL, rel_rows, 'relationships')

    print('  wh_governance populated successfully.')
