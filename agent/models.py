from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ArtifactInfo:
    id: str
    display_name: str
    type: str
    workspace_id: str
    workspace_name: str
    description: str = ''
    last_modified: str = ''
    created_by: str = ''
    layer: str = ''                  # Bronze / Silver / Gold / Unknown


@dataclass
class SqlEndpoint:
    connection_string: str
    endpoint_id: str


@dataclass
class LakehouseInfo(ArtifactInfo):
    sql_endpoint: Optional[SqlEndpoint] = None
    tables_onelake_path: str = ''    # OneLake DFS path to Tables/
    files_onelake_path: str = ''     # OneLake DFS path to Files/


@dataclass
class TableInfo:
    table_name: str
    schema_name: str
    lh_name: str
    lh_id: str
    layer: str
    workspace_name: str
    description: str = ''
    row_count: Optional[int] = None
    last_updated: str = ''
    source_artifact: str = ''        # display name of notebook/dataflow that writes this


@dataclass
class ColumnInfo:
    lh_name: str
    table_name: str
    col_name: str
    datatype: str
    is_nullable: bool = True
    sample_value: str = ''
    pct_null: str = ''


@dataclass
class NotebookSource:
    notebook_id: str
    display_name: str
    source_method: str              # getDefinition | local_file | none
    reads: list[str] = field(default_factory=list)   # normalized table refs
    writes: list[str] = field(default_factory=list)  # normalized table refs


@dataclass
class PipelineActivity:
    name: str
    notebook_id: str
    depends_on: list[str] = field(default_factory=list)  # activity names


@dataclass
class PipelineInfo:
    id: str
    display_name: str
    description: str
    activities: list[PipelineActivity] = field(default_factory=list)


# Confidence values (ordered from most to least certain)
PARSED = 'PARSED'          # notebook source available and patterns matched
INFERRED = 'INFERRED'      # constructed from pipeline structure + table inventory
NEEDS_REVIEW = 'NEEDS REVIEW'  # ambiguous or incomplete; human should verify


@dataclass
class LineageEdge:
    """One directional step: output_table ← notebook ← input_table(s)"""
    output_lh: str
    output_table: str
    notebook_name: str
    input_refs: list[str]           # table names or file paths, comma-joinable
    confidence: str = INFERRED
    notes: str = ''


@dataclass
class DataSourceRow:
    source_name: str
    source_type: str                # File | API | Shortcut | DB | Unknown
    connection_path: str
    target_bronze: str              # file path or table name in bronze
    ingestion_method: str
    notes: str = ''
