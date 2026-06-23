from pathlib import Path

AZ_CLI = r'C:\Program Files (x86)\Microsoft SDKs\Azure\CLI2\wbin\az.cmd'

FABRIC_API_BASE = 'https://api.fabric.microsoft.com/v1'
ONELAKE_BLOB_BASE = 'https://onelake.blob.fabric.microsoft.com'
ONELAKE_DFS_BASE = 'https://onelake.dfs.fabric.microsoft.com'

FABRIC_RESOURCE = 'https://api.fabric.microsoft.com'
SQL_RESOURCE = 'https://database.windows.net/'
STORAGE_RESOURCE = 'https://storage.azure.com/'

PROJECT_ROOT = Path(__file__).parent.parent
TEMPLATE_PATH = PROJECT_ROOT / 'fabric_doc_template.xlsx'
NOTEBOOKS_DIR = PROJECT_ROOT / 'notebooks'

# Item types returned by the items API that we care about
IN_SCOPE_TYPES = {'Lakehouse', 'Warehouse', 'Notebook', 'DataflowGen2', 'Dataflow', 'DataPipeline'}

# Types that appear in Fabric Artifacts tab (excludes pipelines, which are orchestration not artifacts)
ARTIFACT_TAB_TYPES = {'Lakehouse', 'Warehouse', 'Notebook', 'DataflowGen2', 'Dataflow'}

# Types that have queryable SQL endpoints
SQL_ENDPOINT_TYPES = {'Lakehouse', 'Warehouse'}

LAYER_KEYWORDS = ['bronze', 'silver', 'gold']

# Column names that indicate a row-level timestamp (used for Last Updated)
TIMESTAMP_COLUMNS = [
    '_ingested_at', '_generated_at', '_updated_at', '_modified_at',
    'ingestion_date', 'load_date', 'loaded_at', 'updated_at',
    'modified_at', '_created_at', 'last_modified',
]



def detect_layer(display_name: str, description: str = '') -> str:
    text = (display_name + ' ' + description).lower()
    for layer in LAYER_KEYWORDS:
        if layer in text:
            return layer.capitalize()
    return 'Unknown'
