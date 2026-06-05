"""
Parse .ipynb notebook files (or raw source strings) to extract table references.
Used as a fallback when getDefinition returns empty parts.

Drop .ipynb files into the ./notebooks/ folder (display name as filename,
e.g. "nb_bronze_to_silver_time_entries.ipynb") and they will be picked up.
"""
import json
import re
from pathlib import Path


# --- Write patterns: what the notebook produces ---
_WRITE_RE = [
    re.compile(r'\.saveAsTable\s*\(\s*["\']([^"\']+)["\']', re.IGNORECASE),
    re.compile(r'\.writeTo\s*\(\s*["\']([^"\']+)["\']', re.IGNORECASE),
    re.compile(r'spark\.sql\s*\(\s*["\'].*?(?:INSERT\s+(?:OVERWRITE\s+)?(?:INTO\s+)?|CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?)([a-zA-Z_][a-zA-Z0-9_.]+)', re.IGNORECASE | re.DOTALL),
]

# --- Read patterns: what the notebook consumes ---
_READ_RE = [
    re.compile(r'spark\.read\.table\s*\(\s*["\']([^"\']+)["\']', re.IGNORECASE),
    re.compile(r'spark\.table\s*\(\s*["\']([^"\']+)["\']', re.IGNORECASE),
    # Delta/Parquet path reads: spark.read.format(...).load("path")
    re.compile(r'\.load\s*\(\s*["\']([^"\']+)["\']', re.IGNORECASE),
    # SQL FROM clause — only match simple identifiers to avoid false positives
    re.compile(r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_.]+)\b', re.IGNORECASE),
]

# SQL noise — common SQL keywords that appear after FROM and should be excluded
_SQL_NOISE = {
    'information_schema', 'sys', 'dual', 'subquery', 'cte',
    'where', 'join', 'on', 'and', 'or', 'as',
}


def _extract_code(ipynb: dict) -> str:
    """Join source from all code cells into one string."""
    parts = []
    for cell in ipynb.get('cells', []):
        if cell.get('cell_type') == 'code':
            source = cell.get('source', [])
            if isinstance(source, list):
                parts.append(''.join(source))
            elif isinstance(source, str):
                parts.append(source)
    return '\n'.join(parts)


def _clean_ref(ref: str) -> str:
    """Normalize a table reference: strip schema prefix, lowercase."""
    ref = ref.strip().strip('"\'`')
    if '.' in ref:
        # "silver.time_entries" → "time_entries"
        # "silver_time_entries.dbo.time_entries" → "time_entries"
        parts = ref.split('.')
        return parts[-1].lower()
    return ref.lower()


def parse_source(source: str) -> dict[str, list[str]]:
    """
    Parse raw Python/PySpark source code.
    Returns {'reads': [...], 'writes': [...]} with normalized table names.
    """
    reads: set[str] = set()
    writes: set[str] = set()

    for pattern in _WRITE_RE:
        for m in pattern.finditer(source):
            ref = _clean_ref(m.group(1))
            if ref:
                writes.add(ref)

    for pattern in _READ_RE:
        for m in pattern.finditer(source):
            ref = _clean_ref(m.group(1))
            if not ref or ref in _SQL_NOISE or ref.startswith('/'):
                continue
            # Skip file paths (contain slashes) — those go to data sources
            reads.add(ref)

    # Reads that are also writes are internal intermediates; keep in reads for lineage
    return {'reads': sorted(reads - writes), 'writes': sorted(writes)}


def parse_ipynb(path: Path) -> dict[str, list[str]]:
    """Parse a .ipynb file. Returns {'reads': [...], 'writes': [...]}."""
    with open(path, encoding='utf-8') as f:
        nb = json.load(f)
    source = _extract_code(nb)
    return parse_source(source)


def find_local_notebook(display_name: str, notebooks_dir: Path) -> Path | None:
    """Find a .ipynb file in notebooks_dir matching the notebook display name."""
    candidates = [
        notebooks_dir / f'{display_name}.ipynb',
        notebooks_dir / f'{display_name}.py',
    ]
    for path in candidates:
        if path.exists():
            return path
    # Fuzzy: case-insensitive stem match
    if notebooks_dir.exists():
        for f in notebooks_dir.iterdir():
            if f.suffix in ('.ipynb', '.py') and f.stem.lower() == display_name.lower():
                return f
    return None
