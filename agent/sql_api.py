"""
Queries the Fabric SQL analytics endpoint (lakehouse or warehouse) via pyodbc.
One SQLClient per database (lakehouse/warehouse display name).
"""
import struct
from typing import Optional
import pyodbc

from .auth import get_token
from .config import SQL_RESOURCE, TIMESTAMP_COLUMNS


def _build_token_struct() -> bytes:
    token = get_token(SQL_RESOURCE)
    token_bytes = token.encode('utf-16-le')
    return struct.pack('=i', len(token_bytes)) + token_bytes


class SQLClient:
    SQL_COPT_SS_ACCESS_TOKEN = 1256

    def __init__(self, server: str, database: str):
        self.server = server
        self.database = database
        self._conn: Optional[pyodbc.Connection] = None

    def _connect(self) -> pyodbc.Connection:
        if self._conn is None:
            conn_str = (
                f'DRIVER={{ODBC Driver 18 for SQL Server}};'
                f'SERVER={self.server};'
                f'DATABASE={self.database};'
                f'Encrypt=yes;'
                f'Connection Timeout=30;'
            )
            self._conn = pyodbc.connect(
                conn_str,
                attrs_before={self.SQL_COPT_SS_ACCESS_TOKEN: _build_token_struct()},
                timeout=30,
                autocommit=True,
            )
            self._conn.timeout = 120
        return self._conn

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def get_tables(self) -> list[dict]:
        """Returns list of {schema, table_name}."""
        cur = self._connect().cursor()
        cur.execute(
            "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME"
        )
        return [{'schema': r[0], 'table_name': r[1]} for r in cur.fetchall()]

    def get_columns(self, schema: str, table: str) -> list[dict]:
        """Returns list of {col_name, datatype, is_nullable}."""
        cur = self._connect().cursor()
        cur.execute(
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
            "ORDER BY ORDINAL_POSITION",
            schema, table,
        )
        return [
            {'col_name': r[0], 'datatype': r[1], 'is_nullable': r[2] == 'YES'}
            for r in cur.fetchall()
        ]

    def get_row_count(self, schema: str, table: str) -> Optional[int]:
        try:
            cur = self._connect().cursor()
            cur.execute(f'SELECT COUNT(*) FROM [{schema}].[{table}]')
            return cur.fetchone()[0]
        except Exception:
            return None

    def get_column_stats(self, schema: str, table: str, col_names: list[str]) -> dict:
        """
        Returns {col_name: {pct_null: float, sample: str}} for all columns in one pass.
        Uses conditional aggregation (one query per table, not per column).
        """
        if not col_names:
            return {}

        null_exprs = ', '.join(
            f'CAST(SUM(CASE WHEN [{c}] IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,1)) AS [null_{i}]'
            for i, c in enumerate(col_names)
        )
        sample_exprs = ', '.join(
            f'(SELECT TOP 1 CAST([{c}] AS NVARCHAR(200)) FROM [{schema}].[{table}] WHERE [{c}] IS NOT NULL) AS [samp_{i}]'
            for i, c in enumerate(col_names)
        )

        stats: dict[str, dict] = {c: {'pct_null': '', 'sample': ''} for c in col_names}

        try:
            cur = self._connect().cursor()

            cur.execute(f'SELECT {null_exprs} FROM [{schema}].[{table}]')
            null_row = cur.fetchone()
            if null_row:
                for i, col in enumerate(col_names):
                    val = null_row[i]
                    stats[col]['pct_null'] = str(val) if val is not None else ''

            cur.execute(f'SELECT {sample_exprs}')
            samp_row = cur.fetchone()
            if samp_row:
                for i, col in enumerate(col_names):
                    val = samp_row[i]
                    stats[col]['sample'] = str(val) if val is not None else ''

        except Exception as e:
            # Partial failure is fine — we already zeroed the dict
            pass

        return stats

    def get_last_updated(self, schema: str, table: str, col_names: list[str]) -> str:
        """
        Returns MAX of the first recognized timestamp column, or empty string.
        """
        ts_col = next(
            (c for c in col_names if c.lower() in TIMESTAMP_COLUMNS),
            None,
        )
        if not ts_col:
            return ''
        try:
            cur = self._connect().cursor()
            cur.execute(f'SELECT MAX([{ts_col}]) FROM [{schema}].[{table}]')
            val = cur.fetchone()[0]
            return str(val) if val is not None else ''
        except Exception:
            return ''
