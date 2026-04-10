"""SQLite 数据库基类"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any, Iterable, Optional

logger = logging.getLogger(__name__)


class BaseDatabase:
    """所有本地 SQLite 数据库的公共基类，统一连接管理与基础读写工具。"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA foreign_keys = ON')
        conn.execute('PRAGMA journal_mode = WAL')
        conn.execute('PRAGMA synchronous = NORMAL')
        conn.execute('PRAGMA cache_size = -32768')
        return conn

    def _init_db(self) -> None:
        raise NotImplementedError

    def _rows_to_dicts(self, rows: Iterable[sqlite3.Row]) -> list[dict[str, Any]]:
        return [dict(row) for row in rows]

    def _insert_dataframe(
        self,
        conn: sqlite3.Connection,
        sql: str,
        rows: Iterable[tuple[Any, ...]],
    ) -> None:
        conn.executemany(sql, list(rows))

    def _count_table(self, conn: sqlite3.Connection, table: str) -> int:
        try:
            stat = conn.execute(
                "SELECT stat FROM sqlite_stat1 WHERE tbl = ?", (table,)
            ).fetchone()
            if stat:
                return int(stat[0].split()[0])
        except Exception:
            pass
        return conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]

    def list_tables(self) -> list[str]:
        with self.get_connection() as conn:
            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
            ).fetchall()
            return [str(row[0]) for row in rows]

    def get_table_columns(self, table: str) -> list[str]:
        if table not in self.list_tables():
            raise ValueError(f'未知数据表: {table}')
        with self.get_connection() as conn:
            rows = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
            return [str(row[1]) for row in rows]

    def get_table_rows(self, table: str, limit: int = 200) -> list[dict[str, Any]]:
        if table not in self.list_tables():
            raise ValueError(f'未知数据表: {table}')
        safe_limit = max(1, min(int(limit), 500))
        with self.get_connection() as conn:
            rows = conn.execute(f'SELECT * FROM "{table}" LIMIT ?', (safe_limit,)).fetchall()
            return self._rows_to_dicts(rows)

    def analyze(self) -> None:
        try:
            with self.get_connection() as conn:
                conn.execute('ANALYZE')
        except Exception:
            logger.exception('ANALYZE 失败: %s', self.db_path)
