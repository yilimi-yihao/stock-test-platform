from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional

from sql_tool.base_database import BaseDatabase

logger = logging.getLogger(__name__)


class IndexConstituentDatabase(BaseDatabase):
    """指数成分变化数据库（以变更事件为主，贴近 CSV 结构）"""

    def __init__(self, db_path: str = 'data/index_constituents.db'):
        super().__init__(db_path)

    def _init_db(self) -> None:
        with self.get_connection() as conn:
            conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS index_entities (
                    index_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    index_name TEXT NOT NULL UNIQUE,
                    index_code TEXT,
                    category TEXT NOT NULL DEFAULT '宽基',
                    benchmark TEXT,
                    source TEXT NOT NULL DEFAULT 'manual_or_imported'
                )
                '''
            )
            for col_sql in (
                'ALTER TABLE index_entities ADD COLUMN index_code TEXT',
                "ALTER TABLE index_entities ADD COLUMN category TEXT NOT NULL DEFAULT '宽基'",
                'ALTER TABLE index_entities ADD COLUMN benchmark TEXT',
                "ALTER TABLE index_entities ADD COLUMN source TEXT NOT NULL DEFAULT 'manual_or_imported'",
            ):
                try:
                    conn.execute(col_sql)
                except Exception:
                    pass

            conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS index_constituent_changes (
                    change_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    index_id INTEGER NOT NULL,
                    announcement_date TEXT,
                    trade_date TEXT NOT NULL,
                    change_type TEXT NOT NULL,
                    code TEXT NOT NULL,
                    name TEXT,
                    source_file TEXT,
                    note TEXT,
                    UNIQUE(index_id, trade_date, change_type, code)
                )
                '''
            )
            for col_sql in (
                'ALTER TABLE index_constituent_changes ADD COLUMN announcement_date TEXT',
                'ALTER TABLE index_constituent_changes ADD COLUMN source_file TEXT',
            ):
                try:
                    conn.execute(col_sql)
                except Exception:
                    pass

            conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS index_constituent_snapshots (
                    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    index_id INTEGER NOT NULL,
                    trade_date TEXT NOT NULL,
                    effective_date TEXT,
                    announcement_date TEXT,
                    note TEXT,
                    source TEXT NOT NULL DEFAULT 'manual_or_imported',
                    UNIQUE(index_id, trade_date)
                )
                '''
            )
            conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS index_constituent_items (
                    item_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_id INTEGER NOT NULL,
                    code TEXT NOT NULL,
                    name TEXT,
                    weight REAL,
                    source TEXT NOT NULL DEFAULT 'manual_or_imported',
                    UNIQUE(snapshot_id, code)
                )
                '''
            )

            conn.execute('CREATE INDEX IF NOT EXISTS idx_index_entities_name ON index_entities(index_name)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_index_entities_category ON index_entities(category)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_index_changes_date ON index_constituent_changes(index_id, trade_date DESC, change_type)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_index_changes_announcement ON index_constituent_changes(index_id, announcement_date DESC)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_index_snapshots_date ON index_constituent_snapshots(index_id, trade_date DESC)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_index_items_code ON index_constituent_items(snapshot_id, code)')

    def upsert_index_entity(self, index_name: str, benchmark: str = '', index_code: str = '', category: str = '宽基', **_kwargs) -> int:
        with self.get_connection() as conn:
            conn.execute(
                '''
                INSERT OR IGNORE INTO index_entities (index_name, index_code, category, benchmark, source)
                VALUES (?, ?, ?, ?, 'manual_or_imported')
                ''',
                (index_name, index_code, category, benchmark),
            )
            conn.execute(
                '''
                UPDATE index_entities
                SET index_code = COALESCE(NULLIF(?, ''), index_code),
                    category = COALESCE(NULLIF(?, ''), category),
                    benchmark = COALESCE(NULLIF(?, ''), benchmark),
                    source = 'manual_or_imported'
                WHERE index_name = ?
                ''',
                (index_code, category, benchmark, index_name),
            )
            row = conn.execute(
                'SELECT index_id FROM index_entities WHERE index_name = ?',
                (index_name,),
            ).fetchone()
            if row is None:
                row = conn.execute(
                    'SELECT index_id FROM index_entities WHERE index_name = ? ORDER BY index_id LIMIT 1',
                    (index_name,),
                ).fetchone()
            return int(row[0])

    def create_snapshot(self, index_id: int, trade_date: str, note: str = '',
                        effective_date: str = '', announcement_date: str = '') -> int:
        with self.get_connection() as conn:
            conn.execute(
                '''
                INSERT OR IGNORE INTO index_constituent_snapshots
                (index_id, trade_date, effective_date, announcement_date, note)
                VALUES (?, ?, ?, ?, ?)
                ''',
                (index_id, trade_date, effective_date or trade_date, announcement_date, note),
            )
            row = conn.execute(
                'SELECT snapshot_id FROM index_constituent_snapshots WHERE index_id = ? AND trade_date = ?',
                (index_id, trade_date),
            ).fetchone()
            return int(row[0])

    def replace_snapshot_items(self, snapshot_id: int, items: list[dict[str, Any]]) -> int:
        with self.get_connection() as conn:
            conn.execute('DELETE FROM index_constituent_items WHERE snapshot_id = ?', (snapshot_id,))
            if items:
                conn.executemany(
                    '''
                    INSERT INTO index_constituent_items (snapshot_id, code, name, weight)
                    VALUES (?, ?, ?, ?)
                    ''',
                    [(snapshot_id, item['code'], item.get('name'), item.get('weight')) for item in items],
                )
            return len(items)

    def replace_changes(self, index_id: int, trade_date: str, changes: list[dict[str, Any]], from_snapshot_id: Optional[int], to_snapshot_id: Optional[int]) -> int:
        with self.get_connection() as conn:
            conn.execute('DELETE FROM index_constituent_changes WHERE index_id = ? AND trade_date = ?', (index_id, trade_date))
            if changes:
                conn.executemany(
                    '''
                    INSERT INTO index_constituent_changes
                    (index_id, announcement_date, trade_date, change_type, code, name, source_file, note)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''',
                    [
                        (
                            index_id,
                            item.get('announcement_date'),
                            trade_date,
                            item['change_type'],
                            item['code'],
                            item.get('name'),
                            item.get('source_file'),
                            item.get('note'),
                        )
                        for item in changes
                    ],
                )
            return len(changes)

    def get_index_entities(self, category: Optional[str] = None) -> list[dict[str, Any]]:
        with self.get_connection() as conn:
            if category:
                return self._rows_to_dicts(conn.execute(
                    'SELECT * FROM index_entities WHERE category = ? ORDER BY index_name', (category,)
                ).fetchall())
            return self._rows_to_dicts(conn.execute('SELECT * FROM index_entities ORDER BY category, index_name').fetchall())

    def get_snapshots(self, index_id: int) -> list[dict[str, Any]]:
        with self.get_connection() as conn:
            return self._rows_to_dicts(conn.execute('SELECT * FROM index_constituent_snapshots WHERE index_id = ? ORDER BY trade_date DESC', (index_id,)).fetchall())

    def get_snapshot_items(self, snapshot_id: int) -> list[dict[str, Any]]:
        with self.get_connection() as conn:
            return self._rows_to_dicts(conn.execute('SELECT * FROM index_constituent_items WHERE snapshot_id = ? ORDER BY code', (snapshot_id,)).fetchall())

    def get_changes(self, index_id: int) -> list[dict[str, Any]]:
        with self.get_connection() as conn:
            return self._rows_to_dicts(conn.execute(
                'SELECT * FROM index_constituent_changes WHERE index_id = ? ORDER BY trade_date DESC, change_type, code',
                (index_id,),
            ).fetchall())

    def get_table_counts(self) -> dict[str, int]:
        table_names = ['index_entities', 'index_constituent_changes', 'index_constituent_snapshots', 'index_constituent_items']
        with self.get_connection() as conn:
            return {table: self._count_table(conn, table) for table in table_names}

    def get_stats(self) -> dict[str, Any]:
        counts = self.get_table_counts()
        db_file = Path(self.db_path)
        return {
            'db_path': str(db_file),
            'db_exists': db_file.exists(),
            'db_size_bytes': db_file.stat().st_size if db_file.exists() else 0,
            'table_counts': counts,
            'index_count': counts['index_entities'],
            'snapshot_count': counts['index_constituent_snapshots'],
            'change_count': counts['index_constituent_changes'],
        }
