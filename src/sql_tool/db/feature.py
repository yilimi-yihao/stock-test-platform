from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any, Iterable, Optional

from sql_tool.base_database import BaseDatabase

logger = logging.getLogger(__name__)


class FeatureDatabase(BaseDatabase):
    """特色数据数据库操作类"""

    def __init__(self, db_path: str = 'data/a_share_features.db'):
        super().__init__(db_path)

    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA foreign_keys = ON')
        conn.execute('PRAGMA journal_mode = WAL')
        conn.execute('PRAGMA synchronous = NORMAL')
        conn.execute('PRAGMA cache_size = -32768')
        return conn

    def _init_db(self) -> None:
        with self.get_connection() as conn:
            conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS concepts (
                    concept_id TEXT PRIMARY KEY,
                    concept_name TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT 'tushare',
                    category TEXT NOT NULL DEFAULT 'concept',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                '''
            )
            conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS stock_concepts (
                    code TEXT NOT NULL,
                    concept_id TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    in_date TEXT,
                    out_date TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (code, concept_id)
                )
                '''
            )
            conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS stock_moneyflow_daily (
                    code TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    buy_sm_vol REAL,
                    buy_md_vol REAL,
                    buy_lg_vol REAL,
                    buy_elg_vol REAL,
                    sell_sm_vol REAL,
                    sell_md_vol REAL,
                    sell_lg_vol REAL,
                    sell_elg_vol REAL,
                    net_mf_vol REAL,
                    net_mf_amount REAL,
                    source TEXT NOT NULL DEFAULT 'tushare',
                    PRIMARY KEY (code, trade_date)
                )
                '''
            )
            conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS top_list_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    close REAL,
                    pct_change REAL,
                    turnover_rate REAL,
                    amount REAL,
                    net_amount REAL,
                    source TEXT NOT NULL DEFAULT 'tushare',
                    UNIQUE (code, trade_date, reason)
                )
                '''
            )
            conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS top_list_traders (
                    trader_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER NOT NULL,
                    broker_name TEXT NOT NULL,
                    direction TEXT NOT NULL CHECK(direction IN ('buy', 'sell')),
                    rank_no INTEGER,
                    amount REAL,
                    net_amount REAL,
                    source TEXT NOT NULL DEFAULT 'tushare',
                    FOREIGN KEY (event_id) REFERENCES top_list_events(event_id) ON DELETE CASCADE
                )
                '''
            )
            conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS feature_sync_jobs (
                    dataset TEXT NOT NULL,
                    scope_type TEXT NOT NULL,
                    scope_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    last_attempt_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_success_at TIMESTAMP,
                    last_error TEXT,
                    row_count INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (dataset, scope_type, scope_key)
                )
                '''
            )
            conn.execute('CREATE INDEX IF NOT EXISTS idx_stock_concepts_concept ON stock_concepts(concept_id, code)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_moneyflow_date ON stock_moneyflow_daily(trade_date DESC, code)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_top_list_code_date ON top_list_events(code, trade_date DESC)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_top_list_traders_event ON top_list_traders(event_id, direction, rank_no)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_feature_sync_jobs_status ON feature_sync_jobs(dataset, status)')

    def _rows_to_dicts(self, rows: Iterable[sqlite3.Row]) -> list[dict[str, Any]]:
        return [dict(row) for row in rows]

    def insert_concepts(self, items: list[dict[str, Any]], conn: Optional[sqlite3.Connection] = None) -> int:
        if not items:
            return 0
        owns_conn = conn is None
        conn = conn or self.get_connection()
        try:
            conn.executemany(
                '''
                INSERT OR REPLACE INTO concepts (concept_id, concept_name, source, category, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''',
                [
                    (
                        item.get('concept_id'),
                        item.get('concept_name'),
                        item.get('source', 'tushare'),
                        item.get('category', 'concept'),
                    )
                    for item in items
                ],
            )
            if owns_conn:
                conn.commit()
            return len(items)
        finally:
            if owns_conn:
                conn.close()

    def replace_stock_concepts(self, concept_id: str, members: list[dict[str, Any]], conn: Optional[sqlite3.Connection] = None) -> int:
        owns_conn = conn is None
        conn = conn or self.get_connection()
        try:
            conn.execute('DELETE FROM stock_concepts WHERE concept_id = ?', (concept_id,))
            if members:
                conn.executemany(
                    '''
                    INSERT OR REPLACE INTO stock_concepts
                    (code, concept_id, is_active, in_date, out_date, updated_at)
                    VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ''',
                    [
                        (
                            item.get('code'),
                            concept_id,
                            int(item.get('is_active', 1)),
                            item.get('in_date'),
                            item.get('out_date'),
                        )
                        for item in members
                    ],
                )
            if owns_conn:
                conn.commit()
            return len(members)
        finally:
            if owns_conn:
                conn.close()

    def insert_moneyflow(self, rows: list[dict[str, Any]], conn: Optional[sqlite3.Connection] = None) -> int:
        if not rows:
            return 0
        owns_conn = conn is None
        conn = conn or self.get_connection()
        try:
            conn.executemany(
                '''
                INSERT OR REPLACE INTO stock_moneyflow_daily
                (code, trade_date, buy_sm_vol, buy_md_vol, buy_lg_vol, buy_elg_vol,
                 sell_sm_vol, sell_md_vol, sell_lg_vol, sell_elg_vol, net_mf_vol, net_mf_amount, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                [
                    (
                        item.get('code'),
                        item.get('trade_date'),
                        item.get('buy_sm_vol'),
                        item.get('buy_md_vol'),
                        item.get('buy_lg_vol'),
                        item.get('buy_elg_vol'),
                        item.get('sell_sm_vol'),
                        item.get('sell_md_vol'),
                        item.get('sell_lg_vol'),
                        item.get('sell_elg_vol'),
                        item.get('net_mf_vol'),
                        item.get('net_mf_amount'),
                        item.get('source', 'tushare'),
                    )
                    for item in rows
                ],
            )
            if owns_conn:
                conn.commit()
            return len(rows)
        finally:
            if owns_conn:
                conn.close()

    def insert_top_list_events(self, rows: list[dict[str, Any]], conn: Optional[sqlite3.Connection] = None) -> list[int]:
        if not rows:
            return []
        owns_conn = conn is None
        conn = conn or self.get_connection()
        try:
            conn.executemany(
                '''
                INSERT OR REPLACE INTO top_list_events
                (event_id, code, trade_date, reason, close, pct_change, turnover_rate, amount, net_amount, source)
                VALUES (
                    (SELECT event_id FROM top_list_events WHERE code = ? AND trade_date = ? AND reason = ?),
                    ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                ''',
                [
                    (
                        item.get('code'),
                        item.get('trade_date'),
                        item.get('reason'),
                        item.get('code'),
                        item.get('trade_date'),
                        item.get('reason'),
                        item.get('close'),
                        item.get('pct_change'),
                        item.get('turnover_rate'),
                        item.get('amount'),
                        item.get('net_amount'),
                        item.get('source', 'tushare'),
                    )
                    for item in rows
                ],
            )
            event_ids = [
                conn.execute(
                    'SELECT event_id FROM top_list_events WHERE code = ? AND trade_date = ? AND reason = ?',
                    (item.get('code'), item.get('trade_date'), item.get('reason')),
                ).fetchone()[0]
                for item in rows
            ]
            if owns_conn:
                conn.commit()
            return event_ids
        finally:
            if owns_conn:
                conn.close()

    def replace_top_list_traders(self, event_id: int, rows: list[dict[str, Any]], conn: Optional[sqlite3.Connection] = None) -> int:
        owns_conn = conn is None
        conn = conn or self.get_connection()
        try:
            conn.execute('DELETE FROM top_list_traders WHERE event_id = ?', (event_id,))
            if rows:
                conn.executemany(
                    '''
                    INSERT INTO top_list_traders
                    (event_id, broker_name, direction, rank_no, amount, net_amount, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''',
                    [
                        (
                            event_id,
                            item.get('broker_name'),
                            item.get('direction'),
                            item.get('rank_no'),
                            item.get('amount'),
                            item.get('net_amount'),
                            item.get('source', 'tushare'),
                        )
                        for item in rows
                    ],
                )
            if owns_conn:
                conn.commit()
            return len(rows)
        finally:
            if owns_conn:
                conn.close()

    def set_sync_status(
        self,
        dataset: str,
        scope_type: str,
        scope_key: str,
        status: str,
        row_count: int = 0,
        last_error: str = '',
        success: bool = False,
        conn: Optional[sqlite3.Connection] = None,
    ) -> None:
        owns_conn = conn is None
        conn = conn or self.get_connection()
        try:
            conn.execute(
                '''
                INSERT INTO feature_sync_jobs (dataset, scope_type, scope_key, status, last_attempt_at, last_success_at, last_error, row_count)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE NULL END, ?, ?)
                ON CONFLICT(dataset, scope_type, scope_key) DO UPDATE SET
                    status=excluded.status,
                    last_attempt_at=CURRENT_TIMESTAMP,
                    last_success_at=CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE feature_sync_jobs.last_success_at END,
                    last_error=excluded.last_error,
                    row_count=excluded.row_count
                ''',
                (dataset, scope_type, scope_key, status, 1 if success else 0, last_error, row_count, 1 if success else 0),
            )
            if owns_conn:
                conn.commit()
        finally:
            if owns_conn:
                conn.close()

    def get_sync_status(self, dataset: str, scope_type: str, scope_key: str) -> Optional[dict[str, Any]]:
        try:
            with self.get_connection() as conn:
                row = conn.execute(
                    'SELECT * FROM feature_sync_jobs WHERE dataset = ? AND scope_type = ? AND scope_key = ?',
                    (dataset, scope_type, scope_key),
                ).fetchone()
                return dict(row) if row else None
        except Exception:
            logger.exception('获取特色同步状态失败: %s %s %s', dataset, scope_type, scope_key)
            return None

    def get_concepts(self) -> list[dict[str, Any]]:
        try:
            with self.get_connection() as conn:
                rows = conn.execute('SELECT * FROM concepts ORDER BY concept_name').fetchall()
                return self._rows_to_dicts(rows)
        except Exception:
            logger.exception('获取概念列表失败')
            return []

    def get_concept_members(self, concept_id: str) -> list[dict[str, Any]]:
        try:
            with self.get_connection() as conn:
                rows = conn.execute(
                    'SELECT * FROM stock_concepts WHERE concept_id = ? ORDER BY code',
                    (concept_id,),
                ).fetchall()
                return self._rows_to_dicts(rows)
        except Exception:
            logger.exception('获取概念成分失败: %s', concept_id)
            return []

    def get_concepts_for_code(self, code: str) -> list[dict[str, Any]]:
        try:
            with self.get_connection() as conn:
                rows = conn.execute(
                    '''
                    SELECT c.*
                    FROM concepts c
                    INNER JOIN stock_concepts sc ON sc.concept_id = c.concept_id
                    WHERE sc.code = ?
                    ORDER BY c.concept_name
                    ''',
                    (code,),
                ).fetchall()
                return self._rows_to_dicts(rows)
        except Exception:
            logger.exception('按股票获取概念失败: %s', code)
            return []

    def get_moneyflow(self, code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> list[dict[str, Any]]:
        try:
            with self.get_connection() as conn:
                query = 'SELECT * FROM stock_moneyflow_daily WHERE code = ?'
                params: list[Any] = [code]
                if start_date:
                    query += ' AND trade_date >= ?'
                    params.append(start_date)
                if end_date:
                    query += ' AND trade_date <= ?'
                    params.append(end_date)
                query += ' ORDER BY trade_date DESC'
                rows = conn.execute(query, params).fetchall()
                return self._rows_to_dicts(rows)
        except Exception:
            logger.exception('获取资金流向失败: %s', code)
            return []


    def get_top_list(self, code: Optional[str] = None, trade_date: Optional[str] = None) -> list[dict[str, Any]]:
        try:
            with self.get_connection() as conn:
                query = 'SELECT * FROM top_list_events WHERE 1=1'
                params: list[Any] = []
                if code:
                    query += ' AND code = ?'
                    params.append(code)
                if trade_date:
                    query += ' AND trade_date = ?'
                    params.append(trade_date)
                query += ' ORDER BY trade_date DESC, code'
                rows = conn.execute(query, params).fetchall()
                return self._rows_to_dicts(rows)
        except Exception:
            logger.exception('获取龙虎榜失败')
            return []

    def get_top_list_traders(self, event_id: int) -> list[dict[str, Any]]:
        try:
            with self.get_connection() as conn:
                rows = conn.execute(
                    'SELECT * FROM top_list_traders WHERE event_id = ? ORDER BY direction, rank_no',
                    (event_id,),
                ).fetchall()
                return self._rows_to_dicts(rows)
        except Exception:
            logger.exception('获取龙虎榜席位失败: %s', event_id)
            return []

    def get_table_counts(self) -> dict[str, int]:
        table_names = ['concepts', 'stock_concepts', 'stock_moneyflow_daily', 'top_list_events', 'top_list_traders', 'feature_sync_jobs']
        try:
            with self.get_connection() as conn:
                return {table: self._count_table(conn, table) for table in table_names}
        except Exception:
            logger.exception('获取特色表计数失败')
            return {table: 0 for table in table_names}

    def get_stats(self) -> dict[str, Any]:
        try:
            counts = self.get_table_counts()
            db_file = Path(self.db_path)
            return {
                'db_path': str(db_file),
                'db_exists': db_file.exists(),
                'db_size_bytes': db_file.stat().st_size if db_file.exists() else 0,
                'table_counts': counts,
                'concept_count': counts['concepts'],
                'moneyflow_count': counts['stock_moneyflow_daily'],
                'top_list_event_count': counts['top_list_events'],
            }
        except Exception:
            logger.exception('获取特色统计信息失败')
            return {}

    def analyze(self) -> None:
        try:
            with self.get_connection() as conn:
                conn.execute('ANALYZE')
        except Exception:
            logger.exception('Feature ANALYZE 失败')
