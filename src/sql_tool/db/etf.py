from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any, Optional

from sql_tool.base_database import BaseDatabase

logger = logging.getLogger(__name__)


class EtfDatabase(BaseDatabase):
    """ETF 数据库操作类"""

    def __init__(self, db_path: str = 'data/etf.db'):
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
            cursor = conn.cursor()
            cursor.execute(
                '''
                CREATE TABLE IF NOT EXISTS etfs (
                    code TEXT PRIMARY KEY,
                    ts_code TEXT NOT NULL,
                    name TEXT NOT NULL,
                    market TEXT,
                    fund_type TEXT,
                    management TEXT,
                    custodian TEXT,
                    benchmark TEXT,
                    invest_type TEXT,
                    type TEXT,
                    status TEXT,
                    list_date TEXT,
                    due_date TEXT,
                    issue_date TEXT,
                    delist_date TEXT,
                    issue_amount REAL,
                    m_fee REAL,
                    c_fee REAL,
                    duration_year REAL,
                    p_value REAL,
                    min_amount REAL,
                    exp_return REAL,
                    found_date TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                '''
            )
            cursor.execute(
                '''
                CREATE TABLE IF NOT EXISTS etf_daily_prices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL,
                    date TEXT NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    pre_close REAL,
                    change REAL,
                    pct_chg REAL,
                    volume REAL,
                    amount REAL,
                    UNIQUE(code, date),
                    FOREIGN KEY(code) REFERENCES etfs(code)
                )
                '''
            )
            cursor.execute(
                '''
                CREATE TABLE IF NOT EXISTS etf_sync_status (
                    code TEXT NOT NULL,
                    dataset TEXT NOT NULL,
                    status TEXT NOT NULL,
                    last_attempt_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_success_at TIMESTAMP,
                    last_error TEXT,
                    PRIMARY KEY (code, dataset),
                    FOREIGN KEY(code) REFERENCES etfs(code)
                )
                '''
            )
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_etf_name ON etfs(name)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_etf_market ON etfs(market)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_etf_type ON etfs(fund_type)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_etf_daily_code_date ON etf_daily_prices(code, date DESC)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_etf_daily_date ON etf_daily_prices(date DESC)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_etf_sync_dataset ON etf_sync_status(dataset, status)')

    def _rows_to_dicts(self, rows: Iterable[sqlite3.Row]) -> list[dict[str, Any]]:
        return [dict(row) for row in rows]

    def _insert_dataframe(self, conn: sqlite3.Connection, sql: str, rows: Iterable[tuple[Any, ...]]) -> None:
        conn.executemany(sql, list(rows))

    def insert_etf(
        self,
        code: str,
        ts_code: str,
        name: str,
        market: str = '',
        fund_type: str = '',
        management: str = '',
        custodian: str = '',
        benchmark: str = '',
        invest_type: str = '',
        etf_type: str = '',
        status: str = '',
        list_date: str = '',
        due_date: str = '',
        issue_date: str = '',
        delist_date: str = '',
        issue_amount: float = 0,
        m_fee: float = 0,
        c_fee: float = 0,
        duration_year: float = 0,
        p_value: float = 0,
        min_amount: float = 0,
        exp_return: float = 0,
        found_date: str = '',
        conn: Optional[sqlite3.Connection] = None,
    ) -> None:
        owns_conn = conn is None
        conn = conn or self.get_connection()
        try:
            conn.execute(
                '''
                INSERT OR REPLACE INTO etfs (
                    code, ts_code, name, market, fund_type, management, custodian,
                    benchmark, invest_type, type, status, list_date, due_date,
                    issue_date, delist_date, issue_amount, m_fee, c_fee,
                    duration_year, p_value, min_amount, exp_return, found_date, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''',
                (
                    code, ts_code, name, market, fund_type, management, custodian,
                    benchmark, invest_type, etf_type, status, list_date, due_date,
                    issue_date, delist_date, issue_amount, m_fee, c_fee,
                    duration_year, p_value, min_amount, exp_return, found_date,
                ),
            )
            if owns_conn:
                conn.commit()
        finally:
            if owns_conn:
                conn.close()

    def insert_daily_prices(self, code: str, df: Any, conn: Optional[sqlite3.Connection] = None) -> int:
        if df is None or getattr(df, 'empty', True):
            return 0
        owns_conn = conn is None
        conn = conn or self.get_connection()
        try:
            rows = [
                (
                    code,
                    row.get('date'),
                    row.get('open'),
                    row.get('high'),
                    row.get('low'),
                    row.get('close'),
                    row.get('pre_close'),
                    row.get('change'),
                    row.get('pct_chg'),
                    row.get('volume'),
                    row.get('amount'),
                )
                for _, row in df.iterrows()
            ]
            self._insert_dataframe(
                conn,
                '''
                INSERT OR REPLACE INTO etf_daily_prices
                (code, date, open, high, low, close, pre_close, change, pct_chg, volume, amount)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                rows,
            )
            if owns_conn:
                conn.commit()
            return len(rows)
        finally:
            if owns_conn:
                conn.close()

    def set_sync_status(
        self,
        code: str,
        dataset: str,
        status: str,
        last_error: str = '',
        success: bool = False,
        conn: Optional[sqlite3.Connection] = None,
    ) -> None:
        owns_conn = conn is None
        conn = conn or self.get_connection()
        try:
            conn.execute(
                '''
                INSERT INTO etf_sync_status (code, dataset, status, last_attempt_at, last_success_at, last_error)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE NULL END, ?)
                ON CONFLICT(code, dataset) DO UPDATE SET
                    status=excluded.status,
                    last_attempt_at=CURRENT_TIMESTAMP,
                    last_success_at=CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE etf_sync_status.last_success_at END,
                    last_error=excluded.last_error
                ''',
                (code, dataset, status, 1 if success else 0, last_error, 1 if success else 0),
            )
            if owns_conn:
                conn.commit()
        finally:
            if owns_conn:
                conn.close()

    def get_sync_status(self, code: str, dataset: str) -> Optional[dict[str, Any]]:
        try:
            with self.get_connection() as conn:
                row = conn.execute(
                    'SELECT * FROM etf_sync_status WHERE code = ? AND dataset = ?',
                    (code, dataset),
                ).fetchone()
                return dict(row) if row else None
        except Exception:
            logger.exception('获取 ETF 同步状态失败: %s %s', code, dataset)
            return None

    def get_etf_list(self, limit: Optional[int] = None, search: str = '') -> list[dict[str, Any]]:
        page_size = limit if limit is not None else self.count_etfs(search=search)
        page_size = max(page_size, 1)
        return self.get_etf_list_page(page=1, page_size=page_size, search=search)['items']

    def count_etfs(self, search: str = '', fund_type: str = '', market: str = '') -> int:
        try:
            with self.get_connection() as conn:
                query = 'SELECT COUNT(*) FROM etfs'
                clauses: list[str] = []
                params: list[Any] = []
                if search:
                    keyword = search.strip().upper()
                    clauses.append('(name LIKE ? OR management LIKE ? OR code LIKE ? OR ts_code LIKE ?)')
                    fuzzy = f'%{search.strip()}%'
                    params.extend([fuzzy, fuzzy, f'{keyword}%', f'{keyword}%'])
                if fund_type:
                    clauses.append('fund_type = ?')
                    params.append(fund_type)
                if market:
                    clauses.append('market = ?')
                    params.append(market)
                if clauses:
                    query += ' WHERE ' + ' AND '.join(clauses)
                return int(conn.execute(query, params).fetchone()[0])
        except Exception:
            logger.exception('统计 ETF 数失败')
            return 0

    def get_etf_list_page(
        self,
        page: int = 1,
        page_size: int = 200,
        search: str = '',
        fund_type: str = '',
        market: str = '',
        order_by: str = 'code',
        order: str = 'asc',
    ) -> dict[str, Any]:
        try:
            with self.get_connection() as conn:
                allowed_order_by = {'code', 'name', 'market', 'fund_type', 'list_date', 'updated_at'}
                order_by = order_by if order_by in allowed_order_by else 'code'
                order = 'DESC' if order.lower() == 'desc' else 'ASC'
                query = 'SELECT code, ts_code, name, market, fund_type, management, custodian, found_date, list_date, updated_at FROM etfs'
                clauses: list[str] = []
                params: list[Any] = []
                if search:
                    keyword = search.strip().upper()
                    clauses.append('(name LIKE ? OR management LIKE ? OR code LIKE ? OR ts_code LIKE ?)')
                    fuzzy = f'%{search.strip()}%'
                    params.extend([fuzzy, fuzzy, f'{keyword}%', f'{keyword}%'])
                if fund_type:
                    clauses.append('fund_type = ?')
                    params.append(fund_type)
                if market:
                    clauses.append('market = ?')
                    params.append(market)
                if clauses:
                    query += ' WHERE ' + ' AND '.join(clauses)
                query += f' ORDER BY {order_by} {order} LIMIT ? OFFSET ?'
                offset = max(page - 1, 0) * page_size
                rows = conn.execute(query, [*params, page_size, offset]).fetchall()
                total = self.count_etfs(search=search, fund_type=fund_type, market=market)
                return {
                    'items': self._rows_to_dicts(rows),
                    'pagination': {
                        'page': page,
                        'page_size': page_size,
                        'total': total,
                        'pages': (total + page_size - 1) // page_size if page_size else 1,
                    },
                }
        except Exception:
            logger.exception('分页获取 ETF 列表失败')
            return {'items': [], 'pagination': {'page': page, 'page_size': page_size, 'total': 0, 'pages': 0}}

    def get_daily_prices(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        try:
            with self.get_connection() as conn:
                query = 'SELECT * FROM etf_daily_prices WHERE code = ?'
                params: list[Any] = [code]
                if start_date:
                    query += ' AND date >= ?'
                    params.append(start_date)
                if end_date:
                    query += ' AND date <= ?'
                    params.append(end_date)
                query += ' ORDER BY date DESC'
                if limit:
                    query += ' LIMIT ?'
                    params.append(limit)
                rows = conn.execute(query, params).fetchall()
                return self._rows_to_dicts(rows)
        except Exception:
            logger.exception('获取 ETF 日线失败: %s', code)
            return []

    def get_latest_date(self, code: str) -> Optional[str]:
        try:
            with self.get_connection() as conn:
                result = conn.execute('SELECT MAX(date) FROM etf_daily_prices WHERE code = ?', (code,)).fetchone()
                return result[0] if result and result[0] else None
        except Exception:
            logger.exception('获取 ETF 最新日期失败: %s', code)
            return None

    def get_etf_overview(self, code: str) -> Optional[dict[str, Any]]:
        try:
            with self.get_connection() as conn:
                row = conn.execute('SELECT * FROM etfs WHERE code = ?', (code,)).fetchone()
                return dict(row) if row else None
        except Exception:
            logger.exception('获取 ETF 概览失败: %s', code)
            return None

    def get_table_counts(self) -> dict[str, int]:
        table_names = ['etfs', 'etf_daily_prices', 'etf_sync_status']
        try:
            with self.get_connection() as conn:
                return {table: self._count_table(conn, table) for table in table_names}
        except Exception:
            logger.exception('获取 ETF 表计数失败')
            return {table: 0 for table in table_names}

    def get_stats(self) -> dict[str, Any]:
        try:
            counts = self.get_table_counts()
            db_file = Path(self.db_path)
            with self.get_connection() as conn:
                date_range = conn.execute('SELECT MIN(date), MAX(date) FROM etf_daily_prices').fetchone()
                updated_at = conn.execute('SELECT MAX(updated_at) FROM etfs').fetchone()[0]
            return {
                'db_path': str(db_file),
                'db_exists': db_file.exists(),
                'db_size_bytes': db_file.stat().st_size if db_file.exists() else 0,
                'etf_count': counts['etfs'],
                'price_count': counts['etf_daily_prices'],
                'table_counts': counts,
                'latest_etf_update': updated_at,
                'date_range': {
                    'start': date_range[0] if date_range and date_range[0] else None,
                    'end': date_range[1] if date_range and date_range[1] else None,
                },
            }
        except Exception:
            logger.exception('获取 ETF 统计信息失败')
            return {}

    def analyze(self) -> None:
        try:
            with self.get_connection() as conn:
                conn.execute('ANALYZE')
        except Exception:
            logger.exception('ETF ANALYZE 失败')
