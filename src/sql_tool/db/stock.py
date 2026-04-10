"""数据库操作模块"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any, Iterable, Optional

from sql_tool.base_database import BaseDatabase

logger = logging.getLogger(__name__)


class StockDatabase(BaseDatabase):
    """A 股数据库操作类"""

    def __init__(self, db_path: str = 'data/a_share.db'):
        super().__init__(db_path)

    def get_connection(self) -> sqlite3.Connection:
        """获取数据库连接"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA foreign_keys = ON')
        conn.execute('PRAGMA journal_mode = WAL')
        conn.execute('PRAGMA synchronous = NORMAL')
        conn.execute('PRAGMA cache_size = -32768')
        return conn

    def _init_db(self) -> None:
        """初始化数据库表"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute(
                '''
                CREATE TABLE IF NOT EXISTS stocks (
                    code TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    area TEXT,
                    industry TEXT,
                    list_date TEXT,
                    market_cap REAL,
                    circ_mv REAL,
                    pe_ratio REAL,
                    pb_ratio REAL,
                    turnover_rate REAL,
                    volume_ratio REAL,
                    adj_factor REAL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                '''
            )

            cursor.execute(
                '''
                CREATE TABLE IF NOT EXISTS daily_prices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL,
                    date TEXT NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume INTEGER NOT NULL,
                    amount REAL,
                    UNIQUE(code, date),
                    FOREIGN KEY(code) REFERENCES stocks(code)
                )
                '''
            )

            cursor.execute(
                '''
                CREATE TABLE IF NOT EXISTS fina_indicator (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL,
                    ann_date TEXT,
                    end_date TEXT NOT NULL,
                    eps REAL,
                    roe REAL,
                    roa REAL,
                    gross_margin REAL,
                    net_margin REAL,
                    debt_to_assets REAL,
                    current_ratio REAL,
                    quick_ratio REAL,
                    UNIQUE(code, end_date),
                    FOREIGN KEY(code) REFERENCES stocks(code)
                )
                '''
            )

            cursor.execute(
                '''
                CREATE TABLE IF NOT EXISTS income (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL,
                    ann_date TEXT,
                    end_date TEXT NOT NULL,
                    revenue REAL,
                    operate_profit REAL,
                    net_profit REAL,
                    UNIQUE(code, end_date),
                    FOREIGN KEY(code) REFERENCES stocks(code)
                )
                '''
            )

            cursor.execute(
                '''
                CREATE TABLE IF NOT EXISTS balancesheet (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL,
                    ann_date TEXT,
                    end_date TEXT NOT NULL,
                    total_assets REAL,
                    total_liab REAL,
                    total_equity REAL,
                    current_assets REAL,
                    current_liab REAL,
                    cash REAL,
                    accounts_payable REAL,
                    advance_receipts REAL,
                    UNIQUE(code, end_date),
                    FOREIGN KEY(code) REFERENCES stocks(code)
                )
                '''
            )

            cursor.execute(
                '''
                CREATE TABLE IF NOT EXISTS cashflow (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL,
                    ann_date TEXT,
                    end_date TEXT NOT NULL,
                    operate_cash_flow REAL,
                    invest_cash_flow REAL,
                    finance_cash_flow REAL,
                    UNIQUE(code, end_date),
                    FOREIGN KEY(code) REFERENCES stocks(code)
                )
                '''
            )

            cursor.execute(
                '''
                CREATE TABLE IF NOT EXISTS sync_status (
                    code TEXT NOT NULL,
                    dataset TEXT NOT NULL,
                    status TEXT NOT NULL,
                    last_attempt_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_success_at TIMESTAMP,
                    last_error TEXT,
                    PRIMARY KEY (code, dataset),
                    FOREIGN KEY(code) REFERENCES stocks(code)
                )
                '''
            )

            cursor.execute('CREATE INDEX IF NOT EXISTS idx_daily_code_date ON daily_prices(code, date DESC)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_daily_date ON daily_prices(date DESC)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_stocks_name ON stocks(name)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_stocks_industry ON stocks(industry)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_fina_code_date ON fina_indicator(code, end_date DESC)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_income_code_date ON income(code, end_date DESC)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_bs_code_date ON balancesheet(code, end_date DESC)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_cf_code_date ON cashflow(code, end_date DESC)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sync_status_dataset ON sync_status(dataset, status)')

        logger.info('数据库初始化完成')

    def insert_stock(
        self,
        code: str,
        name: str,
        area: str = '',
        industry: str = '',
        list_date: str = '',
        market_cap: float = 0,
        circ_mv: float = 0,
        pe_ratio: float = 0,
        pb_ratio: float = 0,
        turnover_rate: float = 0,
        volume_ratio: float = 0,
        adj_factor: float = 0,
        conn: Optional[sqlite3.Connection] = None,
    ) -> None:
        """插入或更新股票信息"""
        owns_conn = conn is None
        conn = conn or self.get_connection()
        try:
            conn.execute(
                '''
                INSERT OR REPLACE INTO stocks
                (code, name, area, industry, list_date, market_cap, circ_mv,
                 pe_ratio, pb_ratio, turnover_rate, volume_ratio, adj_factor, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''',
                (
                    code,
                    name,
                    area,
                    industry,
                    list_date,
                    market_cap,
                    circ_mv,
                    pe_ratio,
                    pb_ratio,
                    turnover_rate,
                    volume_ratio,
                    adj_factor,
                ),
            )
            if owns_conn:
                conn.commit()
            logger.debug('插入股票: %s - %s', code, name)
        except Exception:
            logger.exception('插入股票失败: %s', code)
            raise
        finally:
            if owns_conn:
                conn.close()

    def insert_daily_prices(self, code: str, df: Any, conn: Optional[sqlite3.Connection] = None) -> int:
        """批量插入日线数据"""
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
                    row.get('volume'),
                    row.get('amount'),
                )
                for _, row in df.iterrows()
            ]
            self._insert_dataframe(
                conn,
                '''
                INSERT OR REPLACE INTO daily_prices
                (code, date, open, high, low, close, volume, amount)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                rows,
            )
            if owns_conn:
                conn.commit()
            logger.debug('插入 %s 条日线数据: %s', len(rows), code)
            return len(rows)
        except Exception:
            logger.exception('插入日线数据失败: %s', code)
            raise
        finally:
            if owns_conn:
                conn.close()

    def insert_fina_indicator(self, code: str, df: Any, conn: Optional[sqlite3.Connection] = None) -> int:
        """批量插入财务指标数据"""
        return self._insert_financial_df(
            code,
            df,
            '''
            INSERT OR REPLACE INTO fina_indicator
            (code, ann_date, end_date, eps, roe, roa, gross_margin, net_margin,
             debt_to_assets, current_ratio, quick_ratio)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            ['ann_date', 'end_date', 'eps', 'roe', 'roa', 'gross_margin', 'net_margin', 'debt_to_assets', 'current_ratio', 'quick_ratio'],
            conn,
        )

    def insert_income(self, code: str, df: Any, conn: Optional[sqlite3.Connection] = None) -> int:
        """批量插入利润表数据"""
        return self._insert_financial_df(
            code,
            df,
            '''
            INSERT OR REPLACE INTO income
            (code, ann_date, end_date, revenue, operate_profit, net_profit)
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            ['ann_date', 'end_date', 'revenue', 'operate_profit', 'net_profit'],
            conn,
        )

    def insert_balancesheet(self, code: str, df: Any, conn: Optional[sqlite3.Connection] = None) -> int:
        """批量插入资产负债表数据"""
        return self._insert_financial_df(
            code,
            df,
            '''
            INSERT OR REPLACE INTO balancesheet
            (code, ann_date, end_date, total_assets, total_liab, total_equity,
             current_assets, current_liab, cash, accounts_payable, advance_receipts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            ['ann_date', 'end_date', 'total_assets', 'total_liab', 'total_equity', 'current_assets', 'current_liab', 'cash', 'accounts_payable', 'advance_receipts'],
            conn,
        )

    def insert_cashflow(self, code: str, df: Any, conn: Optional[sqlite3.Connection] = None) -> int:
        """批量插入现金流量表数据"""
        return self._insert_financial_df(
            code,
            df,
            '''
            INSERT OR REPLACE INTO cashflow
            (code, ann_date, end_date, operate_cash_flow, invest_cash_flow, finance_cash_flow)
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            ['ann_date', 'end_date', 'operate_cash_flow', 'invest_cash_flow', 'finance_cash_flow'],
            conn,
        )

    def _insert_financial_df(
        self,
        code: str,
        df: Any,
        sql: str,
        fields: list[str],
        conn: Optional[sqlite3.Connection] = None,
    ) -> int:
        if df is None or getattr(df, 'empty', True):
            return 0

        owns_conn = conn is None
        conn = conn or self.get_connection()
        try:
            rows = [(code, *[row.get(field) for field in fields]) for _, row in df.iterrows()]
            self._insert_dataframe(conn, sql, rows)
            if owns_conn:
                conn.commit()
            return len(rows)
        except Exception:
            logger.exception('插入财务数据失败: %s', code)
            raise
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
                INSERT INTO sync_status (code, dataset, status, last_attempt_at, last_success_at, last_error)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE NULL END, ?)
                ON CONFLICT(code, dataset) DO UPDATE SET
                    status=excluded.status,
                    last_attempt_at=CURRENT_TIMESTAMP,
                    last_success_at=CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE sync_status.last_success_at END,
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
                    'SELECT * FROM sync_status WHERE code = ? AND dataset = ?',
                    (code, dataset),
                ).fetchone()
                return dict(row) if row else None
        except Exception:
            logger.exception('获取同步状态失败: %s %s', code, dataset)
            return None

    def list_sync_status(self, code: str) -> list[dict[str, Any]]:
        try:
            with self.get_connection() as conn:
                rows = conn.execute('SELECT * FROM sync_status WHERE code = ? ORDER BY dataset', (code,)).fetchall()
                return self._rows_to_dicts(rows)
        except Exception:
            logger.exception('列出同步状态失败: %s', code)
            return []

    def get_stock_list(self, limit: Optional[int] = None, search: str = '') -> list[dict[str, Any]]:
        """获取股票列表"""
        page_size = limit if limit is not None else self.count_stocks(search=search)
        page_size = max(page_size, 1)
        return self.get_stock_list_page(page=1, page_size=page_size, search=search)['items']

    def count_stocks(self, search: str = '', industry: str = '', area: str = '') -> int:
        try:
            with self.get_connection() as conn:
                query = 'SELECT COUNT(*) FROM stocks'
                clauses: list[str] = []
                params: list[Any] = []
                if search:
                    keyword = search.strip().upper()
                    if keyword.isdigit():
                        clauses.append('code LIKE ?')
                        params.append(f'{keyword}%')
                    else:
                        clauses.append('(name LIKE ? OR industry LIKE ? OR code LIKE ?)')
                        fuzzy = f'%{search.strip()}%'
                        params.extend([fuzzy, fuzzy, f'{keyword}%'])
                if industry:
                    clauses.append('industry = ?')
                    params.append(industry)
                if area:
                    clauses.append('area = ?')
                    params.append(area)
                if clauses:
                    query += ' WHERE ' + ' AND '.join(clauses)
                return int(conn.execute(query, params).fetchone()[0])
        except Exception:
            logger.exception('统计股票数失败')
            return 0

    def get_stock_list_page(
        self,
        page: int = 1,
        page_size: int = 200,
        search: str = '',
        industry: str = '',
        area: str = '',
        order_by: str = 'code',
        order: str = 'asc',
    ) -> dict[str, Any]:
        try:
            with self.get_connection() as conn:
                allowed_order_by = {'code', 'name', 'industry', 'area', 'list_date', 'updated_at'}
                order_by = order_by if order_by in allowed_order_by else 'code'
                order = 'DESC' if order.lower() == 'desc' else 'ASC'
                query = 'SELECT code, name, industry, area, list_date, updated_at FROM stocks'
                clauses: list[str] = []
                params: list[Any] = []
                if search:
                    keyword = search.strip().upper()
                    if keyword.isdigit():
                        clauses.append('code LIKE ?')
                        params.append(f'{keyword}%')
                    else:
                        clauses.append('(name LIKE ? OR industry LIKE ? OR code LIKE ?)')
                        fuzzy = f'%{search.strip()}%'
                        params.extend([fuzzy, fuzzy, f'{keyword}%'])
                if industry:
                    clauses.append('industry = ?')
                    params.append(industry)
                if area:
                    clauses.append('area = ?')
                    params.append(area)
                if clauses:
                    query += ' WHERE ' + ' AND '.join(clauses)
                query += f' ORDER BY {order_by} {order} LIMIT ? OFFSET ?'
                offset = max(page - 1, 0) * page_size
                rows = conn.execute(query, [*params, page_size, offset]).fetchall()
                total = self.count_stocks(search=search, industry=industry, area=area)
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
            logger.exception('分页获取股票列表失败')
            return {'items': [], 'pagination': {'page': page, 'page_size': page_size, 'total': 0, 'pages': 0}}

    def get_daily_prices_batch(
        self,
        codes: list[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit_per_code: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        if not codes:
            return []
        try:
            with self.get_connection() as conn:
                normalized_codes = [code.strip().upper().split('.')[0] for code in codes if code.strip()]
                placeholders = ','.join('?' * len(normalized_codes))
                params: list[Any] = [*normalized_codes]
                if limit_per_code:
                    query = f'''
                        SELECT code, date, open, high, low, close, volume, amount
                        FROM (
                            SELECT code, date, open, high, low, close, volume, amount,
                                   ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) AS rn
                            FROM daily_prices
                            WHERE code IN ({placeholders})
                    '''
                    if start_date:
                        query += ' AND date >= ?'
                        params.append(start_date)
                    if end_date:
                        query += ' AND date <= ?'
                        params.append(end_date)
                    query += ') WHERE rn <= ? ORDER BY code, date'
                    params.append(limit_per_code)
                else:
                    query = f'SELECT code, date, open, high, low, close, volume, amount FROM daily_prices WHERE code IN ({placeholders})'
                    if start_date:
                        query += ' AND date >= ?'
                        params.append(start_date)
                    if end_date:
                        query += ' AND date <= ?'
                        params.append(end_date)
                    query += ' ORDER BY code, date'
                rows = conn.execute(query, params).fetchall()
                return self._rows_to_dicts(rows)
        except Exception:
            logger.exception('批量获取日线数据失败')
            return []

    def get_stock_overviews(self, codes: list[str]) -> list[dict[str, Any]]:
        if not codes:
            return []
        try:
            with self.get_connection() as conn:
                normalized_codes = [code.strip().upper().split('.')[0] for code in codes if code.strip()]
                placeholders = ','.join('?' * len(normalized_codes))
                query = f'''
                    SELECT s.code, s.name, s.industry, s.area, s.list_date,
                           s.market_cap, s.circ_mv, s.pe_ratio, s.pb_ratio,
                           s.turnover_rate, s.volume_ratio, s.adj_factor,
                           d.date AS latest_date, d.close AS latest_close,
                           d.volume AS latest_volume, d.amount AS latest_amount
                    FROM stocks s
                    LEFT JOIN (
                        SELECT dp1.code, dp1.date, dp1.close, dp1.volume, dp1.amount
                        FROM daily_prices dp1
                        INNER JOIN (
                            SELECT code, MAX(date) AS max_date
                            FROM daily_prices
                            WHERE code IN ({placeholders})
                            GROUP BY code
                        ) latest ON latest.code = dp1.code AND latest.max_date = dp1.date
                    ) d ON d.code = s.code
                    WHERE s.code IN ({placeholders})
                    ORDER BY s.code
                '''
                rows = conn.execute(query, [*normalized_codes, *normalized_codes]).fetchall()
                return self._rows_to_dicts(rows)
        except Exception:
            logger.exception('批量获取股票概览失败')
            return []

    def get_daily_prices(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """获取日线数据"""
        try:
            with self.get_connection() as conn:
                query = 'SELECT * FROM daily_prices WHERE code = ?'
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
            logger.exception('获取日线数据失败: %s', code)
            return []

    def get_latest_date(self, code: str) -> Optional[str]:
        """获取某只股票的最新日期"""
        try:
            with self.get_connection() as conn:
                result = conn.execute('SELECT MAX(date) FROM daily_prices WHERE code = ?', (code,)).fetchone()
                return result[0] if result and result[0] else None
        except Exception:
            logger.exception('获取最新日期失败: %s', code)
            return None

    def get_table_counts(self) -> dict[str, int]:
        """获取各表行数（用 sqlite_stat1 近似值加速，不存在时降级为 COUNT）"""
        table_names = ['stocks', 'daily_prices', 'fina_indicator', 'income', 'balancesheet', 'cashflow', 'sync_status']
        try:
            with self.get_connection() as conn:
                # 尝试从 sqlite_stat1 读取近似行数（ANALYZE 后才有）
                try:
                    stat_rows = conn.execute(
                        "SELECT tbl, stat FROM sqlite_stat1 WHERE tbl IN ({})".format(
                            ','.join('?' * len(table_names))
                        ),
                        table_names,
                    ).fetchall()
                    stat_map = {row[0]: int(row[1].split()[0]) for row in stat_rows}
                except Exception:
                    stat_map = {}

                result = {}
                for table in table_names:
                    if table in stat_map:
                        result[table] = stat_map[table]
                    else:
                        result[table] = conn.execute(
                            f'SELECT COUNT(*) FROM {table}'
                        ).fetchone()[0]
                return result
        except Exception:
            logger.exception('获取表计数失败')
            return {table: 0 for table in table_names}

    def get_stats(self) -> dict[str, Any]:
        """获取数据库统计信息"""
        try:
            counts = self.get_table_counts()
            db_file = Path(self.db_path)
            with self.get_connection() as conn:
                date_range = conn.execute('SELECT MIN(date), MAX(date) FROM daily_prices').fetchone()
                updated_at = conn.execute('SELECT MAX(updated_at) FROM stocks').fetchone()[0]

            return {
                'db_path': str(db_file),
                'db_exists': db_file.exists(),
                'db_size_bytes': db_file.stat().st_size if db_file.exists() else 0,
                'stock_count': counts['stocks'],
                'price_count': counts['daily_prices'],
                'table_counts': counts,
                'latest_stock_update': updated_at,
                'date_range': {
                    'start': date_range[0] if date_range and date_range[0] else None,
                    'end': date_range[1] if date_range and date_range[1] else None,
                },
            }
        except Exception:
            logger.exception('获取统计信息失败')
            return {}

    def get_fina_indicator(self, code: str, limit: int = 8) -> list[dict[str, Any]]:
        return self._get_financial_rows('fina_indicator', code, limit)

    def get_income(self, code: str, limit: int = 8) -> list[dict[str, Any]]:
        return self._get_financial_rows('income', code, limit)

    def get_balancesheet(self, code: str, limit: int = 8) -> list[dict[str, Any]]:
        return self._get_financial_rows('balancesheet', code, limit)

    def get_cashflow(self, code: str, limit: int = 8) -> list[dict[str, Any]]:
        return self._get_financial_rows('cashflow', code, limit)

    def _get_financial_rows(self, table: str, code: str, limit: int) -> list[dict[str, Any]]:
        try:
            with self.get_connection() as conn:
                rows = conn.execute(
                    f'SELECT * FROM {table} WHERE code = ? ORDER BY end_date DESC LIMIT ?',
                    (code, limit),
                ).fetchall()
                return self._rows_to_dicts(rows)
        except Exception:
            logger.exception('获取财务数据失败: %s %s', table, code)
            return []

    def get_stock_overview(self, code: str) -> Optional[dict[str, Any]]:
        """获取股票概览"""
        try:
            with self.get_connection() as conn:
                row = conn.execute('SELECT * FROM stocks WHERE code = ?', (code,)).fetchone()
                return dict(row) if row else None
        except Exception:
            logger.exception('获取股票概览失败: %s', code)
            return None

    def get_stock_financials(self, code: str, limit: int = 8) -> dict[str, list[dict[str, Any]]]:
        """获取股票财务数据聚合结果"""
        return {
            'income': self.get_income(code, limit=limit),
            'fina_indicator': self.get_fina_indicator(code, limit=limit),
            'balancesheet': self.get_balancesheet(code, limit=limit),
            'cashflow': self.get_cashflow(code, limit=limit),
        }

    def analyze(self) -> None:
        try:
            with self.get_connection() as conn:
                conn.execute('ANALYZE')
        except Exception:
            logger.exception('ANALYZE 失败')

    def clear_all(self) -> None:
        """清空所有数据并回收磁盘空间"""
        try:
            with self.get_connection() as conn:
                conn.execute('DELETE FROM daily_prices')
                conn.execute('DELETE FROM fina_indicator')
                conn.execute('DELETE FROM income')
                conn.execute('DELETE FROM balancesheet')
                conn.execute('DELETE FROM cashflow')
                conn.execute('DELETE FROM sync_status')
                conn.execute('DELETE FROM stocks')
            conn = self.get_connection()
            conn.execute('VACUUM')
            conn.close()
            logger.info('数据库已清空并 VACUUM')
        except Exception:
            logger.exception('清空数据库失败')
            raise

