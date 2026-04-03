"""数据库操作模块"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any, Iterable, Optional

logger = logging.getLogger(__name__)


class StockDatabase:
    """A 股数据库操作类"""

    def __init__(self, db_path: str = 'data/a_share.db'):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def get_connection(self) -> sqlite3.Connection:
        """获取数据库连接"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA foreign_keys = ON')
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
                    ann_date TEXT NOT NULL,
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
                    ann_date TEXT NOT NULL,
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
                    ann_date TEXT NOT NULL,
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
                    ann_date TEXT NOT NULL,
                    end_date TEXT NOT NULL,
                    operate_cash_flow REAL,
                    invest_cash_flow REAL,
                    finance_cash_flow REAL,
                    UNIQUE(code, end_date),
                    FOREIGN KEY(code) REFERENCES stocks(code)
                )
                '''
            )

            cursor.execute('CREATE INDEX IF NOT EXISTS idx_daily_code_date ON daily_prices(code, date DESC)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_daily_date ON daily_prices(date DESC)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_stocks_industry ON stocks(industry)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_fina_code_date ON fina_indicator(code, end_date DESC)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_income_code_date ON income(code, end_date DESC)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_bs_code_date ON balancesheet(code, end_date DESC)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_cf_code_date ON cashflow(code, end_date DESC)')

        logger.info('数据库初始化完成')

    def _rows_to_dicts(self, rows: Iterable[sqlite3.Row]) -> list[dict[str, Any]]:
        return [dict(row) for row in rows]

    def _insert_dataframe(
        self,
        conn: sqlite3.Connection,
        sql: str,
        rows: Iterable[tuple[Any, ...]],
    ) -> None:
        conn.executemany(sql, list(rows))

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

    def get_stock_list(self, limit: Optional[int] = None, search: str = '') -> list[dict[str, Any]]:
        """获取股票列表"""
        try:
            with self.get_connection() as conn:
                query = 'SELECT code, name, industry, area, list_date, updated_at FROM stocks'
                params: list[Any] = []
                if search:
                    query += ' WHERE code LIKE ? OR name LIKE ? OR industry LIKE ?'
                    keyword = f'%{search}%'
                    params.extend([keyword, keyword, keyword])
                query += ' ORDER BY code'
                if limit:
                    query += ' LIMIT ?'
                    params.append(limit)
                rows = conn.execute(query, params).fetchall()
                return self._rows_to_dicts(rows)
        except Exception:
            logger.exception('获取股票列表失败')
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
        """获取各表行数"""
        table_names = ['stocks', 'daily_prices', 'fina_indicator', 'income', 'balancesheet', 'cashflow']
        try:
            with self.get_connection() as conn:
                return {
                    table: conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
                    for table in table_names
                }
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

    def clear_all(self) -> None:
        """清空所有数据"""
        try:
            with self.get_connection() as conn:
                conn.execute('DELETE FROM daily_prices')
                conn.execute('DELETE FROM fina_indicator')
                conn.execute('DELETE FROM income')
                conn.execute('DELETE FROM balancesheet')
                conn.execute('DELETE FROM cashflow')
                conn.execute('DELETE FROM stocks')
            logger.info('数据库已清空')
        except Exception:
            logger.exception('清空数据库失败')
            raise
