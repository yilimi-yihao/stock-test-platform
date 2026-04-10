from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import pandas as pd

from sql_tool.base_source import BaseTushareSource

logger = logging.getLogger(__name__)


class EtfSource(BaseTushareSource):
    """ETF 数据源"""

    @staticmethod
    def normalize_ts_code(code: str, ts_code: Optional[str] = None) -> str:
        explicit = (ts_code or '').strip().upper()
        if explicit:
            return explicit

        normalized = (code or '').strip().upper()
        if not normalized:
            return ''
        if '.' in normalized:
            return normalized
        if normalized.startswith(('159', '160', '161', '162', '163', '164', '165', '166', '167', '168', '169')):
            return f'{normalized}.SZ'
        return f'{normalized}.SH'

    def get_etf_list(self) -> List[Dict[str, Any]]:
        try:
            df = self.pro.fund_basic(
                market='E',
                status='L',
                fields='ts_code,name,management,custodian,fund_type,found_date,due_date,list_date,issue_date,delist_date,issue_amount,m_fee,c_fee,duration_year,p_value,min_amount,exp_return,benchmark,invest_type,type,status,market',
            )
            items = []
            for _, row in df.iterrows():
                ts_code = row.get('ts_code', '')
                code = ts_code.split('.')[0] if ts_code else ''
                items.append(
                    {
                        'code': code,
                        'ts_code': ts_code,
                        'name': row.get('name', code),
                        'management': row.get('management', ''),
                        'custodian': row.get('custodian', ''),
                        'fund_type': row.get('fund_type', ''),
                        'found_date': row.get('found_date', ''),
                        'due_date': row.get('due_date', ''),
                        'list_date': row.get('list_date', ''),
                        'issue_date': row.get('issue_date', ''),
                        'delist_date': row.get('delist_date', ''),
                        'issue_amount': row.get('issue_amount', 0),
                        'm_fee': row.get('m_fee', 0),
                        'c_fee': row.get('c_fee', 0),
                        'duration_year': row.get('duration_year', 0),
                        'p_value': row.get('p_value', 0),
                        'min_amount': row.get('min_amount', 0),
                        'exp_return': row.get('exp_return', 0),
                        'benchmark': row.get('benchmark', ''),
                        'invest_type': row.get('invest_type', ''),
                        'type': row.get('type', ''),
                        'status': row.get('status', ''),
                        'market': row.get('market', ''),
                    }
                )
            return items
        except Exception:
            logger.exception('获取 ETF 列表失败')
            return []

    def get_etf_daily_prices(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        ts_code: Optional[str] = None,
    ) -> pd.DataFrame:
        normalized_ts_code = self.normalize_ts_code(code, ts_code=ts_code)
        try:
            df = self.pro.fund_daily(ts_code=normalized_ts_code, start_date=start_date, end_date=end_date)
        except Exception:
            logger.exception('获取 ETF %s 日线失败', normalized_ts_code or code)
            return self._empty_df()

        if df is None or df.empty:
            return self._empty_df()

        df = df.rename(columns={'trade_date': 'date', 'vol': 'volume'})
        for column in ['open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'volume', 'amount']:
            if column not in df.columns:
                df[column] = None
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        return df[['date', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'volume', 'amount']].sort_values('date')

    def detect_capabilities(self, sample_code: str = '510300') -> list[dict[str, Any]]:
        ts_code = self.normalize_ts_code(sample_code)
        checks = [
            ('fund_basic', 'ETF列表', lambda: self.pro.fund_basic(market='E', status='L', fields='ts_code,name,management,custodian,fund_type,found_date', limit=10)),
            ('fund_daily', 'ETF日线', lambda: self.pro.fund_daily(ts_code=ts_code, start_date='20240101', end_date='20241231')),
        ]
        results: list[dict[str, Any]] = []
        for api_name, display_name, fetcher in checks:
            try:
                df = fetcher()
                rows = 0 if df is None or df.empty else len(df)
                results.append(
                    {
                        'api_name': api_name,
                        'display_name': display_name,
                        'sample_code': sample_code,
                        'ts_code': ts_code,
                        'available': True,
                        'empty': rows == 0,
                        'rows': rows,
                        'error': '',
                    }
                )
            except Exception as exc:
                results.append(
                    {
                        'api_name': api_name,
                        'display_name': display_name,
                        'sample_code': sample_code,
                        'ts_code': ts_code,
                        'available': False,
                        'empty': True,
                        'rows': 0,
                        'error': str(exc),
                    }
                )
        return results
