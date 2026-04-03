"""Tushare 数据源模块"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class TushareSource:
    """Tushare 数据源"""

    def __init__(self, token: str, api_url: str = 'http://api.tushare.pro'):
        if not token or not token.strip():
            raise ValueError('Tushare token 不能为空')

        try:
            import tushare as ts

            self.ts = ts
            self.pro = ts.pro_api(token)
            self.api_url = api_url
            logger.info('Tushare 初始化成功')
        except Exception:
            logger.exception('Tushare 初始化失败')
            raise

    def _to_ts_code(self, code: str) -> str:
        """将 6 位代码转换为 Tushare ts_code"""
        if code.endswith(('.SZ', '.SH', '.BJ')):
            return code
        if code.startswith(('0', '3')):
            return f'{code}.SZ'
        if code.startswith(('8', '4')):
            return f'{code}.BJ'
        return f'{code}.SH'

    def _empty_df(self) -> pd.DataFrame:
        return pd.DataFrame()

    def get_stock_list(self) -> List[Dict[str, str]]:
        """获取 A 股股票列表"""
        try:
            df = self.pro.stock_basic(
                exchange='',
                list_status='L',
                fields='ts_code,symbol,name,area,industry,list_date',
            )
            stocks = []
            for _, row in df.iterrows():
                stocks.append(
                    {
                        'code': row['symbol'],
                        'ts_code': row['ts_code'],
                        'name': row['name'],
                        'industry': row.get('industry', ''),
                        'area': row.get('area', ''),
                        'list_date': row.get('list_date', ''),
                    }
                )
            logger.info('获取 %s 只股票', len(stocks))
            return stocks
        except Exception:
            logger.exception('获取股票列表失败')
            return []

    def get_daily_prices(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """获取日线数据"""
        try:
            ts_code = self._to_ts_code(code)
            df = self.pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
            if df.empty:
                return self._empty_df()

            df = df.rename(columns={'trade_date': 'date', 'vol': 'volume'})
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            df = df[['date', 'open', 'high', 'low', 'close', 'volume', 'amount']].sort_values('date')
            logger.debug('获取 %s 的 %s 条日线数据', code, len(df))
            return df
        except Exception:
            logger.exception('获取 %s 日线数据失败', code)
            return self._empty_df()

    def get_stock_info(self, code: str) -> Dict[str, Any]:
        """获取股票详细信息"""
        info = {
            'code': code,
            'name': code,
            'area': '',
            'industry': '',
            'list_date': '',
            'pe_ratio': 0,
            'pb_ratio': 0,
            'market_cap': 0,
            'circ_mv': 0,
            'turnover_rate': 0,
            'volume_ratio': 0,
            'adj_factor': 0,
        }

        try:
            ts_code = self._to_ts_code(code)
            basic_df = self.pro.stock_basic(
                ts_code=ts_code,
                fields='ts_code,symbol,name,area,industry,list_date',
            )
            daily_basic_df = self.pro.daily_basic(
                ts_code=ts_code,
                fields='ts_code,trade_date,pe,pb,total_mv,circ_mv,turnover_rate,volume_ratio',
            )
            adj_df = self.pro.adj_factor(ts_code=ts_code, fields='ts_code,trade_date,adj_factor')

            if not basic_df.empty:
                basic = basic_df.iloc[0]
                info['name'] = basic.get('name', code)
                info['area'] = basic.get('area', '')
                info['industry'] = basic.get('industry', '')
                info['list_date'] = basic.get('list_date', '')

            if not daily_basic_df.empty:
                daily_basic = daily_basic_df.iloc[0]
                info['pe_ratio'] = float(daily_basic.get('pe', 0) or 0)
                info['pb_ratio'] = float(daily_basic.get('pb', 0) or 0)
                info['market_cap'] = float(daily_basic.get('total_mv', 0) or 0)
                info['circ_mv'] = float(daily_basic.get('circ_mv', 0) or 0)
                info['turnover_rate'] = float(daily_basic.get('turnover_rate', 0) or 0)
                info['volume_ratio'] = float(daily_basic.get('volume_ratio', 0) or 0)

            if not adj_df.empty:
                info['adj_factor'] = float(adj_df.iloc[0].get('adj_factor', 0) or 0)

            return info
        except Exception:
            logger.exception('获取 %s 信息失败', code)
            return info

    def get_fina_indicator(self, code: str) -> pd.DataFrame:
        """获取财务指标数据"""
        try:
            ts_code = self._to_ts_code(code)
            df = self.pro.fina_indicator(
                ts_code=ts_code,
                fields='ts_code,ann_date,end_date,eps,roe,roa,grossprofit_margin,netprofit_margin,debt_to_assets,current_ratio,quick_ratio',
            )
            if df.empty:
                return self._empty_df()
            df = df.rename(columns={'grossprofit_margin': 'gross_margin', 'netprofit_margin': 'net_margin'})
            return df.sort_values('end_date')
        except Exception:
            logger.exception('获取 %s 财务指标失败', code)
            return self._empty_df()

    def get_income(self, code: str) -> pd.DataFrame:
        """获取利润表数据"""
        try:
            ts_code = self._to_ts_code(code)
            df = self.pro.income(ts_code=ts_code, fields='ts_code,ann_date,end_date,revenue,operate_profit,n_income')
            if df.empty:
                return self._empty_df()
            df = df.rename(columns={'n_income': 'net_profit'})
            return df.sort_values('end_date')
        except Exception:
            logger.exception('获取 %s 利润表失败', code)
            return self._empty_df()

    def get_balancesheet(self, code: str) -> pd.DataFrame:
        """获取资产负债表数据"""
        try:
            ts_code = self._to_ts_code(code)
            df = self.pro.balancesheet(
                ts_code=ts_code,
                fields='ts_code,ann_date,end_date,total_assets,total_liab,total_hldr_eqy_exc_min_int,current_assets,current_liab,cash_and_cash_equiv,accounts_payable,advance_receipts',
            )
            if df.empty:
                return self._empty_df()
            df = df.rename(columns={'total_hldr_eqy_exc_min_int': 'total_equity', 'cash_and_cash_equiv': 'cash'})
            return df.sort_values('end_date')
        except Exception:
            logger.exception('获取 %s 资产负债表失败', code)
            return self._empty_df()

    def get_cashflow(self, code: str) -> pd.DataFrame:
        """获取现金流量表数据"""
        try:
            ts_code = self._to_ts_code(code)
            df = self.pro.cashflow(
                ts_code=ts_code,
                fields='ts_code,ann_date,end_date,operate_cash_flow,invest_cash_flow,finance_cash_flow',
            )
            if df.empty:
                return self._empty_df()
            return df.sort_values('end_date')
        except Exception:
            logger.exception('获取 %s 现金流量表失败', code)
            return self._empty_df()

    def detect_capabilities(self, sample_code: str = '000001') -> list[dict[str, Any]]:
        """检测当前账号可用接口"""
        ts_code = self._to_ts_code(sample_code)
        checks = [
            ('stock_basic', '股票基础信息', lambda: self.pro.stock_basic(ts_code=ts_code, fields='ts_code,symbol,name')),
            ('daily', '日线行情', lambda: self.pro.daily(ts_code=ts_code)),
            ('daily_basic', '每日指标', lambda: self.pro.daily_basic(ts_code=ts_code, fields='ts_code,trade_date,pe,pb')),
            ('adj_factor', '复权因子', lambda: self.pro.adj_factor(ts_code=ts_code, fields='ts_code,trade_date,adj_factor')),
            ('fina_indicator', '财务指标', lambda: self.pro.fina_indicator(ts_code=ts_code, fields='ts_code,ann_date,end_date,roe')),
            ('income', '利润表', lambda: self.pro.income(ts_code=ts_code, fields='ts_code,ann_date,end_date,revenue')),
            ('balancesheet', '资产负债表', lambda: self.pro.balancesheet(ts_code=ts_code, fields='ts_code,ann_date,end_date,total_assets')),
            ('cashflow', '现金流量表', lambda: self.pro.cashflow(ts_code=ts_code, fields='ts_code,ann_date,end_date,operate_cash_flow')),
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
