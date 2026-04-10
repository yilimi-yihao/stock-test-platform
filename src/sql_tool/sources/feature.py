from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from sql_tool.base_source import BaseTushareSource

logger = logging.getLogger(__name__)


class FeatureSource(BaseTushareSource):
    """特色数据源"""

    def get_concepts(self) -> list[dict[str, Any]]:
        try:
            df = self.pro.concept(src='ts')
            if df is None or df.empty:
                return []
            items = []
            for _, row in df.iterrows():
                items.append(
                    {
                        'concept_id': row.get('code') or row.get('id'),
                        'concept_name': row.get('name') or row.get('concept_name'),
                        'source': 'tushare',
                        'category': 'concept',
                    }
                )
            return [item for item in items if item['concept_id']]
        except Exception:
            logger.exception('获取概念列表失败')
            return []

    def get_concept_members(self, concept_id: str) -> list[dict[str, Any]]:
        try:
            df = self.pro.concept_detail(id=concept_id, fields='id,concept_name,ts_code,name,in_date,out_date')
            if df is None or df.empty:
                return []
            items = []
            for _, row in df.iterrows():
                ts_code = row.get('ts_code', '')
                code = ts_code.split('.')[0] if ts_code else ''
                items.append(
                    {
                        'code': code,
                        'concept_id': row.get('id') or concept_id,
                        'in_date': row.get('in_date'),
                        'out_date': row.get('out_date'),
                        'is_active': 0 if row.get('out_date') else 1,
                    }
                )
            return [item for item in items if item['code']]
        except Exception:
            logger.exception('获取概念成分失败: %s', concept_id)
            return []

    def get_moneyflow(self, code: str, start_date: str | None = None, end_date: str | None = None) -> list[dict[str, Any]]:
        ts_code = code if '.' in code else (f'{code}.SZ' if code.startswith(('0', '3')) else f'{code}.SH')
        try:
            df = self.pro.moneyflow(ts_code=ts_code, start_date=start_date, end_date=end_date)
            if df is None or df.empty:
                return []
            rows = []
            for _, row in df.iterrows():
                rows.append(
                    {
                        'code': code,
                        'trade_date': row.get('trade_date'),
                        'buy_sm_vol': row.get('buy_sm_vol'),
                        'buy_md_vol': row.get('buy_md_vol'),
                        'buy_lg_vol': row.get('buy_lg_vol'),
                        'buy_elg_vol': row.get('buy_elg_vol'),
                        'sell_sm_vol': row.get('sell_sm_vol'),
                        'sell_md_vol': row.get('sell_md_vol'),
                        'sell_lg_vol': row.get('sell_lg_vol'),
                        'sell_elg_vol': row.get('sell_elg_vol'),
                        'net_mf_vol': row.get('net_mf_vol'),
                        'net_mf_amount': row.get('net_mf_amount'),
                        'source': 'tushare',
                    }
                )
            return rows
        except Exception:
            logger.exception('获取资金流向失败: %s', code)
            return []

    def get_top_list(self, code: str | None = None, trade_date: str | None = None) -> list[dict[str, Any]]:
        kwargs: dict[str, Any] = {}
        if code:
            kwargs['ts_code'] = code if '.' in code else (f'{code}.SZ' if code.startswith(('0', '3')) else f'{code}.SH')
        if trade_date:
            kwargs['trade_date'] = trade_date
        try:
            df = self.pro.top_list(**kwargs)
            if df is None or df.empty:
                return []
            rows = []
            for _, row in df.iterrows():
                ts_code = row.get('ts_code', '')
                rows.append(
                    {
                        'code': ts_code.split('.')[0] if ts_code else code,
                        'trade_date': row.get('trade_date'),
                        'reason': row.get('exalter') or row.get('reason') or '龙虎榜',
                        'close': row.get('close'),
                        'pct_change': row.get('pct_change') or row.get('pct_chg'),
                        'turnover_rate': row.get('turnover_rate'),
                        'amount': row.get('amount'),
                        'net_amount': row.get('net_amount') or row.get('net_buy'),
                        'source': 'tushare',
                    }
                )
            return rows
        except Exception:
            logger.exception('获取龙虎榜失败')
            return []

    def get_top_list_traders(self, code: str | None = None, trade_date: str | None = None) -> list[dict[str, Any]]:
        return []

    def detect_capabilities(self, sample_code: str = '000001', include_pending: bool = True) -> dict[str, Any]:
        ts_code = sample_code if '.' in sample_code else (f'{sample_code}.SZ' if sample_code.startswith(('0', '3')) else f'{sample_code}.SH')
        checks = [
            ('concept', '概念板块', lambda: self.pro.concept(src='ts')),
            ('concept_detail', '概念成分', lambda: self.pro.concept_detail(id='TS0', fields='id,concept_name,ts_code,name,in_date,out_date')),
            ('moneyflow', '资金流向', lambda: self.pro.moneyflow(ts_code=ts_code, start_date='20240401', end_date='20240410')),
            ('top_list', '龙虎榜', lambda: self.pro.top_list(trade_date='20240430')),
        ]
        results: list[dict[str, Any]] = []
        pending: list[dict[str, Any]] = []
        pending_checks = [
            ('auction', '集合竞价', lambda: self.pro.auction(ts_code=ts_code, start_date='20240401', end_date='20240410')),
        ] if include_pending else []

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

        for api_name, display_name, fetcher in pending_checks:
            try:
                df = fetcher()
                rows = 0 if df is None or df.empty else len(df)
                pending.append(
                    {
                        'api_name': api_name,
                        'display_name': display_name,
                        'sample_code': sample_code,
                        'ts_code': ts_code,
                        'available': False,
                        'empty': rows == 0,
                        'rows': rows,
                        'error': '',
                        'pending': True,
                    }
                )
            except Exception as exc:
                pending.append(
                    {
                        'api_name': api_name,
                        'display_name': display_name,
                        'sample_code': sample_code,
                        'ts_code': ts_code,
                        'available': False,
                        'empty': True,
                        'rows': 0,
                        'error': str(exc),
                        'pending': True,
                    }
                )
        return {
            'sample_code': sample_code,
            'available_count': sum(1 for item in results if item['available']),
            'total_count': len(results),
            'results': results,
            'pending': pending,
            'available_datasets': [item['api_name'] for item in results if item['available']],
        }
