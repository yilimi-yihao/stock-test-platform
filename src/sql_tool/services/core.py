from __future__ import annotations

import time
from typing import Any, Callable, Dict, Optional

from sql_tool.config import ConfigManager
from sql_tool.db import (
    StockDatabase,
    EtfDatabase,
    FeatureDatabase,
    EventDatabase,
    IndexConstituentDatabase,
    IndexForecastDatabase,
)
from sql_tool.sources import TushareSource, EtfSource, FeatureSource

LogFn = Optional[Callable[[str], None]]
SYNC_DATASETS = ['daily', 'daily_basic', 'adj_factor', 'fina_indicator', 'income', 'balancesheet', 'cashflow']


class SqlToolService:
    """sql-tool 高层服务"""

    def __init__(self, config_path: str = 'config/settings.json'):
        self.config = ConfigManager(config_path=config_path)
        self.db = StockDatabase(self.config.get_stock_db_path())
        self.etf_db = EtfDatabase(self.config.get_etf_db_path())
        self.feature_db = FeatureDatabase(self.config.get_feature_db_path())
        self.event_db = EventDatabase(self.config.get_event_db_path())
        self.index_db = IndexConstituentDatabase(self.config.get_index_constituent_db_path())
        self.index_forecast_db = IndexForecastDatabase(self.config.get_index_forecast_db_path())
        self.source: Optional[TushareSource] = None
        self.etf_source: Optional[EtfSource] = None
        self.feature_source: Optional[FeatureSource] = None
        self._request_interval = self.config.get_tushare_request_interval()
        self._cancelled = False

    def cancel(self) -> None:
        """请求取消当前正在执行的导入/更新任务"""
        self._cancelled = True

    def _reset_cancel(self) -> None:
        self._cancelled = False

    def init_source(self) -> bool:
        token = self.config.get_tushare_token()
        api_url = self.config.get_tushare_api_url()
        if not token:
            return False
        self.source = TushareSource(token, api_url=api_url)
        return True

    def _ensure_source(self) -> TushareSource:
        if self.source is None and not self.init_source():
            raise ValueError('Tushare token 未配置，请先配置 token')
        return self.source

    def init_etf_source(self) -> bool:
        token = self.config.get_tushare_token()
        api_url = self.config.get_tushare_api_url()
        if not token:
            return False
        self.etf_source = EtfSource(token, api_url=api_url)
        return True

    def _ensure_etf_source(self) -> EtfSource:
        if self.etf_source is None and not self.init_etf_source():
            raise ValueError('Tushare token 未配置，请先配置 token')
        return self.etf_source

    def init_feature_source(self) -> bool:
        token = self.config.get_tushare_token()
        api_url = self.config.get_tushare_api_url()
        if not token:
            return False
        self.feature_source = FeatureSource(token, api_url=api_url)
        return True

    def _ensure_feature_source(self) -> FeatureSource:
        if self.feature_source is None and not self.init_feature_source():
            raise ValueError('Tushare token 未配置，请先配置 token')
        return self.feature_source

    def _log(self, log: LogFn, message: str) -> None:
        if log:
            log(message)

    def _pick_existing_stock_code(self) -> Optional[str]:
        items = self.db.get_stock_list(limit=1)
        if items:
            return items[0].get('code')
        return None

    def _pick_existing_etf_code(self) -> Optional[str]:
        items = self.etf_db.get_etf_list(limit=1)
        if items:
            return items[0].get('code')
        return None

    def _resolve_stock_sample_code(self, sample_code: Optional[str] = None) -> str:
        code = (sample_code or '').strip().upper().split('.')[0]
        if code:
            return code
        existing = self._pick_existing_stock_code()
        if existing:
            return existing
        legacy = self.config.get_sample_stock().strip().upper().split('.')[0]
        return legacy or '002594'

    def _resolve_etf_sample_code(self, sample_code: Optional[str] = None) -> str:
        code = (sample_code or '').strip().upper().split('.')[0]
        if code:
            return code
        existing = self._pick_existing_etf_code()
        if existing:
            return existing
        legacy = self.config.get_sample_etf().strip().upper().split('.')[0]
        return legacy or '510300'

    def _get_capability_map(self, sample_code: Optional[str] = None) -> dict[str, bool]:
        source = self._ensure_source()
        resolved = self._resolve_stock_sample_code(sample_code)
        results = source.detect_capabilities(resolved)
        return source.capability_map(results)

    def _available_datasets(self, capabilities: dict[str, bool]) -> list[str]:
        return [dataset for dataset in SYNC_DATASETS if capabilities.get(dataset, False)]

    def _make_summary(self, mode: str, total: int, capabilities: dict[str, bool]) -> dict[str, Any]:
        return {
            'mode': mode,
            'total': total,
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'cancelled': False,
            'daily_rows': 0,
            'financial_rows': 0,
            'backfilled': 0,
            'available_datasets': self._available_datasets(capabilities),
        }

    def get_stats(self) -> Dict[str, Any]:
        return self.db.get_stats()

    def get_all_stats(self) -> Dict[str, Any]:
        return {
            'stocks': self.db.get_stats(),
            'etfs': self.etf_db.get_stats(),
            'features': self.feature_db.get_stats(),
            'events': self.event_db.get_stats(),
            'indexes': self.index_db.get_stats(),
            'index_forecasts': self.index_forecast_db.get_stats(),
        }

    def get_stocks(self, limit: Optional[int] = None, search: str = '') -> list[dict[str, Any]]:
        return self.db.get_stock_list(limit=limit, search=search)

    def get_all_stock_codes(self) -> list[str]:
        return [item['code'] for item in self.db.get_stock_list(limit=None)]

    def get_stock_page(
        self,
        page: int = 1,
        page_size: int = 200,
        search: str = '',
        industry: str = '',
        area: str = '',
        order_by: str = 'code',
        order: str = 'asc',
    ) -> dict[str, Any]:
        return self.db.get_stock_list_page(
            page=page,
            page_size=page_size,
            search=search,
            industry=industry,
            area=area,
            order_by=order_by,
            order=order,
        )

    def get_stock_overviews(self, codes: list[str]) -> list[dict[str, Any]]:
        return self.db.get_stock_overviews(codes)

    def get_daily_batch(
        self,
        codes: list[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit_per_code: Optional[int] = None,
    ) -> dict[str, Any]:
        items = self.db.get_daily_prices_batch(
            codes=codes,
            start_date=start_date,
            end_date=end_date,
            limit_per_code=limit_per_code,
        )
        return {
            'items': items,
            'meta': {
                'codes': len([code for code in codes if code.strip()]),
                'rows': len(items),
                'start_date': start_date,
                'end_date': end_date,
                'limit_per_code': limit_per_code,
            },
        }

    def get_stock_daily(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        return self.db.get_daily_prices(code, start_date=start_date, end_date=end_date, limit=limit)

    def get_stock_financials(self, code: str, limit: int = 8) -> dict[str, Any]:
        return {
            'stock': self.db.get_stock_overview(code),
            'financials': self.db.get_stock_financials(code, limit=limit),
        }

    def clear_data(self) -> None:
        self.db.clear_all()

    def get_etfs(self, limit: Optional[int] = None, search: str = '') -> list[dict[str, Any]]:
        return self.etf_db.get_etf_list(limit=limit, search=search)

    def get_all_etf_codes(self) -> list[str]:
        return [item['code'] for item in self.etf_db.get_etf_list(limit=None)]

    def get_etf_page(
        self,
        page: int = 1,
        page_size: int = 200,
        search: str = '',
        fund_type: str = '',
        market: str = '',
        order_by: str = 'code',
        order: str = 'asc',
    ) -> dict[str, Any]:
        return self.etf_db.get_etf_list_page(
            page=page,
            page_size=page_size,
            search=search,
            fund_type=fund_type,
            market=market,
            order_by=order_by,
            order=order,
        )

    def get_etf_daily(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        return self.etf_db.get_daily_prices(code, start_date=start_date, end_date=end_date, limit=limit)

    def get_etf_stats(self) -> dict[str, Any]:
        return self.etf_db.get_stats()

    def get_feature_stats(self) -> dict[str, Any]:
        return self.feature_db.get_stats()

    def get_event_stats(self) -> dict[str, Any]:
        return self.event_db.get_stats()

    def get_index_stats(self) -> dict[str, Any]:
        return self.index_db.get_stats()

    def get_index_forecast_stats(self) -> dict[str, Any]:
        return self.index_forecast_db.get_stats()

    def get_database_table_options(self) -> dict[str, list[str]]:
        return {
            'stocks': self.db.list_tables(),
            'etfs': self.etf_db.list_tables(),
            'features': self.feature_db.list_tables(),
            'events': self.event_db.list_tables(),
            'indexes': self.index_db.list_tables(),
            'index_forecasts': self.index_forecast_db.list_tables(),
        }

    def get_database_table_rows(self, domain: str, table: str, limit: int = 200) -> dict[str, Any]:
        mapping = {
            'stocks': self.db,
            'etfs': self.etf_db,
            'features': self.feature_db,
            'events': self.event_db,
            'indexes': self.index_db,
            'index_forecasts': self.index_forecast_db,
        }
        if domain not in mapping:
            raise ValueError(f'未知数据域: {domain}')
        db = mapping[domain]
        rows = db.get_table_rows(table, limit=limit)
        return {
            'domain': domain,
            'table': table,
            'columns': db.get_table_columns(table),
            'rows': rows,
            'row_count': len(rows),
        }

    def analyze_all(self) -> dict[str, Any]:
        results = {}
        for name, db in [
            ('stocks', self.db),
            ('etfs', self.etf_db),
            ('features', self.feature_db),
            ('events', self.event_db),
            ('indexes', self.index_db),
            ('index_forecasts', self.index_forecast_db),
        ]:
            try:
                db.analyze()
                results[name] = 'ok'
            except Exception as exc:
                results[name] = f'error: {exc}'
        return results

    def upsert_holiday(self, holiday_date: str, name: str, market_scope: str = 'CN',
                       is_trading_closed: bool = True, notes: str = '') -> int:
        return self.event_db.upsert_holiday(holiday_date, name, market_scope, is_trading_closed, notes)

    def replace_holiday_mappings(self, holiday_id: int, mappings: list[dict]) -> int:
        return self.event_db.replace_holiday_mappings(holiday_id, mappings)

    def get_holiday_mappings(self, holiday_id: int) -> list[dict]:
        return self.event_db.get_holiday_mappings(holiday_id)

    def upsert_event(self, event_date: str, name: str, category: str,
                     location: str = '', notes: str = '') -> int:
        return self.event_db.upsert_event(event_date, name, category, location, notes)

    def replace_event_mappings(self, event_id: int, mappings: list[dict]) -> int:
        return self.event_db.replace_event_mappings(event_id, mappings)

    def get_event_mappings(self, event_id: int) -> list[dict]:
        return self.event_db.get_event_mappings(event_id)

    def delete_holiday(self, holiday_id: int) -> bool:
        return self.event_db.delete_holiday(holiday_id)

    def clear_holidays(self) -> int:
        return self.event_db.clear_holidays()

    def delete_event(self, event_id: int) -> bool:
        return self.event_db.delete_event(event_id)

    def clear_events(self) -> int:
        return self.event_db.clear_events()

    def seed_event_data(self) -> dict[str, int]:
        return self.event_db.seed_initial_data()

    def get_holidays(self) -> list[dict[str, Any]]:
        return self.event_db.get_holidays()

    def get_events(self) -> list[dict[str, Any]]:
        return self.event_db.get_events()

    def derive_index_entities_from_etfs(self) -> dict[str, Any]:
        etfs = self.etf_db.get_etf_list(limit=5000)
        created = 0
        items = []
        for etf in etfs:
            name = (etf.get('name') or '').strip()
            benchmark = (etf.get('benchmark') or '').strip()
            fund_type = (etf.get('fund_type') or '').strip().upper()
            if 'ETF' not in fund_type and '指数' not in name and '300' not in name and '500' not in name and '1000' not in name:
                continue
            index_name = benchmark or name.replace('ETF', '').strip() or name
            index_id = self.index_db.upsert_index_entity(index_name=index_name, benchmark=benchmark)
            created += 1
            items.append({'index_id': index_id, 'index_name': index_name})
        return {'created': created, 'items': items}

    def import_index_constituent_snapshot(self, index_id: int, trade_date: str, items: list[dict[str, Any]], note: str = '') -> dict[str, Any]:
        snapshot_id = self.index_db.create_snapshot(index_id=index_id, trade_date=trade_date, note=note)
        rows = self.index_db.replace_snapshot_items(snapshot_id, items)
        return {'snapshot_id': snapshot_id, 'rows': rows}

    def analyze_index_constituent_changes(self, index_id: int) -> dict[str, Any]:
        snapshots = self.index_db.get_snapshots(index_id)
        if len(snapshots) < 2:
            return {'index_id': index_id, 'changes': 0, 'reason': 'need_at_least_two_snapshots'}
        latest = snapshots[0]
        previous = snapshots[1]
        latest_items = {item['code']: item for item in self.index_db.get_snapshot_items(latest['snapshot_id'])}
        previous_items = {item['code']: item for item in self.index_db.get_snapshot_items(previous['snapshot_id'])}
        added = [
            {'change_type': 'added', 'code': code, 'name': latest_items[code].get('name')}
            for code in latest_items.keys() - previous_items.keys()
        ]
        removed = [
            {'change_type': 'removed', 'code': code, 'name': previous_items[code].get('name')}
            for code in previous_items.keys() - latest_items.keys()
        ]
        changes = sorted(added + removed, key=lambda x: (x['change_type'], x['code']))
        rows = self.index_db.replace_changes(index_id, latest['trade_date'], changes, previous['snapshot_id'], latest['snapshot_id'])
        return {'index_id': index_id, 'changes': rows, 'trade_date': latest['trade_date']}

    def get_index_entities(self, category: Optional[str] = None) -> list[dict[str, Any]]:
        return self.index_db.get_index_entities(category=category)

    def get_index_changes(self, index_id: int) -> list[dict[str, Any]]:
        return self.index_db.get_changes(index_id)

    def add_index_forecast(
        self,
        index_name: str,
        forecast_month: str,
        forecast_direction: str,
        stock_code: str,
        stock_name: str,
        broker_name: str,
        source_note: str = '',
    ) -> None:
        self.index_forecast_db.upsert_forecast(index_name, forecast_month, forecast_direction, stock_code, stock_name, broker_name, source_note)

    def get_index_forecasts(self) -> list[dict[str, Any]]:
        return self.index_forecast_db.get_forecasts()

    def export_index_forecasts_csv(self, output_path: str) -> str:
        return self.index_forecast_db.export_forecasts_csv(output_path)

    def export_index_forecasts_excel(self, output_path: str) -> str:
        return self.index_forecast_db.export_forecasts_excel(output_path)

    def export_index_forecasts_pdf(self, output_path: str) -> str:
        return self.index_forecast_db.export_forecasts_pdf(output_path)

    def get_concepts(self) -> list[dict[str, Any]]:
        return self.feature_db.get_concepts()

    def get_concept_members(self, concept_id: str) -> list[dict[str, Any]]:
        return self.feature_db.get_concept_members(concept_id)

    def get_stock_feature_profile(self, code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> dict[str, Any]:
        return {
            'stock': self.db.get_stock_overview(code),
            'concepts': self._get_concepts_for_code(code),
            'moneyflow': self.feature_db.get_moneyflow(code, start_date=start_date, end_date=end_date),
            'top_list': self.feature_db.get_top_list(code=code),
        }

    def detect_capabilities(self, sample_code: Optional[str] = None) -> dict[str, Any]:
        source = self._ensure_source()
        sample = self._resolve_stock_sample_code(sample_code)
        results = source.detect_capabilities(sample)
        available_count = sum(1 for item in results if item['available'])
        available_datasets = [item['api_name'] for item in results if item['available']]
        return {
            'sample_code': sample,
            'available_count': available_count,
            'total_count': len(results),
            'results': results,
            'available_datasets': available_datasets,
        }

    def detect_etf_capabilities(self, sample_code: Optional[str] = None) -> dict[str, Any]:
        source = self._ensure_etf_source()
        sample = self._resolve_etf_sample_code(sample_code)
        results = source.detect_capabilities(sample)
        return {
            'sample_code': sample,
            'available_count': sum(1 for item in results if item['available']),
            'total_count': len(results),
            'results': results,
            'available_datasets': [item['api_name'] for item in results if item['available']],
        }

    def _upsert_etf_metadata(self, item: dict[str, Any]) -> None:
        code = item['code']
        self.etf_db.insert_etf(
            code=code,
            ts_code=item.get('ts_code', ''),
            name=item.get('name', code),
            market=item.get('market', ''),
            fund_type=item.get('fund_type', ''),
            management=item.get('management', ''),
            custodian=item.get('custodian', ''),
            benchmark=item.get('benchmark', ''),
            invest_type=item.get('invest_type', ''),
            etf_type=item.get('type', ''),
            status=item.get('status', ''),
            list_date=item.get('list_date', ''),
            due_date=item.get('due_date', ''),
            issue_date=item.get('issue_date', ''),
            delist_date=item.get('delist_date', ''),
            issue_amount=float(item.get('issue_amount') or 0),
            m_fee=float(item.get('m_fee') or 0),
            c_fee=float(item.get('c_fee') or 0),
            duration_year=float(item.get('duration_year') or 0),
            p_value=float(item.get('p_value') or 0),
            min_amount=float(item.get('min_amount') or 0),
            exp_return=float(item.get('exp_return') or 0),
            found_date=item.get('found_date', ''),
        )

    def _normalize_etf_target(self, code: str) -> str:
        return code.strip().upper().split('.')[0]

    def _build_etf_item_map(self, items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        return {item['code']: item for item in items if item.get('code')}

    def _refresh_etf_universe(
        self,
        items: list[dict[str, Any]],
        summary: Optional[dict[str, Any]] = None,
        log: LogFn = None,
    ) -> dict[str, dict[str, Any]]:
        item_map = self._build_etf_item_map(items)
        existing_codes = {item['code'] for item in self.etf_db.get_etf_list(limit=100000)}
        new_codes = [code for code in item_map if code not in existing_codes]
        for code in new_codes:
            self._upsert_etf_metadata(item_map[code])
        if summary is not None:
            summary['new_etfs'] = len(new_codes)
        if log is not None and new_codes:
            self._log(log, f'ETF 标的清单已刷新，新增 {len(new_codes)} 只')
        return item_map

    def detect_feature_capabilities(self, sample_code: Optional[str] = None) -> dict[str, Any]:
        source = self._ensure_feature_source()
        sample = self._resolve_stock_sample_code(sample_code)
        return source.detect_capabilities(sample, include_pending=True)

    def detect_all_capabilities(self) -> dict[str, Any]:
        return {
            'stock': self.detect_capabilities(),
            'etf': self.detect_etf_capabilities(),
            'feature': self.detect_feature_capabilities(),
        }

    def _get_concepts_for_code(self, code: str) -> list[dict[str, Any]]:
        return self.feature_db.get_concepts_for_code(code)

    def _sync_dataset(self, dataset: str, code: str, capabilities: dict[str, bool], conn, log: LogFn) -> tuple[int, str]:
        source = self._ensure_source()
        if not capabilities.get(dataset, False):
            self.db.set_sync_status(code, dataset, 'unavailable', last_error='接口不可用', conn=conn)
            self._log(log, f'  跳过 {dataset}：接口不可用')
            return 0, 'unavailable'

        try:
            if dataset == 'fina_indicator':
                rows = self.db.insert_fina_indicator(code, source.get_fina_indicator(code), conn=conn)
            elif dataset == 'income':
                rows = self.db.insert_income(code, source.get_income(code), conn=conn)
            elif dataset == 'balancesheet':
                rows = self.db.insert_balancesheet(code, source.get_balancesheet(code), conn=conn)
            elif dataset == 'cashflow':
                rows = self.db.insert_cashflow(code, source.get_cashflow(code), conn=conn)
            else:
                rows = 0

            status = 'success' if rows > 0 else 'empty'
            self.db.set_sync_status(code, dataset, status, success=rows > 0, conn=conn)
            return rows, status
        except Exception as exc:
            self.db.set_sync_status(code, dataset, 'failed', last_error=str(exc), conn=conn)
            raise

    def _needs_backfill(self, code: str, dataset: str) -> bool:
        status = self.db.get_sync_status(code, dataset)
        return not status or status.get('last_success_at') is None

    def _sync_stock(self, code: str, name: str, stock: dict[str, Any], capabilities: dict[str, bool], log: LogFn) -> dict[str, int]:
        source = self._ensure_source()
        if not capabilities.get('daily', False):
            raise ValueError('当前 token 无日线接口权限，无法执行导入或更新')

        info = source.get_stock_info(code, capabilities=capabilities)
        daily_df = source.get_daily_prices(code)
        if daily_df.empty:
            self._log(log, '  无日线数据')
            return {'daily_rows': 0, 'financial_rows': 0, 'backfilled': 0}

        with self.db.get_connection() as conn:
            self.db.insert_stock(
                code=code,
                name=info.get('name', name),
                area=info.get('area', stock.get('area', '')),
                industry=info.get('industry', stock.get('industry', '')),
                list_date=info.get('list_date', stock.get('list_date', '')),
                market_cap=info.get('market_cap', 0),
                circ_mv=info.get('circ_mv', 0),
                pe_ratio=info.get('pe_ratio', 0),
                pb_ratio=info.get('pb_ratio', 0),
                turnover_rate=info.get('turnover_rate', 0),
                volume_ratio=info.get('volume_ratio', 0),
                adj_factor=info.get('adj_factor', 0),
                conn=conn,
            )
            daily_rows = self.db.insert_daily_prices(code, daily_df, conn=conn)
            self.db.set_sync_status(code, 'daily', 'success' if daily_rows > 0 else 'empty', success=daily_rows > 0, conn=conn)
            self.db.set_sync_status(code, 'daily_basic', 'success' if capabilities.get('daily_basic', False) else 'unavailable', success=capabilities.get('daily_basic', False), conn=conn)
            self.db.set_sync_status(code, 'adj_factor', 'success' if capabilities.get('adj_factor', False) else 'unavailable', success=capabilities.get('adj_factor', False), conn=conn)

            financial_rows = 0
            backfilled = 0
            for dataset in ['fina_indicator', 'income', 'balancesheet', 'cashflow']:
                before = self.db.get_sync_status(code, dataset)
                rows, _ = self._sync_dataset(dataset, code, capabilities, conn, log)
                financial_rows += rows
                if capabilities.get(dataset, False) and rows > 0 and (not before or before.get('last_success_at') is None):
                    backfilled += 1

        return {'daily_rows': daily_rows, 'financial_rows': financial_rows, 'backfilled': backfilled}

    def _update_one_stock(self, code: str, capabilities: dict[str, bool], log: LogFn = None) -> dict[str, Any]:
        source = self._ensure_source()
        if not capabilities.get('daily', False):
            raise ValueError('当前 token 无日线接口权限，无法执行更新')

        stock = self.db.get_stock_overview(code)
        if stock:
            name = stock.get('name', code)
            latest_date = self.db.get_latest_date(code)
            self._log(log, f'更新 {code} {name}')

            daily_df = source.get_daily_prices(code)
            if daily_df.empty:
                self.db.set_sync_status(code, 'daily', 'empty')
                self._log(log, '  无数据')
                return {'code': code, 'status': 'empty', 'daily_rows': 0, 'financial_rows': 0, 'backfilled': 0}

            if latest_date:
                daily_df = daily_df[daily_df['date'] > latest_date]

            needs_backfill = any(
                self._needs_backfill(code, dataset) and capabilities.get(dataset, False)
                for dataset in ['fina_indicator', 'income', 'balancesheet', 'cashflow']
            )
            if daily_df.empty and not needs_backfill:
                self._log(log, f'  已最新（{latest_date}）')
                return {'code': code, 'status': 'skipped', 'daily_rows': 0, 'financial_rows': 0, 'backfilled': 0}

            info = source.get_stock_info(code, capabilities=capabilities)
            with self.db.get_connection() as conn:
                self.db.insert_stock(
                    code=code,
                    name=info.get('name', name),
                    area=info.get('area', stock.get('area', '')),
                    industry=info.get('industry', stock.get('industry', '')),
                    list_date=info.get('list_date', stock.get('list_date', '')),
                    market_cap=info.get('market_cap', 0),
                    circ_mv=info.get('circ_mv', 0),
                    pe_ratio=info.get('pe_ratio', 0),
                    pb_ratio=info.get('pb_ratio', 0),
                    turnover_rate=info.get('turnover_rate', 0),
                    volume_ratio=info.get('volume_ratio', 0),
                    adj_factor=info.get('adj_factor', 0),
                    conn=conn,
                )
                inserted_daily_rows = self.db.insert_daily_prices(code, daily_df, conn=conn) if not daily_df.empty else 0
                self.db.set_sync_status(code, 'daily', 'success' if inserted_daily_rows > 0 or latest_date else 'empty', success=inserted_daily_rows > 0, conn=conn)
                self.db.set_sync_status(code, 'daily_basic', 'success' if capabilities.get('daily_basic', False) else 'unavailable', success=capabilities.get('daily_basic', False), conn=conn)
                self.db.set_sync_status(code, 'adj_factor', 'success' if capabilities.get('adj_factor', False) else 'unavailable', success=capabilities.get('adj_factor', False), conn=conn)

                financial_rows = 0
                backfilled = 0
                for dataset in ['fina_indicator', 'income', 'balancesheet', 'cashflow']:
                    before = self.db.get_sync_status(code, dataset)
                    rows, _ = self._sync_dataset(dataset, code, capabilities, conn, log)
                    financial_rows += rows
                    if capabilities.get(dataset, False) and rows > 0 and (not before or before.get('last_success_at') is None):
                        backfilled += 1

            self._log(log, f'  成功，新增 {inserted_daily_rows} 条日线，财务 {financial_rows} 条')
            return {'code': code, 'status': 'success', 'daily_rows': inserted_daily_rows, 'financial_rows': financial_rows, 'backfilled': backfilled}

        self._log(log, f'补录 {code}')
        row_stats = self._sync_stock(code, code, {'code': code}, capabilities, log)
        if row_stats['daily_rows'] == 0:
            return {'code': code, 'status': 'empty', 'daily_rows': 0, 'financial_rows': row_stats['financial_rows'], 'backfilled': row_stats['backfilled']}
        self._log(log, f'  成功，新增 {row_stats["daily_rows"]} 条日线，财务 {row_stats["financial_rows"]} 条')
        return {'code': code, 'status': 'success', 'daily_rows': row_stats['daily_rows'], 'financial_rows': row_stats['financial_rows'], 'backfilled': row_stats['backfilled']}

    def import_data(
        self,
        limit: Optional[int] = 100,
        skip_existing: bool = True,
        log: LogFn = None,
    ) -> Dict[str, Any]:
        source = self._ensure_source()
        self._reset_cancel()
        capabilities = self._get_capability_map()
        stocks = source.get_stock_list()
        if limit:
            stocks = stocks[:limit]

        summary = self._make_summary('import', len(stocks), capabilities)
        summary.pop('backfilled', None)
        self._log(log, f'开始导入，股票数: {summary["total"]}，可用接口: {summary["available_datasets"]}')

        for index, stock in enumerate(stocks, 1):
            if self._cancelled:
                summary['cancelled'] = True
                self._log(log, f'[{index}/{summary["total"]}] 任务已取消')
                break

            code = stock['code']
            name = stock.get('name', code)

            if skip_existing and self.db.get_latest_date(code):
                summary['skipped'] += 1
                self._log(log, f'[{index}/{summary["total"]}] 跳过 {code}（已有日线数据）')
                continue

            try:
                self._log(log, f'[{index}/{summary["total"]}] 导入 {code} {name}')
                row_stats = self._sync_stock(code, name, stock, capabilities, log)
                if row_stats['daily_rows'] == 0:
                    summary['failed'] += 1
                    continue
                summary['success'] += 1
                summary['daily_rows'] += row_stats['daily_rows']
                summary['financial_rows'] += row_stats['financial_rows']
                self._log(log, f'  成功，新增 {row_stats["daily_rows"]} 条日线，财务 {row_stats["financial_rows"]} 条')
            except Exception as exc:
                summary['failed'] += 1
                self._log(log, f'  失败: {exc}')
            finally:
                time.sleep(self._request_interval)

        self._log(
            log,
            f'导入完成：成功 {summary["success"]}，失败 {summary["failed"]}，跳过 {summary["skipped"]}，日线 {summary["daily_rows"]}，财务 {summary["financial_rows"]}',
        )
        return summary

    def update_data(self, log: LogFn = None, code: Optional[str] = None) -> Dict[str, Any]:
        self._ensure_source()
        self._reset_cancel()
        capabilities = self._get_capability_map(code)

        if code:
            normalized_code = code.strip().upper().split('.')[0]
            summary = self._make_summary('update', 1, capabilities)
            self._log(log, f'开始更新指定股票 {normalized_code}，可用接口: {summary["available_datasets"]}')
            result = self._update_one_stock(normalized_code, capabilities, log=log)
            if result['status'] == 'success':
                summary['success'] = 1
            elif result['status'] == 'skipped':
                summary['skipped'] = 1
            else:
                summary['failed'] = 1 if result['status'] not in {'empty'} else 0
                summary['skipped'] += 1 if result['status'] == 'empty' else 0
            summary['daily_rows'] = result['daily_rows']
            summary['financial_rows'] = result['financial_rows']
            summary['backfilled'] = result['backfilled']
            summary['code'] = normalized_code
            summary['status'] = result['status']
            self._log(log, f'指定股票更新完成：{normalized_code}，状态 {result["status"]}，日线 {result["daily_rows"]}，财务 {result["financial_rows"]}，回填 {result["backfilled"]}')
            return summary

        stocks = self.db.get_stock_list()
        summary = self._make_summary('update', len(stocks), capabilities)
        self._log(log, f'开始更新，股票数: {summary["total"]}，可用接口: {summary["available_datasets"]}')

        if not capabilities.get('daily', False):
            raise ValueError('当前 token 无日线接口权限，无法执行更新')

        for index, stock in enumerate(stocks, 1):
            if self._cancelled:
                summary['cancelled'] = True
                self._log(log, f'[{index}/{summary["total"]}] 任务已取消')
                break

            code = stock['code']
            try:
                self._log(log, f'[{index}/{summary["total"]}] 更新 {code} {stock.get("name", code)}')
                result = self._update_one_stock(code, capabilities, log=None)
                if result['status'] == 'success':
                    summary['success'] += 1
                    summary['daily_rows'] += result['daily_rows']
                    summary['financial_rows'] += result['financial_rows']
                    summary['backfilled'] += result['backfilled']
                elif result['status'] in {'skipped', 'empty'}:
                    summary['skipped'] += 1
                else:
                    summary['failed'] += 1
            except Exception as exc:
                summary['failed'] += 1
                self._log(log, f'  失败: {exc}')
            finally:
                time.sleep(self._request_interval)

        self._log(
            log,
            f'更新完成：成功 {summary["success"]}，失败 {summary["failed"]}，跳过 {summary["skipped"]}，日线 {summary["daily_rows"]}，财务 {summary["financial_rows"]}，回填 {summary["backfilled"]}',
        )
        return summary

    def import_etf_data(self, limit: Optional[int] = 100, skip_existing: bool = True, log: LogFn = None) -> dict[str, Any]:
        source = self._ensure_etf_source()
        self._reset_cancel()
        capabilities = source.capability_map(source.detect_capabilities(self._resolve_etf_sample_code()))
        items = source.get_etf_list()
        if limit:
            items = items[:limit]
        summary = {
            'mode': 'etf_import',
            'total': len(items),
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'cancelled': False,
            'daily_rows': 0,
            'available_datasets': [name for name in ['fund_basic', 'fund_daily'] if capabilities.get(name)],
            'failure_reasons': {},
        }
        for index, item in enumerate(items, 1):
            if self._cancelled:
                summary['cancelled'] = True
                break
            code = item['code']
            if skip_existing and self.etf_db.get_latest_date(code):
                summary['skipped'] += 1
                continue
            try:
                self._upsert_etf_metadata(item)
                daily_df = source.get_etf_daily_prices(code, ts_code=item.get('ts_code'))
                daily_rows = self.etf_db.insert_daily_prices(code, daily_df)
                if daily_rows == 0:
                    summary['failed'] += 1
                    summary['failure_reasons']['empty_daily'] = summary['failure_reasons'].get('empty_daily', 0) + 1
                    self.etf_db.set_sync_status(code, 'fund_daily', 'empty', last_error='未返回ETF日线数据', success=False)
                    self._log(log, f'[{index}/{summary["total"]}] ETF {code} 导入失败：无日线数据')
                    continue
                self.etf_db.set_sync_status(code, 'fund_daily', 'success', success=True)
                summary['success'] += 1
                summary['daily_rows'] += daily_rows
                self._log(log, f'[{index}/{summary["total"]}] ETF {code} 导入完成，日线数 {daily_rows}')
            except Exception as exc:
                summary['failed'] += 1
                reason = 'request_error'
                summary['failure_reasons'][reason] = summary['failure_reasons'].get(reason, 0) + 1
                self._log(log, f'[{index}/{summary["total"]}] ETF {code} 导入失败: {exc}')
            finally:
                time.sleep(self._request_interval)
        return summary

    def update_etf_data(self, log: LogFn = None, code: Optional[str] = None) -> dict[str, Any]:
        source = self._ensure_etf_source()
        self._reset_cancel()
        all_items = source.get_etf_list()
        item_map = self._build_etf_item_map(all_items)
        summary = {
            'mode': 'etf_update',
            'total': 0,
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'cancelled': False,
            'daily_rows': 0,
            'new_etfs': 0,
        }

        if code:
            target = self._normalize_etf_target(code)
            if target in item_map:
                self._upsert_etf_metadata(item_map[target])
                summary['new_etfs'] = 0 if self.etf_db.get_etf_overview(target) else 1
            elif not self.etf_db.get_etf_overview(target):
                raise ValueError(f'未找到 ETF: {target}')
            targets = [target]
        else:
            item_map = self._refresh_etf_universe(all_items, summary=summary, log=log)
            targets = sorted({*item_map.keys(), *(item['code'] for item in self.etf_db.get_etf_list(limit=100000))})

        summary['total'] = len(targets)
        for index, target in enumerate(targets, 1):
            if self._cancelled:
                summary['cancelled'] = True
                break
            try:
                item = item_map.get(target)
                if item:
                    self._upsert_etf_metadata(item)
                latest_date = self.etf_db.get_latest_date(target)
                start_date = latest_date.replace('-', '') if latest_date else None
                daily_df = source.get_etf_daily_prices(target, start_date=start_date, ts_code=item.get('ts_code') if item else None)
                if daily_df.empty:
                    summary['skipped'] += 1
                    self._log(log, f'[{index}/{summary["total"]}] ETF {target} 已最新（{latest_date}）')
                    continue
                if latest_date:
                    daily_df = daily_df[daily_df['date'] > latest_date]
                if daily_df.empty:
                    summary['skipped'] += 1
                    self._log(log, f'[{index}/{summary["total"]}] ETF {target} 已最新（{latest_date}）')
                    continue
                daily_rows = self.etf_db.insert_daily_prices(target, daily_df)
                self.etf_db.set_sync_status(target, 'fund_daily', 'success', success=daily_rows > 0)
                summary['success'] += 1
                summary['daily_rows'] += daily_rows
                self._log(log, f'[{index}/{summary["total"]}] ETF {target} 更新完成，新增日线 {daily_rows}')
            except Exception as exc:
                summary['failed'] += 1
                self._log(log, f'[{index}/{summary["total"]}] ETF {target} 更新失败: {exc}')
            finally:
                time.sleep(self._request_interval)
        return summary

    def sync_feature_concepts(self, log: LogFn = None) -> dict[str, Any]:
        source = self._ensure_feature_source()
        self._log(log, '开始同步概念基表：刷新概念列表并更新概念成员')
        concepts = source.get_concepts()
        concept_rows = self.feature_db.insert_concepts(concepts)
        if not concepts:
            self.feature_db.set_sync_status('concepts', 'global', 'all', 'empty', row_count=0, success=False)
            self._log(log, '概念基表同步完成：未返回概念数据')
            return {'dataset': 'concepts', 'concept_rows': 0, 'member_rows': 0}
        member_rows = 0
        for index, concept in enumerate(concepts, 1):
            if self._cancelled:
                self._log(log, f'[{index}/{len(concepts)}] 已收到停止请求，概念基表同步终止')
                break
            members = source.get_concept_members(concept['concept_id'])
            member_rows += self.feature_db.replace_stock_concepts(concept['concept_id'], members)
            self._log(log, f'[{index}/{len(concepts)}] 概念 {concept.get("concept_name", concept["concept_id"])} 成分 {len(members)} 条')
            time.sleep(self._request_interval)
        self.feature_db.set_sync_status('concepts', 'global', 'all', 'success', row_count=concept_rows + member_rows, success=True)
        self._log(log, f'概念同步完成：概念 {concept_rows}，成分 {member_rows}')
        return {'dataset': 'concepts', 'concept_rows': concept_rows, 'member_rows': member_rows}

    def sync_feature_moneyflow(self, code: str, start_date: Optional[str] = None, end_date: Optional[str] = None, log: LogFn = None) -> dict[str, Any]:
        source = self._ensure_feature_source()
        self._log(log, f'步骤 1/2：同步资金流向 {code}（仅资金流向，不含概念/龙虎榜）')
        rows = source.get_moneyflow(code, start_date=start_date, end_date=end_date)
        inserted = self.feature_db.insert_moneyflow(rows)
        self.feature_db.set_sync_status('moneyflow', 'code', code, 'success' if inserted > 0 else 'empty', row_count=inserted, success=inserted > 0)
        if inserted == 0:
            self._log(log, f'资金流向同步完成：{code} 无新增数据')
        else:
            self._log(log, f'资金流向同步完成：{code} {inserted} 条')
        return {'dataset': 'moneyflow', 'code': code, 'rows': inserted}

    def sync_feature_market_wide(self, trade_date: Optional[str] = None, log: LogFn = None) -> dict[str, Any]:
        self._log(log, '开始同步全市场扩展（当前仅包含龙虎榜事件，不包含资金流向）')
        result = self.sync_feature_top_list(trade_date=trade_date, log=log)
        result['scope'] = 'market_wide'
        return result

    def sync_feature_for_stock(self, code: str, start_date: Optional[str] = None, end_date: Optional[str] = None, log: LogFn = None) -> dict[str, Any]:
        self._log(log, f'开始同步单股扩展：{code}（步骤 1/2 资金流向，步骤 2/2 概念归属）')
        moneyflow = self.sync_feature_moneyflow(code, start_date=start_date, end_date=end_date, log=log)
        if self._cancelled:
            self._log(log, f'已收到停止请求，跳过 {code} 的概念归属同步')
            return {
                'scope': 'single_stock',
                'code': code,
                'moneyflow_rows': moneyflow.get('rows', 0),
                'concept_rows': 0,
                'cancelled': True,
            }
        concepts = {'dataset': 'concepts', 'rows': 0}
        try:
            source = self._ensure_feature_source()
            self._log(log, f'步骤 2/2：同步 {code} 的概念归属')
            concept_items = source.get_concepts()
            if concept_items:
                self.feature_db.insert_concepts(concept_items)
                for concept in concept_items:
                    if self._cancelled:
                        self._log(log, f'已收到停止请求，{code} 的概念归属同步终止')
                        break
                    members = source.get_concept_members(concept['concept_id'])
                    filtered = [item for item in members if item.get('code') == code]
                    if filtered:
                        self.feature_db.replace_stock_concepts(concept['concept_id'], filtered)
                        concepts['rows'] += len(filtered)
                self._log(log, f'单股概念同步完成：{code} 命中 {concepts["rows"]} 条概念成分')
            else:
                self._log(log, f'单股概念同步完成：{code} 未返回概念数据')
        except Exception as exc:
            self._log(log, f'概念同步失败: {exc}')
        return {
            'scope': 'single_stock',
            'code': code,
            'moneyflow_rows': moneyflow.get('rows', 0),
            'concept_rows': concepts['rows'],
        }

    def sync_feature_moneyflow_all(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        log: LogFn = None,
    ) -> dict[str, Any]:
        """批量同步全部已入库股票的资金流向（逐只调用 Tushare moneyflow 接口）"""
        stocks = self.db.get_stock_list(limit=None)
        total = len(stocks)
        self._log(log, f'开始全量资金流向同步，共 {total} 只股票')
        summary = {
            'dataset': 'moneyflow_all',
            'total': total,
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'total_rows': 0,
        }
        for index, stock in enumerate(stocks, 1):
            if self._cancelled:
                summary['cancelled'] = True
                self._log(log, f'[{index}/{total}] 任务已取消')
                break
            code = stock['code']
            try:
                result = self.sync_feature_moneyflow(code, start_date=start_date, end_date=end_date)
                inserted = result.get('rows', 0)
                summary['total_rows'] += inserted
                if inserted > 0:
                    summary['success'] += 1
                    self._log(log, f'[{index}/{total}] {code} 资金流向 {inserted} 条')
                else:
                    summary['skipped'] += 1
                    self._log(log, f'[{index}/{total}] {code} 无新增资金流向数据')
            except Exception as exc:
                summary['failed'] += 1
                self._log(log, f'[{index}/{total}] {code} 资金流向失败: {exc}')
            finally:
                time.sleep(self._request_interval)
        self._log(log, f'全量资金流向同步完成：成功 {summary["success"]}，跳过 {summary["skipped"]}，失败 {summary["failed"]}，共 {summary["total_rows"]} 条')
        return summary

    def sync_feature_top_list(self, code: Optional[str] = None, trade_date: Optional[str] = None, log: LogFn = None) -> dict[str, Any]:
        source = self._ensure_feature_source()
        # source.get_top_list 返回 list[dict]（events），traders 目前为空列表
        events = source.get_top_list(code=code, trade_date=trade_date)
        traders = source.get_top_list_traders(code=code, trade_date=trade_date) if hasattr(source, 'get_top_list_traders') else []
        event_ids = self.feature_db.insert_top_list_events(events)
        # replace_top_list_traders 每次针对一个 event_id；批量时逐一写入
        trader_rows = 0
        if traders and event_ids:
            for eid in event_ids:
                trader_rows += self.feature_db.replace_top_list_traders(eid, traders)
        rows = len(event_ids)
        scope_key = code or trade_date or 'all'
        self.feature_db.set_sync_status('top_list', 'scope', scope_key, 'success' if rows > 0 else 'empty', row_count=rows, success=rows > 0)
        self._log(log, f'龙虎榜同步完成：事件 {rows} 条，席位 {trader_rows} 条')
        return {'dataset': 'top_list', 'rows': rows, 'trader_rows': trader_rows, 'code': code, 'trade_date': trade_date}
