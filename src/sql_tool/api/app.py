"""本地 HTTP API"""

from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from sql_tool.services import SqlToolService


class StockUpdateRequest(BaseModel):
    code: str | None = None


class EtfUpdateRequest(BaseModel):
    code: str | None = None


class FeatureMoneyflowRequest(BaseModel):
    code: str
    start_date: str | None = None
    end_date: str | None = None


class FeatureTopListRequest(BaseModel):
    code: str | None = None
    trade_date: str | None = None


class MoneyflowAllRequest(BaseModel):
    start_date: str | None = None
    end_date: str | None = None


class IndexForecastRequest(BaseModel):
    index_name: str
    forecast_month: str
    forecast_direction: str
    stock_code: str
    stock_name: str
    broker_name: str
    source_note: str | None = None


class IndexEntityRequest(BaseModel):
    index_name: str
    benchmark: str = ''
    index_code: str = ''
    category: str = '宽基'


class SnapshotRequest(BaseModel):
    index_id: int
    trade_date: str
    note: str = ''
    effective_date: str = ''
    announcement_date: str = ''
    items: list[dict] = []


class IndexChangeRequest(BaseModel):
    index_id: int
    trade_date: str
    changes: list[dict]
    from_snapshot_id: int | None = None
    to_snapshot_id: int | None = None


class DailyBatchRequest(BaseModel):
    codes: list[str]
    start_date: str | None = None
    end_date: str | None = None
    limit_per_code: int | None = None


class StockOverviewBatchRequest(BaseModel):
    codes: list[str]


class ThemeMappingItem(BaseModel):
    impact_level: str          # 'direct' | 'indirect' | 'phonetic'
    target_type: str           # 'industry' | 'concept' | 'stock'
    target_name: str
    code: str = ''
    notes: str = ''
    relevance: float = 0.5     # 0.0~1.0 相关性评分


class HolidayRequest(BaseModel):
    holiday_date: str          # YYYY-MM-DD
    name: str
    market_scope: str = 'CN'
    is_trading_closed: bool = True
    notes: str = ''
    mappings: list[ThemeMappingItem] = []


class MajorEventRequest(BaseModel):
    event_date: str            # YYYY-MM-DD
    name: str
    category: str              # 'sports' | 'expo' | 'policy' | 'economy' | 'other'
    location: str = ''
    notes: str = ''
    mappings: list[ThemeMappingItem] = []


def create_app(service: SqlToolService | None = None) -> FastAPI:
    service = service or SqlToolService()
    app = FastAPI(title='sql-tool API', version='0.2.0', description='Tushare 本地数据库 HTTP API')

    @app.get('/health')
    def health() -> dict[str, str]:
        return {'status': 'ok'}

    @app.get('/stats')
    def stats() -> dict:
        return service.get_all_stats()

    @app.get('/stocks')
    def stocks(limit: int = Query(default=200, ge=1, le=2000), search: str = '') -> list[dict]:
        return service.get_stocks(limit=limit, search=search)

    @app.get('/stocks/page')
    def stocks_page(
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=200, ge=1, le=1000),
        search: str = '',
        industry: str = '',
        area: str = '',
        order_by: str = 'code',
        order: str = 'asc',
    ) -> dict:
        return service.get_stock_page(
            page=page,
            page_size=page_size,
            search=search,
            industry=industry,
            area=area,
            order_by=order_by,
            order=order,
        )

    @app.get('/stocks/{code}/daily')
    def stock_daily(
        code: str,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int = Query(default=60, ge=1, le=2000),
    ) -> dict:
        stock = service.db.get_stock_overview(code)
        if not stock:
            raise HTTPException(status_code=404, detail='股票不存在')
        return {
            'stock': stock,
            'items': service.get_stock_daily(code, start_date=start_date, end_date=end_date, limit=limit),
        }

    @app.get('/stocks/{code}/financials')
    def stock_financials(code: str, limit: int = Query(default=8, ge=1, le=40)) -> dict:
        payload = service.get_stock_financials(code, limit=limit)
        if not payload['stock']:
            raise HTTPException(status_code=404, detail='股票不存在')
        return payload

    @app.get('/capabilities')
    def capabilities(sample_code: str | None = None) -> dict:
        if sample_code:
            stock_caps = service.detect_capabilities(sample_code=sample_code)
            etf_caps = service.detect_etf_capabilities()
            feature_caps = service.detect_feature_capabilities(sample_code=sample_code)
            return {'stock': stock_caps, 'etf': etf_caps, 'feature': feature_caps}
        return service.detect_all_capabilities()

    @app.post('/daily/batch')
    def daily_batch(payload: DailyBatchRequest) -> dict:
        if not payload.codes:
            raise HTTPException(status_code=400, detail='codes 不能为空')
        return service.get_daily_batch(
            codes=payload.codes,
            start_date=payload.start_date,
            end_date=payload.end_date,
            limit_per_code=payload.limit_per_code,
        )

    @app.post('/stocks/overview/batch')
    def stock_overview_batch(payload: StockOverviewBatchRequest) -> dict:
        if not payload.codes:
            raise HTTPException(status_code=400, detail='codes 不能为空')
        return {'items': service.get_stock_overviews(payload.codes)}

    @app.post('/stocks/{code}/update')
    def update_stock(code: str) -> dict:
        try:
            return service.update_data(log=None, code=code)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post('/stocks/update')
    def update_stock_by_body(payload: StockUpdateRequest) -> dict:
        if not payload.code:
            raise HTTPException(status_code=400, detail='code 不能为空')
        try:
            return service.update_data(log=None, code=payload.code)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get('/etfs')
    def etfs(limit: int = Query(default=200, ge=1, le=2000), search: str = '') -> list[dict]:
        return service.get_etfs(limit=limit, search=search)

    @app.get('/etfs/page')
    def etfs_page(
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=200, ge=1, le=1000),
        search: str = '',
        fund_type: str = '',
        market: str = '',
        order_by: str = 'code',
        order: str = 'asc',
    ) -> dict:
        return service.get_etf_page(
            page=page,
            page_size=page_size,
            search=search,
            fund_type=fund_type,
            market=market,
            order_by=order_by,
            order=order,
        )

    @app.get('/etfs/{code}/daily')
    def etf_daily(
        code: str,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int = Query(default=60, ge=1, le=2000),
    ) -> dict:
        etf = service.etf_db.get_etf_overview(code)
        if not etf:
            raise HTTPException(status_code=404, detail='ETF 不存在')
        return {
            'etf': etf,
            'items': service.get_etf_daily(code, start_date=start_date, end_date=end_date, limit=limit),
        }

    @app.post('/etfs/{code}/update')
    def update_etf(code: str) -> dict:
        try:
            return service.update_etf_data(log=None, code=code)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post('/etfs/update')
    def update_etf_by_body(payload: EtfUpdateRequest) -> dict:
        if not payload.code:
            raise HTTPException(status_code=400, detail='code 不能为空')
        try:
            return service.update_etf_data(log=None, code=payload.code)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get('/feature/capabilities')
    def feature_capabilities(sample_code: str | None = None) -> dict:
        return service.detect_feature_capabilities(sample_code=sample_code)

    @app.get('/concepts')
    def concepts() -> list[dict]:
        return service.get_concepts()

    @app.get('/concepts/{concept_id}/stocks')
    def concept_members(concept_id: str) -> dict:
        return {'items': service.get_concept_members(concept_id)}

    @app.get('/events/holidays')
    def holidays() -> dict:
        return {'items': service.get_holidays()}

    @app.get('/events/major')
    def events() -> dict:
        return {'items': service.get_events()}

    @app.get('/indexes')
    def indexes(category: str | None = None) -> dict:
        return {'items': service.get_index_entities(category=category)}

    @app.post('/indexes/entities')
    def create_index_entity(payload: IndexEntityRequest) -> dict:
        index_id = service.index_db.upsert_index_entity(
            index_name=payload.index_name,
            benchmark=payload.benchmark,
            index_code=payload.index_code,
            category=payload.category,
        )
        return {'index_id': index_id}

    @app.post('/indexes/snapshots')
    def create_snapshot(payload: SnapshotRequest) -> dict:
        snapshot_id = service.index_db.create_snapshot(
            index_id=payload.index_id,
            trade_date=payload.trade_date,
            note=payload.note,
            effective_date=payload.effective_date,
            announcement_date=payload.announcement_date,
        )
        rows = service.index_db.replace_snapshot_items(snapshot_id, payload.items)
        return {'snapshot_id': snapshot_id, 'rows': rows}

    @app.post('/indexes/changes')
    def create_changes(payload: IndexChangeRequest) -> dict:
        rows = service.index_db.replace_changes(
            index_id=payload.index_id,
            trade_date=payload.trade_date,
            changes=payload.changes,
            from_snapshot_id=payload.from_snapshot_id,
            to_snapshot_id=payload.to_snapshot_id,
        )
        return {'rows': rows}

    @app.post('/indexes/{index_id}/analyze-changes')
    def analyze_index_changes(index_id: int) -> dict:
        return service.analyze_index_constituent_changes(index_id)

    @app.get('/indexes/{index_id}/changes')
    def index_changes(index_id: int) -> dict:
        return {'items': service.get_index_changes(index_id)}

    @app.get('/index-forecasts')
    def index_forecasts() -> dict:
        return {'items': service.get_index_forecasts()}

    @app.post('/index-forecasts')
    def add_index_forecast(payload: IndexForecastRequest) -> dict:
        service.add_index_forecast(
            index_name=payload.index_name,
            forecast_month=payload.forecast_month,
            forecast_direction=payload.forecast_direction,
            stock_code=payload.stock_code,
            stock_name=payload.stock_name,
            broker_name=payload.broker_name,
            source_note=payload.source_note or '',
        )
        return {'status': 'ok'}

    @app.post('/index-forecasts/export')
    def export_index_forecasts() -> dict:
        return {
            'csv': service.export_index_forecasts_csv('data/index_forecasts_export.csv'),
            'excel': service.export_index_forecasts_excel('data/index_forecasts_export.xlsx'),
            'pdf': service.export_index_forecasts_pdf('data/index_forecasts_export.pdf'),
        }

    @app.post('/indexes/derive')
    def derive_indexes() -> dict:
        return service.derive_index_entities_from_etfs()

    @app.post('/events/seed')
    def seed_events() -> dict:
        return service.seed_event_data()

    @app.post('/events/holidays')
    def create_holiday(payload: HolidayRequest) -> dict:
        holiday_id = service.upsert_holiday(
            holiday_date=payload.holiday_date,
            name=payload.name,
            market_scope=payload.market_scope,
            is_trading_closed=payload.is_trading_closed,
            notes=payload.notes,
        )
        mapping_count = service.replace_holiday_mappings(
            holiday_id, [m.model_dump() for m in payload.mappings]
        )
        return {'holiday_id': holiday_id, 'mapping_count': mapping_count}

    @app.get('/events/holidays/{holiday_id}/mappings')
    def get_holiday_mappings(holiday_id: int) -> dict:
        return {'items': service.get_holiday_mappings(holiday_id)}

    @app.post('/events/major')
    def create_major_event(payload: MajorEventRequest) -> dict:
        event_id = service.upsert_event(
            event_date=payload.event_date,
            name=payload.name,
            category=payload.category,
            location=payload.location,
            notes=payload.notes,
        )
        mapping_count = service.replace_event_mappings(
            event_id, [m.model_dump() for m in payload.mappings]
        )
        return {'event_id': event_id, 'mapping_count': mapping_count}

    @app.get('/events/major/{event_id}/mappings')
    def get_event_mappings(event_id: int) -> dict:
        return {'items': service.get_event_mappings(event_id)}

    @app.delete('/events/holidays/{holiday_id}')
    def delete_holiday(holiday_id: int) -> dict:
        deleted = service.delete_holiday(holiday_id)
        if not deleted:
            raise HTTPException(status_code=404, detail='节假日不存在')
        return {'deleted': holiday_id}

    @app.delete('/events/holidays')
    def clear_holidays() -> dict:
        count = service.clear_holidays()
        return {'cleared': count}

    @app.delete('/events/major/{event_id}')
    def delete_event(event_id: int) -> dict:
        deleted = service.delete_event(event_id)
        if not deleted:
            raise HTTPException(status_code=404, detail='事件不存在')
        return {'deleted': event_id}

    @app.delete('/events/major')
    def clear_events() -> dict:
        count = service.clear_events()
        return {'cleared': count}

    @app.get('/stocks/{code}/features')
    def stock_features(code: str, start_date: str | None = None, end_date: str | None = None) -> dict:
        payload = service.get_stock_feature_profile(code, start_date=start_date, end_date=end_date)
        if not payload['stock']:
            raise HTTPException(status_code=404, detail='股票不存在')
        return payload

    @app.post('/features/sync/concepts')
    def sync_feature_concepts() -> dict:
        return service.sync_feature_concepts(log=None)

    @app.post('/features/sync/moneyflow')
    def sync_feature_moneyflow(payload: FeatureMoneyflowRequest) -> dict:
        return service.sync_feature_moneyflow(payload.code, start_date=payload.start_date, end_date=payload.end_date, log=None)

    @app.post('/features/sync/moneyflow/all')
    def sync_feature_moneyflow_all(payload: MoneyflowAllRequest) -> dict:
        return service.sync_feature_moneyflow_all(start_date=payload.start_date, end_date=payload.end_date, log=None)

    @app.post('/features/sync/top-list')
    def sync_feature_top_list(payload: FeatureTopListRequest) -> dict:
        return service.sync_feature_top_list(code=payload.code, trade_date=payload.trade_date, log=None)

    return app
