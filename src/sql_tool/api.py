"""本地 HTTP API"""

from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query

from sql_tool.service import SqlToolService


def create_app(service: SqlToolService | None = None) -> FastAPI:
    service = service or SqlToolService()
    app = FastAPI(title='sql-tool API', version='0.2.0', description='Tushare 本地数据库 HTTP API')

    @app.get('/health')
    def health() -> dict[str, str]:
        return {'status': 'ok'}

    @app.get('/stats')
    def stats() -> dict:
        return service.get_stats()

    @app.get('/stocks')
    def stocks(limit: int = Query(default=200, ge=1, le=5000), search: str = '') -> list[dict]:
        return service.get_stocks(limit=limit, search=search)

    @app.get('/stocks/{code}/daily')
    def stock_daily(
        code: str,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int = Query(default=60, ge=1, le=5000),
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
        return service.detect_capabilities(sample_code=sample_code)

    return app
