import pandas as pd
from fastapi.testclient import TestClient

from sql_tool.api.app import create_app
from sql_tool.services.core import SqlToolService


class FakeApiService(SqlToolService):
    def __init__(self):
        pass

    def get_stats(self):
        return {'stock_count': 1, 'price_count': 2}

    def get_all_stats(self):
        return {
            'stocks': {'stock_count': 1, 'price_count': 2, 'table_counts': {'stocks': 1}},
            'etfs': {'etf_count': 1, 'price_count': 2, 'table_counts': {'etfs': 1}},
            'features': {'concept_count': 1, 'table_counts': {'concepts': 1}},
        }

    def get_stocks(self, limit=None, search=''):
        return [{'code': '000001', 'name': '平安银行'}]

    def get_etfs(self, limit=None, search=''):
        return [{'code': '510300', 'name': '沪深300ETF'}]

    def get_etf_page(self, page=1, page_size=200, search='', fund_type='', market='', order_by='code', order='asc'):
        return {
            'items': [{'code': '510300', 'name': '沪深300ETF'}],
            'pagination': {'page': page, 'page_size': page_size, 'total': 1, 'pages': 1},
        }

    def get_etf_daily(self, code, start_date=None, end_date=None, limit=None):
        return [{'date': '2024-04-02', 'close': 4.5}]

    def get_stock_page(self, page=1, page_size=200, search='', industry='', area='', order_by='code', order='asc'):
        return {
            'items': [{'code': '000001', 'name': '平安银行'}],
            'pagination': {'page': page, 'page_size': page_size, 'total': 1, 'pages': 1},
        }

    def get_stock_daily(self, code, start_date=None, end_date=None, limit=None):
        return [{'date': '2024-04-02', 'close': 10.5}]

    def get_stock_financials(self, code, limit=8):
        return {'stock': {'code': code, 'name': '平安银行'}, 'financials': {'income': [{'end_date': '20240331'}]}}

    def detect_capabilities(self, sample_code=None):
        return {'sample_code': sample_code or '000001', 'available_count': 1, 'total_count': 1, 'results': []}

    def detect_etf_capabilities(self, sample_code=None):
        return {'sample_code': sample_code or '510300', 'available_count': 2, 'total_count': 2, 'results': []}

    def detect_feature_capabilities(self, sample_code=None):
        return {'sample_code': sample_code or '000001', 'available_count': 3, 'total_count': 4, 'results': [], 'pending': [{'api_name': 'auction', 'error': '待确认'}]}

    def detect_all_capabilities(self):
        return {
            'stock': self.detect_capabilities(),
            'etf': self.detect_etf_capabilities(),
            'feature': self.detect_feature_capabilities(),
        }

    def get_daily_batch(self, codes, start_date=None, end_date=None, limit_per_code=None):
        return {
            'items': [{'code': c, 'date': '20240102', 'close': 10.0} for c in codes],
            'meta': {'codes': len(codes), 'rows': len(codes), 'start_date': start_date, 'end_date': end_date, 'limit_per_code': limit_per_code},
        }

    def get_stock_overviews(self, codes):
        return [{'code': c, 'name': '平安银行', 'latest_close': 10.0} for c in codes]

    def get_concepts(self):
        return [{'concept_id': 'TS1', 'concept_name': '新能源车'}]

    def get_concept_members(self, concept_id):
        return [{'code': '000001', 'concept_id': concept_id}]

    def get_stock_feature_profile(self, code, start_date=None, end_date=None):
        return {'stock': {'code': code, 'name': '平安银行'}, 'concepts': [], 'moneyflow': [], 'top_list': []}

    def update_etf_data(self, log=None, code=None):
        return {'status': 'success', 'daily_rows': 1, 'success': 1, 'failed': 0, 'skipped': 0}

    def sync_feature_concepts(self, log=None):
        return {'dataset': 'concepts', 'concept_rows': 1, 'member_rows': 1}

    def sync_feature_moneyflow(self, code, start_date=None, end_date=None, log=None):
        return {'dataset': 'moneyflow', 'code': code, 'rows': 1}

    def sync_feature_top_list(self, code=None, trade_date=None, log=None):
        return {'dataset': 'top_list', 'rows': 1}

    class db:
        @staticmethod
        def get_stock_overview(code):
            return {'code': code, 'name': '平安银行'}

    class etf_db:
        @staticmethod
        def get_etf_overview(code):
            return {'code': code, 'name': '沪深300ETF'}


def test_api_endpoints():
    client = TestClient(create_app(FakeApiService()))

    assert client.get('/health').json()['status'] == 'ok'
    assert 'stocks' in client.get('/stats').json()
    assert client.get('/stocks').json()[0]['code'] == '000001'
    assert client.get('/stocks/000001/daily').json()['items'][0]['date'] == '2024-04-02'
    assert client.get('/stocks/000001/financials').json()['stock']['code'] == '000001'
    assert 'stock' in client.get('/capabilities').json()
    assert client.get('/etfs').json()[0]['code'] == '510300'
    assert client.get('/etfs/510300/daily').json()['items'][0]['date'] == '2024-04-02'
    assert client.get('/feature/capabilities').json()['available_count'] == 3
    assert client.get('/concepts').json()[0]['concept_id'] == 'TS1'
    assert client.get('/concepts/TS1/stocks').json()['items'][0]['code'] == '000001'
    assert client.get('/stocks/000001/features').json()['stock']['code'] == '000001'


def test_stocks_page():
    client = TestClient(create_app(FakeApiService()))

    resp = client.get('/stocks/page', params={'page': 1, 'page_size': 50})
    assert resp.status_code == 200
    body = resp.json()
    assert 'items' in body
    assert 'pagination' in body
    assert body['pagination']['page'] == 1
    assert body['items'][0]['code'] == '000001'


def test_daily_batch():
    client = TestClient(create_app(FakeApiService()))

    payload = {'codes': ['000001', '002594'], 'start_date': '20240101', 'end_date': '20241231'}
    resp = client.post('/daily/batch', json=payload)
    assert resp.status_code == 200
    body = resp.json()
    assert 'items' in body
    assert 'meta' in body
    assert body['meta']['codes'] == 2
    assert len(body['items']) == 2

    resp_empty = client.post('/daily/batch', json={'codes': []})
    assert resp_empty.status_code == 400


def test_stock_overview_batch():
    client = TestClient(create_app(FakeApiService()))

    payload = {'codes': ['000001', '002594', '600519']}
    resp = client.post('/stocks/overview/batch', json=payload)
    assert resp.status_code == 200
    body = resp.json()
    assert 'items' in body
    assert len(body['items']) == 3
    assert body['items'][0]['code'] == '000001'

    resp_empty = client.post('/stocks/overview/batch', json={'codes': []})
    assert resp_empty.status_code == 400
