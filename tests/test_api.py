from fastapi.testclient import TestClient

from sql_tool.api import create_app
from sql_tool.service import SqlToolService


class FakeApiService(SqlToolService):
    def __init__(self):
        pass

    def get_stats(self):
        return {'stock_count': 1, 'price_count': 2}

    def get_stocks(self, limit=None, search=''):
        return [{'code': '000001', 'name': '平安银行'}]

    def get_stock_daily(self, code, start_date=None, end_date=None, limit=None):
        return [{'date': '2024-04-02', 'close': 10.5}]

    def get_stock_financials(self, code, limit=8):
        return {'stock': {'code': code, 'name': '平安银行'}, 'financials': {'income': [{'end_date': '20240331'}]}}

    def detect_capabilities(self, sample_code=None):
        return {'sample_code': sample_code or '000001', 'available_count': 1, 'total_count': 1, 'results': []}

    class db:
        @staticmethod
        def get_stock_overview(code):
            return {'code': code, 'name': '平安银行'}


def test_api_endpoints():
    client = TestClient(create_app(FakeApiService()))

    assert client.get('/health').json()['status'] == 'ok'
    assert client.get('/stats').json()['stock_count'] == 1
    assert client.get('/stocks').json()[0]['code'] == '000001'
    assert client.get('/stocks/000001/daily').json()['items'][0]['date'] == '2024-04-02'
    assert client.get('/stocks/000001/financials').json()['stock']['code'] == '000001'
    assert client.get('/capabilities').json()['available_count'] == 1
