import pandas as pd

from sql_tool.service import SqlToolService


class FakeSource:
    def get_stock_list(self):
        return [{'code': '000001', 'name': '平安银行', 'industry': '银行', 'area': '深圳', 'list_date': '19910403'}]

    def get_stock_info(self, code):
        return {
            'name': '平安银行',
            'area': '深圳',
            'industry': '银行',
            'list_date': '19910403',
            'market_cap': 1,
            'circ_mv': 1,
            'pe_ratio': 1,
            'pb_ratio': 1,
            'turnover_rate': 1,
            'volume_ratio': 1,
            'adj_factor': 1,
        }

    def get_daily_prices(self, code):
        return pd.DataFrame([
            {'date': '2024-04-01', 'open': 10.0, 'high': 10.5, 'low': 9.8, 'close': 10.2, 'volume': 1000, 'amount': 5000},
            {'date': '2024-04-02', 'open': 10.2, 'high': 10.7, 'low': 10.1, 'close': 10.6, 'volume': 1200, 'amount': 6200},
        ])

    def get_fina_indicator(self, code):
        return pd.DataFrame([
            {'ann_date': '20240430', 'end_date': '20240331', 'eps': 1, 'roe': 2, 'roa': 3, 'gross_margin': 4, 'net_margin': 5, 'debt_to_assets': 6, 'current_ratio': 7, 'quick_ratio': 8}
        ])

    def get_income(self, code):
        return pd.DataFrame([
            {'ann_date': '20240430', 'end_date': '20240331', 'revenue': 100000000, 'operate_profit': 20000000, 'net_profit': 15000000}
        ])

    def get_balancesheet(self, code):
        return pd.DataFrame([
            {'ann_date': '20240430', 'end_date': '20240331', 'total_assets': 100, 'total_liab': 60, 'total_equity': 40, 'current_assets': 50, 'current_liab': 20, 'cash': 10, 'accounts_payable': 5, 'advance_receipts': 2}
        ])

    def get_cashflow(self, code):
        return pd.DataFrame([
            {'ann_date': '20240430', 'end_date': '20240331', 'operate_cash_flow': 9, 'invest_cash_flow': -2, 'finance_cash_flow': 1}
        ])

    def detect_capabilities(self, sample_code='000001'):
        return [
            {'api_name': 'daily', 'display_name': '日线行情', 'sample_code': sample_code, 'ts_code': '000001.SZ', 'available': True, 'empty': False, 'rows': 2, 'error': ''}
        ]


class FakeService(SqlToolService):
    def init_source(self):
        self.source = FakeSource()
        return True


def test_service_import_update_and_capabilities(tmp_path):
    config_path = tmp_path / 'settings.json'
    config_path.write_text(
        '{"tushare": {"token": "fake", "sample_stock": "000001"}, "database": {"path": "%s"}}' % str(tmp_path / 'service.db').replace('\\', '/'),
        encoding='utf-8',
    )

    service = FakeService(config_path=str(config_path))
    import_result = service.import_data(limit=1)
    assert import_result['success'] == 1
    assert import_result['daily_rows'] == 2

    update_result = service.update_data()
    assert update_result['skipped'] == 1

    capabilities = service.detect_capabilities()
    assert capabilities['available_count'] == 1
    assert capabilities['results'][0]['api_name'] == 'daily'

    financials = service.get_stock_financials('000001')
    assert financials['stock']['code'] == '000001'
    assert len(financials['financials']['income']) == 1
