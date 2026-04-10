import pandas as pd

from sql_tool.services.core import SqlToolService
from sql_tool.sources.etf import EtfSource


class FakeSource:
    def __init__(self):
        self.capability_mode = 'daily_only'

    def detect_capabilities(self, sample_code='000001'):
        available = {'stock_basic', 'daily'}
        if self.capability_mode == 'full':
            available |= {'daily_basic', 'adj_factor', 'fina_indicator', 'income', 'balancesheet', 'cashflow'}
        return [
            {
                'api_name': api_name, 'display_name': api_name,
                'sample_code': sample_code, 'ts_code': '000001.SZ',
                'available': api_name in available,
                'empty': False,
                'rows': 1 if api_name in available else 0,
                'error': '' if api_name in available else 'forbidden',
            }
            for api_name in ['stock_basic', 'daily', 'daily_basic', 'adj_factor',
                             'fina_indicator', 'income', 'balancesheet', 'cashflow']
        ]

    def capability_map(self, results):
        return {item['api_name']: item['available'] for item in results}

    def get_stock_list(self):
        return [
            {'code': '000001', 'name': '平安银行', 'industry': '银行', 'area': '深圳', 'list_date': '19910403'},
            {'code': '002594', 'name': '比亚迪', 'industry': '汽车', 'area': '深圳', 'list_date': '20110630'},
        ]

    def get_stock_info(self, code, capabilities=None):
        capabilities = capabilities or {}
        return {
            'name': '平安银行' if code == '000001' else '比亚迪',
            'area': '深圳', 'industry': '银行' if code == '000001' else '汽车',
            'list_date': '19910403',
            'market_cap': 1 if capabilities.get('daily_basic', False) else 0,
            'circ_mv': 1 if capabilities.get('daily_basic', False) else 0,
            'pe_ratio': 1 if capabilities.get('daily_basic', False) else 0,
            'pb_ratio': 1 if capabilities.get('daily_basic', False) else 0,
            'turnover_rate': 1 if capabilities.get('daily_basic', False) else 0,
            'volume_ratio': 1 if capabilities.get('daily_basic', False) else 0,
            'adj_factor': 1 if capabilities.get('adj_factor', False) else 0,
        }

    def get_daily_prices(self, code):
        return pd.DataFrame([
            {'date': '2024-04-01', 'open': 10.0, 'high': 10.5, 'low': 9.8, 'close': 10.2, 'volume': 1000, 'amount': 5000},
            {'date': '2024-04-02', 'open': 10.2, 'high': 10.7, 'low': 10.1, 'close': 10.6, 'volume': 1200, 'amount': 6200},
        ])

    def get_fina_indicator(self, code):
        if self.capability_mode != 'full':
            return pd.DataFrame()
        return pd.DataFrame([
            {'ann_date': '20240430', 'end_date': '20240331', 'eps': 1, 'roe': 2, 'roa': 3,
             'gross_margin': 4, 'net_margin': 5, 'debt_to_assets': 6, 'current_ratio': 7, 'quick_ratio': 8}
        ])

    def get_income(self, code):
        if self.capability_mode != 'full':
            return pd.DataFrame()
        return pd.DataFrame([
            {'ann_date': '20240430', 'end_date': '20240331', 'revenue': 100000000, 'operate_profit': 20000000, 'net_profit': 15000000}
        ])

    def get_balancesheet(self, code):
        if self.capability_mode != 'full':
            return pd.DataFrame()
        return pd.DataFrame([
            {'ann_date': '20240430', 'end_date': '20240331', 'total_assets': 100, 'total_liab': 60,
             'total_equity': 40, 'current_assets': 50, 'current_liab': 20, 'cash': 10,
             'accounts_payable': 5, 'advance_receipts': 2}
        ])

    def get_cashflow(self, code):
        if self.capability_mode != 'full':
            return pd.DataFrame()
        return pd.DataFrame([
            {'ann_date': '20240430', 'end_date': '20240331', 'operate_cash_flow': 9, 'invest_cash_flow': -2, 'finance_cash_flow': 1}
        ])


class FakeEtfSource:
    def __init__(self):
        self.items = [
            {'code': '159001', 'ts_code': '159001.SZ', 'name': '深市ETF', 'market': 'SZ', 'fund_type': 'ETF'},
            {'code': '510300', 'ts_code': '510300.SH', 'name': '沪深300ETF', 'market': 'SH', 'fund_type': 'ETF'},
        ]
        self.calls = []
        self.daily_map = {
            '159001.SZ': pd.DataFrame([
                {'date': '2024-04-03', 'open': 1.0, 'high': 1.1, 'low': 0.9, 'close': 1.05, 'pre_close': 1.0, 'change': 0.05, 'pct_chg': 5.0, 'volume': 1000, 'amount': 10000},
                {'date': '2024-04-08', 'open': 1.05, 'high': 1.2, 'low': 1.0, 'close': 1.1, 'pre_close': 1.05, 'change': 0.05, 'pct_chg': 4.76, 'volume': 1200, 'amount': 12000},
            ]),
            '510300.SH': pd.DataFrame([
                {'date': '2024-04-08', 'open': 3.0, 'high': 3.1, 'low': 2.9, 'close': 3.05, 'pre_close': 3.0, 'change': 0.05, 'pct_chg': 1.67, 'volume': 2000, 'amount': 20000},
            ]),
        }

    def detect_capabilities(self, sample_code='510300'):
        ts_code = EtfSource.normalize_ts_code(sample_code)
        return [
            {'api_name': 'fund_basic', 'display_name': 'ETF列表', 'sample_code': sample_code, 'ts_code': ts_code, 'available': True, 'empty': False, 'rows': 1, 'error': ''},
            {'api_name': 'fund_daily', 'display_name': 'ETF日线', 'sample_code': sample_code, 'ts_code': ts_code, 'available': True, 'empty': False, 'rows': 1, 'error': ''},
        ]

    def capability_map(self, results):
        return {item['api_name']: item['available'] for item in results}

    def get_etf_list(self):
        return list(self.items)

    def get_etf_daily_prices(self, code, start_date=None, end_date=None, ts_code=None):
        normalized = EtfSource.normalize_ts_code(code, ts_code=ts_code)
        self.calls.append((code, normalized, start_date, end_date))
        df = self.daily_map.get(normalized, pd.DataFrame()).copy()
        if df.empty:
            return df
        if start_date:
            start = pd.to_datetime(start_date).strftime('%Y-%m-%d')
            df = df[df['date'] >= start]
        return df.reset_index(drop=True)


class FakeService(SqlToolService):
    def init_source(self):
        self.source = FakeSource()
        return True


class FakeEtfService(SqlToolService):
    def init_etf_source(self):
        self.etf_source = FakeEtfSource()
        return True


def _make_service(tmp_path):
    config_path = tmp_path / 'settings.json'
    config_path.write_text(
        '{"tushare": {"token": "fake", "sample_stock": "000001"}, "database": {"path": "%s"}}'
        % str(tmp_path / 'service.db').replace('\\', '/'),
        encoding='utf-8',
    )
    return FakeService(config_path=str(config_path))



def _make_etf_service(tmp_path):
    config_path = tmp_path / 'settings.json'
    config_path.write_text(
        (
            '{"tushare": {"token": "fake", "sample_etf": "510300"}, '
            '"database": {"stock_path": "%s", "etf_path": "%s"}}'
        )
        % (
            str(tmp_path / 'stock.db').replace('\\', '/'),
            str(tmp_path / 'etf.db').replace('\\', '/'),
        ),
        encoding='utf-8',
    )
    return FakeEtfService(config_path=str(config_path))


def test_import_daily_only(tmp_path):
    service = _make_service(tmp_path)
    result = service.import_data(limit=1)
    assert result['success'] == 1
    assert result['daily_rows'] == 2
    assert result['financial_rows'] == 0
    assert result['available_datasets'] == ['daily']
    assert service.db.get_sync_status('000001', 'income')['status'] == 'unavailable'


def test_update_backfill_financial(tmp_path):
    service = _make_service(tmp_path)
    service.import_data(limit=1)

    service.source.capability_mode = 'full'
    update_result = service.update_data()
    assert update_result['backfilled'] == 4
    assert update_result['financial_rows'] == 4


def test_detect_capabilities(tmp_path):
    service = _make_service(tmp_path)
    service.init_source()  # 确保 source 已初始化
    service.source.capability_mode = 'full'
    caps = service.detect_capabilities()
    assert caps['available_count'] == 8
    assert 'income' in caps['available_datasets']


def test_get_all_stats_includes_events(tmp_path):
    service = _make_service(tmp_path)
    stats = service.get_all_stats()
    assert 'stocks' in stats
    assert 'etfs' in stats
    assert 'features' in stats
    assert 'events' in stats


def test_get_stock_financials(tmp_path):
    service = _make_service(tmp_path)
    service.init_source()
    service.source.capability_mode = 'full'
    service.import_data(limit=1)
    financials = service.get_stock_financials('000001')
    assert financials['stock']['code'] == '000001'
    assert len(financials['financials']['income']) == 1


def test_update_single_stock_new(tmp_path):
    """单股更新：库中不存在时自动补录"""
    service = _make_service(tmp_path)
    result = service.update_data(code='000001')
    assert result['status'] == 'success'
    assert result['daily_rows'] == 2
    overview = service.db.get_stock_overview('000001')
    assert overview is not None


def test_update_single_stock_existing(tmp_path):
    """单股更新：库中已存在且已最新时跳过"""
    service = _make_service(tmp_path)
    service.import_data(limit=1)
    result = service.update_data(code='000001')
    assert result['status'] == 'skipped'
    assert result['daily_rows'] == 0


def test_get_stock_page(tmp_path):
    service = _make_service(tmp_path)
    service.import_data(limit=2)  # 导入全部 2 只股票

    page = service.get_stock_page(page=1, page_size=10)
    assert page['pagination']['total'] == 2
    assert len(page['items']) == 2


def test_get_all_stock_codes_returns_full_list(tmp_path):
    service = _make_service(tmp_path)
    service.import_data(limit=2)

    assert service.get_all_stock_codes() == ['000001', '002594']


def test_get_daily_batch(tmp_path):
    service = _make_service(tmp_path)
    service.import_data(limit=2)

    result = service.get_daily_batch(['000001', '002594'])
    assert result['meta']['codes'] == 2
    assert result['meta']['rows'] == 4  # 每只 2 条
    codes_in_result = {r['code'] for r in result['items']}
    assert codes_in_result == {'000001', '002594'}


def test_get_stock_overviews(tmp_path):
    service = _make_service(tmp_path)
    service.import_data(limit=2)

    overviews = service.get_stock_overviews(['000001', '002594'])
    assert len(overviews) == 2
    by_code = {r['code']: r for r in overviews}
    assert by_code['000001']['name'] == '平安银行'
    assert by_code['000001']['latest_close'] == 10.6  # 最新一条


def test_cancel_flag(tmp_path):
    """cancel() 设置标志，_reset_cancel() 重置，两者语义符合预期"""
    service = _make_service(tmp_path)
    service.init_source()
    assert service._cancelled is False
    service.cancel()
    assert service._cancelled is True
    service._reset_cancel()
    assert service._cancelled is False


def test_analyze_all_includes_domains(tmp_path):
    service = _make_service(tmp_path)
    result = service.analyze_all()
    assert 'stocks' in result
    assert 'etfs' in result
    assert 'features' in result
    assert 'events' in result
    assert 'indexes' in result
    assert 'index_forecasts' in result


def test_etf_update_adds_missing_universe_and_refreshes_latest(tmp_path):
    service = _make_etf_service(tmp_path)
    service.etf_db.insert_etf(code='510300', ts_code='510300.SH', name='沪深300ETF', market='SH', fund_type='ETF')
    service.etf_db.insert_daily_prices('510300', pd.DataFrame([
        {'date': '2024-04-03', 'open': 2.9, 'high': 3.0, 'low': 2.8, 'close': 3.0, 'pre_close': 2.9, 'change': 0.1, 'pct_chg': 3.45, 'volume': 1500, 'amount': 15000}
    ]))

    result = service.update_etf_data()

    assert result['new_etfs'] == 1
    assert result['success'] == 2
    assert result['daily_rows'] == 3
    assert service.etf_db.get_etf_overview('159001') is not None
    assert service.etf_db.get_latest_date('510300') == '2024-04-08'
    assert service.etf_db.get_latest_date('159001') == '2024-04-08'
    assert ('159001', '159001.SZ', None, None) in service.etf_source.calls
    assert ('510300', '510300.SH', '20240403', None) in service.etf_source.calls


def test_etf_import_uses_item_ts_code_for_sz_etf(tmp_path):
    service = _make_etf_service(tmp_path)

    result = service.import_etf_data(limit=1, skip_existing=False)

    assert result['success'] == 1
    assert result['failed'] == 0
    assert service.etf_db.get_latest_date('159001') == '2024-04-08'
    assert service.etf_source.calls[0][1] == '159001.SZ'


def test_get_all_etf_codes_returns_full_list(tmp_path):
    service = _make_etf_service(tmp_path)
    service.import_etf_data(limit=None, skip_existing=False)

    assert service.get_all_etf_codes() == ['159001', '510300']
