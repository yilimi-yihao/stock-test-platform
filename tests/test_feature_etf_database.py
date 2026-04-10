import pandas as pd
import pytest

from sql_tool.db.etf import EtfDatabase
from sql_tool.db.feature import FeatureDatabase


@pytest.fixture
def etf_db(tmp_path):
    return EtfDatabase(str(tmp_path / 'etf.db'))


@pytest.fixture
def feature_db(tmp_path):
    return FeatureDatabase(str(tmp_path / 'features.db'))


def _insert_etf(db, code='510300', ts_code='510300.SH', name='沪深300ETF'):
    db.insert_etf(code=code, ts_code=ts_code, name=name, market='SH', fund_type='ETF')


def _insert_etf_daily(db, code, rows):
    df = pd.DataFrame(rows)
    return db.insert_daily_prices(code, df)


class TestEtfDatabase:
    def test_init_tables_exist(self, etf_db):
        counts = etf_db.get_table_counts()
        assert 'etfs' in counts
        assert 'etf_daily_prices' in counts
        assert 'etf_sync_status' in counts

    def test_insert_etf_and_stats(self, etf_db):
        _insert_etf(etf_db)
        stats = etf_db.get_stats()
        assert stats['etf_count'] == 1

    def test_insert_daily_prices_and_latest(self, etf_db):
        _insert_etf(etf_db)
        rows = [
            {'date': '2024-04-01', 'open': 4.0, 'high': 4.1, 'low': 3.9, 'close': 4.05, 'pre_close': 4.0, 'change': 0.05, 'pct_chg': 1.25, 'volume': 10000, 'amount': 40500},
            {'date': '2024-04-02', 'open': 4.05, 'high': 4.15, 'low': 4.0, 'close': 4.10, 'pre_close': 4.05, 'change': 0.05, 'pct_chg': 1.23, 'volume': 12000, 'amount': 49200},
        ]
        inserted = _insert_etf_daily(etf_db, '510300', rows)
        assert inserted == 2
        assert etf_db.get_latest_date('510300') == '2024-04-02'
        prices = etf_db.get_daily_prices('510300', limit=1)
        assert len(prices) == 1
        assert prices[0]['date'] == '2024-04-02'

    def test_upsert_daily_prices(self, etf_db):
        _insert_etf(etf_db)
        rows = [{'date': '2024-04-01', 'open': 4.0, 'high': 4.1, 'low': 3.9, 'close': 4.05, 'pre_close': None, 'change': None, 'pct_chg': None, 'volume': None, 'amount': None}]
        _insert_etf_daily(etf_db, '510300', rows)
        rows2 = [{'date': '2024-04-01', 'open': 4.1, 'high': 4.2, 'low': 4.0, 'close': 4.15, 'pre_close': None, 'change': None, 'pct_chg': None, 'volume': None, 'amount': None}]
        _insert_etf_daily(etf_db, '510300', rows2)
        prices = etf_db.get_daily_prices('510300')
        assert len(prices) == 1
        assert prices[0]['close'] == 4.15

    def test_sync_status(self, etf_db):
        _insert_etf(etf_db)
        etf_db.set_sync_status('510300', 'fund_daily', 'success', success=True)
        status = etf_db.get_sync_status('510300', 'fund_daily')
        assert status['status'] == 'success'
        assert status['last_success_at'] is not None

    def test_list_page(self, etf_db):
        for i in range(5):
            etf_db.insert_etf(code=f'51030{i}', ts_code=f'51030{i}.SH', name=f'ETF{i}', market='SH', fund_type='ETF')
        result = etf_db.get_etf_list_page(page=1, page_size=3)
        assert result['pagination']['total'] == 5
        assert result['pagination']['pages'] == 2
        assert len(result['items']) == 3

    def test_etf_overview(self, etf_db):
        _insert_etf(etf_db)
        ov = etf_db.get_etf_overview('510300')
        assert ov['name'] == '沪深300ETF'


class TestFeatureDatabase:
    def test_init_tables_exist(self, feature_db):
        counts = feature_db.get_table_counts()
        for table in ['concepts', 'stock_concepts', 'stock_moneyflow_daily', 'top_list_events', 'top_list_traders']:
            assert table in counts

    def test_insert_and_get_concepts(self, feature_db):
        items = [
            {'concept_id': 'TS1', 'concept_name': '新能源车', 'source': 'tushare', 'category': 'concept'},
            {'concept_id': 'TS2', 'concept_name': '半导体', 'source': 'tushare', 'category': 'concept'},
        ]
        count = feature_db.insert_concepts(items)
        assert count == 2
        concepts = feature_db.get_concepts()
        assert len(concepts) == 2
        assert concepts[0]['concept_id'] in ['TS1', 'TS2']

    def test_replace_stock_concepts(self, feature_db):
        feature_db.insert_concepts([{'concept_id': 'TS1', 'concept_name': '新能源车', 'source': 'tushare', 'category': 'concept'}])
        members1 = [{'code': '000001'}, {'code': '000002'}]
        feature_db.replace_stock_concepts('TS1', members1)
        assert len(feature_db.get_concept_members('TS1')) == 2

        members2 = [{'code': '000001'}]
        feature_db.replace_stock_concepts('TS1', members2)
        assert len(feature_db.get_concept_members('TS1')) == 1

    def test_insert_moneyflow_upsert(self, feature_db):
        rows = [{'code': '000001', 'trade_date': '20240401', 'net_mf_amount': 100.0}]
        feature_db.insert_moneyflow(rows)
        rows2 = [{'code': '000001', 'trade_date': '20240401', 'net_mf_amount': 200.0}]
        feature_db.insert_moneyflow(rows2)
        mf = feature_db.get_moneyflow('000001')
        assert len(mf) == 1
        assert mf[0]['net_mf_amount'] == 200.0

    def test_top_list_parent_child_cascade(self, feature_db):
        events = [{'code': '000001', 'trade_date': '20240401', 'reason': '涨停', 'close': 10.5}]
        [event_id] = feature_db.insert_top_list_events(events)
        traders = [{'broker_name': '中信证券', 'direction': 'buy', 'rank_no': 1, 'amount': 1000000}]
        feature_db.replace_top_list_traders(event_id, traders)
        assert len(feature_db.get_top_list_traders(event_id)) == 1
        with feature_db.get_connection() as conn:
            conn.execute('DELETE FROM top_list_events WHERE event_id = ?', (event_id,))
            conn.commit()
        assert feature_db.get_top_list_traders(event_id) == []

    def test_get_top_list_filters(self, feature_db):
        events = [
            {'code': '000001', 'trade_date': '20240401', 'reason': '涨停'},
            {'code': '000002', 'trade_date': '20240401', 'reason': '龙虎榜'},
            {'code': '000001', 'trade_date': '20240402', 'reason': '涨停'},
        ]
        feature_db.insert_top_list_events(events)
        all_events = feature_db.get_top_list()
        assert len(all_events) == 3
        by_code = feature_db.get_top_list(code='000001')
        assert len(by_code) == 2
        by_date = feature_db.get_top_list(trade_date='20240401')
        assert len(by_date) == 2

    def test_sync_status(self, feature_db):
        feature_db.set_sync_status('concepts', 'global', 'all', 'success', row_count=10, success=True)
        status = feature_db.get_sync_status('concepts', 'global', 'all')
        assert status['status'] == 'success'
        assert status['row_count'] == 10

    def test_feature_db_independent_from_core(self, tmp_path):
        """特色库操作不影响核心股票库文件"""
        from sql_tool.db.stock import StockDatabase
        stock_db = StockDatabase(str(tmp_path / 'core.db'))
        stock_db.insert_stock(code='000001', name='平安银行')
        f_db = FeatureDatabase(str(tmp_path / 'features.db'))
        f_db.insert_concepts([{'concept_id': 'X1', 'concept_name': '测试', 'source': 'tushare', 'category': 'concept'}])
        assert stock_db.get_stock_overview('000001')['name'] == '平安银行'
        assert f_db.get_concepts()[0]['concept_id'] == 'X1'
        import os
        assert os.path.exists(tmp_path / 'core.db')
        assert os.path.exists(tmp_path / 'features.db')
