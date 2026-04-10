import pandas as pd
import pytest

from sql_tool.db.stock import StockDatabase


@pytest.fixture
def db(tmp_path):
    return StockDatabase(str(tmp_path / 'test.db'))


def _insert_stock(db, code='000001', name='平安银行', industry='银行', area='深圳'):
    db.insert_stock(code=code, name=name, industry=industry, area=area)


def _insert_daily(db, code, rows):
    df = pd.DataFrame(rows)
    db.insert_daily_prices(code, df)


def test_basic_stats(db):
    _insert_stock(db)
    _insert_daily(db, '000001', [
        {'date': '2024-04-01', 'open': 10.0, 'high': 10.5, 'low': 9.8, 'close': 10.2, 'volume': 1000, 'amount': 5000},
        {'date': '2024-04-02', 'open': 10.2, 'high': 10.7, 'low': 10.1, 'close': 10.6, 'volume': 1200, 'amount': 6200},
    ])
    db.set_sync_status('000001', 'income', 'unavailable', last_error='接口不可用')
    db.set_sync_status('000001', 'income', 'success', success=True)

    stats = db.get_stats()
    assert stats['stock_count'] == 1
    assert stats['price_count'] == 2
    assert stats['date_range']['start'] == '2024-04-01'
    assert stats['date_range']['end'] == '2024-04-02'
    assert stats['table_counts']['sync_status'] == 1

    assert len(db.get_stock_list(search='平安')) == 1
    prices = db.get_daily_prices('000001', limit=1)
    assert len(prices) == 1
    assert prices[0]['date'] == '2024-04-02'
    assert db.get_stock_overview('000001')['name'] == '平安银行'
    assert db.get_sync_status('000001', 'income')['status'] == 'success'


def test_stock_list_page(db):
    for i in range(15):
        code = f'{i + 1:06d}'
        ind = '银行' if i % 2 == 0 else '证券'
        db.insert_stock(code=code, name=f'股票{i}', industry=ind, area='深圳')

    result = db.get_stock_list_page(page=1, page_size=5)
    assert result['pagination']['total'] == 15
    assert result['pagination']['pages'] == 3
    assert len(result['items']) == 5

    result_p2 = db.get_stock_list_page(page=2, page_size=5)
    assert len(result_p2['items']) == 5
    assert result_p2['items'][0]['code'] not in [r['code'] for r in result['items']]

    result_filter = db.get_stock_list_page(page=1, page_size=20, industry='银行')
    assert all(r['industry'] == '银行' for r in result_filter['items'])


def test_daily_prices_batch(db):
    for code in ['000001', '002594']:
        _insert_stock(db, code=code, name=f'股票{code}')
        _insert_daily(db, code, [
            {'date': f'2024-040{i}', 'open': 10.0, 'high': 10.5, 'low': 9.8, 'close': 10.2, 'volume': 1000, 'amount': 5000}
            for i in range(1, 4)
        ])

    rows = db.get_daily_prices_batch(['000001', '002594'])
    assert len(rows) == 6
    codes = {r['code'] for r in rows}
    assert codes == {'000001', '002594'}

    rows_limited = db.get_daily_prices_batch(['000001', '002594'], limit_per_code=2)
    assert len(rows_limited) == 4
    # 每只最多 2 条
    from collections import Counter
    counts = Counter(r['code'] for r in rows_limited)
    assert counts['000001'] == 2
    assert counts['002594'] == 2

    rows_empty = db.get_daily_prices_batch([])
    assert rows_empty == []


def test_stock_overviews(db):
    _insert_stock(db, code='000001', name='平安银行', industry='银行')
    _insert_stock(db, code='002594', name='比亚迪', industry='汽车')
    _insert_daily(db, '000001', [
        {'date': '2024-04-01', 'open': 10.0, 'high': 10.5, 'low': 9.8, 'close': 10.2, 'volume': 1000, 'amount': 5000},
    ])
    _insert_daily(db, '002594', [
        {'date': '2024-04-01', 'open': 200.0, 'high': 210.0, 'low': 198.0, 'close': 205.0, 'volume': 500, 'amount': 100000},
    ])

    overviews = db.get_stock_overviews(['000001', '002594'])
    assert len(overviews) == 2
    by_code = {r['code']: r for r in overviews}
    assert by_code['000001']['latest_close'] == 10.2
    assert by_code['002594']['latest_close'] == 205.0
    assert by_code['000001']['name'] == '平安银行'

    overviews_empty = db.get_stock_overviews([])
    assert overviews_empty == []
