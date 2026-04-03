import pandas as pd

from sql_tool.database import StockDatabase


def test_database_stats_and_queries(tmp_path):
    db_path = tmp_path / 'test.db'
    db = StockDatabase(str(db_path))

    db.insert_stock(code='000001', name='平安银行', industry='银行')
    df = pd.DataFrame(
        [
            {'date': '2024-04-01', 'open': 10.0, 'high': 10.5, 'low': 9.8, 'close': 10.2, 'volume': 1000, 'amount': 5000},
            {'date': '2024-04-02', 'open': 10.2, 'high': 10.7, 'low': 10.1, 'close': 10.6, 'volume': 1200, 'amount': 6200},
        ]
    )
    db.insert_daily_prices('000001', df)

    stats = db.get_stats()
    assert stats['stock_count'] == 1
    assert stats['price_count'] == 2
    assert stats['date_range']['start'] == '2024-04-01'
    assert stats['date_range']['end'] == '2024-04-02'

    stocks = db.get_stock_list(search='平安')
    assert len(stocks) == 1

    prices = db.get_daily_prices('000001', limit=1)
    assert len(prices) == 1
    assert prices[0]['date'] == '2024-04-02'

    overview = db.get_stock_overview('000001')
    assert overview['name'] == '平安银行'
