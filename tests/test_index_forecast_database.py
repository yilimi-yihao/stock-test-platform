import pytest

from sql_tool.db.index_forecast import IndexForecastDatabase


@pytest.fixture
def forecast_db(tmp_path):
    return IndexForecastDatabase(str(tmp_path / 'index_forecasts.db'))


def test_forecast_upsert_and_list(forecast_db):
    forecast_db.upsert_forecast('沪深300', '202604', '预测调入', '300750', '宁德时代', '中信证券', '样例')
    forecast_db.upsert_forecast('沪深300', '202604', '预测调出', '000725', '京东方A', '中信证券', '样例')
    rows = forecast_db.get_forecasts()
    assert len(rows) == 2
    assert rows[0]['broker_name'] == '中信证券'


def test_forecast_export_csv(forecast_db, tmp_path):
    forecast_db.upsert_forecast('中证500', '202605', '预测调入', '600036', '招商银行', '华泰证券', '')
    path = forecast_db.export_forecasts_csv(str(tmp_path / 'forecast_export.csv'))
    assert path.endswith('forecast_export.csv')
