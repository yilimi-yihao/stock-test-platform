import json
from pathlib import Path

import pytest

from sql_tool.db.index_forecast import IndexForecastDatabase
from sql_tool.services.core import SqlToolService
from sql_tool.tools.import_broker_forecasts import parse_broker_forecast_csv, import_broker_forecast_file


@pytest.fixture
def forecast_db(tmp_path):
    return IndexForecastDatabase(str(tmp_path / 'index_forecasts.db'))


@pytest.fixture
def forecast_service(tmp_path):
    config_path = tmp_path / 'settings.json'
    stock_path = tmp_path / 'stock.db'
    forecast_path = tmp_path / 'index_forecasts.db'
    config_path.write_text(json.dumps({
        'tushare': {'token': 'fake'},
        'database': {
            'stock_path': str(stock_path),
            'index_forecast_path': str(forecast_path),
        }
    }), encoding='utf-8')
    service = SqlToolService(config_path=str(config_path))
    service.db.insert_stock(code='600036', name='招商银行', industry='银行', area='深圳')
    service.db.insert_stock(code='601138', name='工业富联', industry='元器件', area='深圳')
    return service


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


def test_parse_broker_forecast_csv(tmp_path):
    csv_path = tmp_path / 'broker.csv'
    csv_path.write_text(
        '调入\n研报日期,预测调整月,证券名称\n2025-11-07,202512,招商银行\n\n调出\n研报日期,预测调整月,证券名称\n2025-11-07,202512,工业富联\n',
        encoding='utf-8'
    )
    broker_name, index_name, rows, warnings = parse_broker_forecast_csv(csv_path)
    assert broker_name == tmp_path.name
    assert index_name == 'broker'
    assert len(rows) == 2
    assert rows[0].forecast_direction == '预测调入'
    assert rows[1].forecast_direction == '预测调出'
    assert warnings == []


def test_import_broker_forecast_file(forecast_service, tmp_path):
    broker_dir = tmp_path / '华泰证券'
    broker_dir.mkdir()
    csv_path = broker_dir / '沪深300.csv'
    csv_path.write_text(
        '调入\n研报日期,预测调整月,证券名称\n2025-11-07,202512,招商银行\n\n调出\n研报日期,预测调整月,证券名称\n2025-11-07,202512,工业富联\n',
        encoding='utf-8'
    )

    summary = import_broker_forecast_file(forecast_service, csv_path)

    assert summary.imported_rows == 2
    assert summary.skipped_rows == 0
    rows = forecast_service.get_index_forecasts()
    assert len(rows) == 2
    assert rows[0]['broker_name'] == '华泰证券'
