from pathlib import Path

from sql_tool.config import ConfigManager
from sql_tool.db.event import EventDatabase
from sql_tool.gui.desktop import _check_api_health, _is_port_open
from sql_tool.sources.tushare import TushareSource


def test_config_manager_merges_default_api_settings(tmp_path):
    config_path = tmp_path / 'settings.json'
    config_path.write_text(
        '{"tushare": {"token": "fake"}, "database": {"path": "data/test.db"}}',
        encoding='utf-8',
    )

    config = ConfigManager(str(config_path))

    assert config.get_api_host() == '127.0.0.1'
    assert config.get_api_port() == 8000
    # sample_stock and sample_etf still readable for legacy compat
    assert config.get_sample_stock() == '002594'
    assert config.get_sample_etf() == '510300'
    assert config.get_stock_db_path() == 'data/test.db'


def test_gui_api_helpers_detect_closed_local_port():
    assert _is_port_open('127.0.0.1', 65530) is False
    healthy, _ = _check_api_health('127.0.0.1', 65530, timeout=0.1)
    assert healthy is False


def test_capability_map_builds_available_flags():
    results = [
        {'api_name': 'daily', 'available': True},
        {'api_name': 'income', 'available': False},
    ]

    assert TushareSource.capability_map(results) == {'daily': True, 'income': False}


def test_event_database_seed_and_stats(tmp_path):
    db = EventDatabase(str(tmp_path / 'events.db'))
    seeded = db.seed_initial_data()
    stats = db.get_stats()
    holidays = db.get_holidays()
    events = db.get_events()

    assert seeded['holidays'] >= 2
    assert seeded['events'] >= 2
    assert stats['holiday_count'] >= 2
    assert stats['event_count'] >= 2
    assert len(holidays) >= 2
    assert len(events) >= 2



def test_event_mapping_replace_preserves_existing_when_empty(tmp_path):
    db = EventDatabase(str(tmp_path / 'events.db'))
    holiday_id = db.upsert_holiday('2024-10-01', '国庆假期')
    inserted = db.replace_holiday_mappings(holiday_id, [
        {'impact_level': 'direct', 'target_type': 'concept', 'target_name': '旅游酒店'}
    ])
    assert inserted == 1

    preserved = db.replace_holiday_mappings(holiday_id, [])
    assert preserved == 1
    assert len(db.get_holiday_mappings(holiday_id)) == 1

    event_id = db.upsert_event('2024-10-01', '国庆消费旺季', 'holiday')
    event_inserted = db.replace_event_mappings(event_id, [
        {'impact_level': 'direct', 'target_type': 'concept', 'target_name': '商贸零售'}
    ])
    assert event_inserted == 1

    event_preserved = db.replace_event_mappings(event_id, [])
    assert event_preserved == 1
    assert len(db.get_event_mappings(event_id)) == 1


def test_docs_framework_files_exist():
    root = Path(__file__).resolve().parents[1]
    assert (root / 'docs' / 'INDEX_CONSTITUENT_CHANGES.md').exists()
    assert (root / 'docs' / 'BROKER_INDEX_FORECASTS.md').exists()
    assert (root / 'assets' / 'app_icon.png').exists()
    assert (root / 'assets' / 'app_icon.ico').exists()


def test_service_resolve_stock_sample_no_config(tmp_path):
    """detect_capabilities fallback: DB empty + no config sample → hardcoded default"""
    from sql_tool.services.core import SqlToolService
    config_path = tmp_path / 'settings.json'
    config_path.write_text('{"tushare": {"token": "fake"}}', encoding='utf-8')
    svc = SqlToolService(config_path=str(config_path))
    # No data in DB, no sample_stock in config → still resolves a non-empty string
    resolved = svc._resolve_stock_sample_code(None)
    assert isinstance(resolved, str) and resolved


def test_service_resolve_etf_sample_no_config(tmp_path):
    from sql_tool.services.core import SqlToolService
    config_path = tmp_path / 'settings.json'
    config_path.write_text('{"tushare": {"token": "fake"}}', encoding='utf-8')
    svc = SqlToolService(config_path=str(config_path))
    resolved = svc._resolve_etf_sample_code(None)
    assert isinstance(resolved, str) and resolved


def test_service_resolve_uses_explicit_code(tmp_path):
    from sql_tool.services.core import SqlToolService
    config_path = tmp_path / 'settings.json'
    config_path.write_text('{"tushare": {"token": "fake"}}', encoding='utf-8')
    svc = SqlToolService(config_path=str(config_path))
    assert svc._resolve_stock_sample_code('600519') == '600519'
    assert svc._resolve_etf_sample_code('510300') == '510300'


def test_service_database_table_options(tmp_path):
    from sql_tool.services.core import SqlToolService
    config_path = tmp_path / 'settings.json'
    import json
    config_path.write_text(json.dumps({
        'tushare': {'token': 'fake'},
        'database': {
            'stock_path': str(tmp_path / 'stock.db'),
            'etf_path': str(tmp_path / 'etf.db'),
            'feature_path': str(tmp_path / 'feature.db'),
            'event_path': str(tmp_path / 'event.db'),
            'index_constituent_path': str(tmp_path / 'index.db'),
            'index_forecast_path': str(tmp_path / 'forecast.db'),
        }
    }), encoding='utf-8')
    svc = SqlToolService(config_path=str(config_path))
    opts = svc.get_database_table_options()
    assert 'stocks' in opts
    assert 'etfs' in opts
    assert 'features' in opts
    assert 'events' in opts
    assert 'indexes' in opts
    assert 'index_forecasts' in opts
    # each domain should expose at least one table
    for domain, tables in opts.items():
        assert isinstance(tables, list) and len(tables) > 0, f'{domain} has no tables'


def test_service_database_table_rows(tmp_path):
    import json
    import pandas as pd
    from sql_tool.services.core import SqlToolService
    from sql_tool.db.stock import StockDatabase

    db_path = str(tmp_path / 'stock.db')
    db = StockDatabase(db_path)
    db.insert_stock(code='000001', name='平安银行', industry='银行', area='深圳')

    config_path = tmp_path / 'settings.json'
    config_path.write_text(json.dumps({
        'tushare': {'token': 'fake'},
        'database': {'stock_path': db_path}
    }), encoding='utf-8')
    svc = SqlToolService(config_path=str(config_path))
    result = svc.get_database_table_rows('stocks', 'stocks', limit=10)
    assert result['domain'] == 'stocks'
    assert result['table'] == 'stocks'
    assert len(result['columns']) > 0
    assert result['row_count'] == 1
    assert result['rows'][0]['code'] == '000001'
