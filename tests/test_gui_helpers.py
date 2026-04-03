from pathlib import Path

from sql_tool.config import ConfigManager
from sql_tool.gui import _check_api_health, _is_port_open


def test_config_manager_merges_default_api_settings(tmp_path):
    config_path = tmp_path / 'settings.json'
    config_path.write_text(
        '{"tushare": {"token": "fake"}, "database": {"path": "data/test.db"}}',
        encoding='utf-8',
    )

    config = ConfigManager(str(config_path))

    assert config.get_api_host() == '127.0.0.1'
    assert config.get_api_port() == 8000
    assert config.get_sample_stock() == '000001'


def test_gui_api_helpers_detect_closed_local_port():
    assert _is_port_open('127.0.0.1', 65530) is False
    healthy, _ = _check_api_health('127.0.0.1', 65530, timeout=0.1)
    assert healthy is False
