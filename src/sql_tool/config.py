"""配置管理模块"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class ConfigManager:
    """配置管理器"""

    def __init__(self, config_path: str = 'config/settings.json'):
        self.config_path = Path(config_path)
        self.config = self._load_config()

    def _load_config(self) -> dict[str, Any]:
        """加载配置文件"""
        try:
            if self.config_path.exists():
                with open(self.config_path, 'r', encoding='utf-8') as file:
                    config = json.load(file)
                    return self._merge_defaults(config)
        except Exception:
            logger.exception('加载配置失败')
        return self._get_default_config()

    def _get_default_config(self) -> dict[str, Any]:
        """默认配置"""
        return {
            'tushare': {
                'token': '',
                'api_url': 'http://tushare.nlink.vip',
                'enabled': True,
                'sample_stock': '002594',
                'sample_etf': '510300',
                'request_interval_seconds': 0.25,
            },
            'database': {
                'path': 'data/a_share.db',
                'stock_path': 'data/a_share.db',
                'etf_path': 'data/etf.db',
                'feature_path': 'data/a_share_features.db',
                'event_path': 'data/event_calendar.db',
                'index_constituent_path': 'data/index_constituents.db',
                'index_forecast_path': 'data/index_forecasts.db',
            },
            'import': {
                'default_start_date': '20100101',
                'batch_size': 100,
            },
            'api': {
                'host': '127.0.0.1',
                'port': 8000,
                'enabled': True,
            },
        }

    def _merge_defaults(self, config: dict[str, Any]) -> dict[str, Any]:
        defaults = self._get_default_config()
        for section, values in defaults.items():
            existing = config.setdefault(section, {})
            if isinstance(existing, dict):
                for key, value in values.items():
                    if section == 'database' and key == 'stock_path' and existing.get('path'):
                        existing.setdefault(key, existing['path'])
                    else:
                        existing.setdefault(key, value)
        return config

    def get_tushare_token(self) -> str:
        return self.config.get('tushare', {}).get('token', '')

    def set_tushare_token(self, token: str) -> None:
        self.config.setdefault('tushare', {})['token'] = token
        self.save()

    def set_sample_stock(self, code: str) -> None:
        self.config.setdefault('tushare', {})['sample_stock'] = code
        self.save()

    def set_api_host_port(self, host: str, port: int) -> None:
        api = self.config.setdefault('api', {})
        api['host'] = host
        api['port'] = int(port)
        self.save()

    def get_db_path(self) -> str:
        database = self.config.get('database', {})
        return database.get('stock_path') or database.get('path', 'data/a_share.db')

    def get_stock_db_path(self) -> str:
        return self.get_db_path()

    def get_etf_db_path(self) -> str:
        return self.config.get('database', {}).get('etf_path', 'data/etf.db')

    def get_feature_db_path(self) -> str:
        return self.config.get('database', {}).get('feature_path', 'data/a_share_features.db')

    def get_event_db_path(self) -> str:
        return self.config.get('database', {}).get('event_path', 'data/event_calendar.db')

    def get_index_constituent_db_path(self) -> str:
        return self.config.get('database', {}).get('index_constituent_path', 'data/index_constituents.db')

    def get_index_forecast_db_path(self) -> str:
        return self.config.get('database', {}).get('index_forecast_path', 'data/index_forecasts.db')

    def get_tushare_api_url(self) -> str:
        return self.config.get('tushare', {}).get('api_url', 'http://tushare.nlink.vip')

    def get_sample_stock(self) -> str:
        return self.config.get('tushare', {}).get('sample_stock', '002594')

    def get_sample_etf(self) -> str:
        return self.config.get('tushare', {}).get('sample_etf', '510300')

    def get_tushare_request_interval(self) -> float:
        return float(self.config.get('tushare', {}).get('request_interval_seconds', 0.25))

    def get_api_host(self) -> str:
        return self.config.get('api', {}).get('host', '127.0.0.1')

    def get_api_port(self) -> int:
        return int(self.config.get('api', {}).get('port', 8000))

    def save(self) -> None:
        """保存配置"""
        try:
            self.config_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.config_path, 'w', encoding='utf-8') as file:
                json.dump(self.config, file, indent=2, ensure_ascii=False)
            logger.info('配置已保存')
        except Exception:
            logger.exception('保存配置失败')
            raise
