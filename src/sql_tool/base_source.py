"""Tushare 数据源基类"""
from __future__ import annotations

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


class BaseTushareSource:
    """所有 Tushare 数据源的公共基类，统一处理 token 注入与 API 初始化。"""

    def __init__(self, token: str, api_url: str = 'http://tushare.nlink.vip'):
        if not token or not token.strip():
            raise ValueError('Tushare token 不能为空')
        try:
            import tushare as ts
            self.ts = ts
            try:
                self.pro = ts.pro_api(token)
            except Exception:
                from tushare.pro.data_pro import DataApi
                self.pro = DataApi(token)
            self.pro._DataApi__token = token
            self.pro._DataApi__http_url = api_url
            self.api_url = api_url
            logger.info('%s 初始化成功: url=%s, token=%s...%s',
                        self.__class__.__name__, api_url, token[:8], token[-4:])
        except Exception:
            logger.exception('%s 初始化失败', self.__class__.__name__)
            raise

    @staticmethod
    def capability_map(results: list[dict[str, Any]]) -> dict[str, bool]:
        return {item['api_name']: bool(item.get('available')) for item in results}

    def _empty_df(self) -> pd.DataFrame:
        return pd.DataFrame()
