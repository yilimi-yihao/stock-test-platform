from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from sql_tool.base_database import BaseDatabase
from sql_tool.exporters_pkg.table import export_rows_to_csv, export_rows_to_excel, export_rows_to_pdf

logger = logging.getLogger(__name__)


class IndexForecastDatabase(BaseDatabase):
    """指数变化预测数据库"""

    def __init__(self, db_path: str = 'data/index_forecasts.db'):
        super().__init__(db_path)

    def _init_db(self) -> None:
        with self.get_connection() as conn:
            conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS index_forecasts (
                    forecast_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    index_name TEXT NOT NULL,
                    forecast_month TEXT NOT NULL,
                    forecast_direction TEXT NOT NULL,
                    stock_code TEXT NOT NULL,
                    stock_name TEXT NOT NULL,
                    broker_name TEXT NOT NULL,
                    source_note TEXT,
                    UNIQUE(index_name, forecast_month, forecast_direction, stock_code, broker_name)
                )
                '''
            )
            conn.execute('CREATE INDEX IF NOT EXISTS idx_index_forecasts_month ON index_forecasts(index_name, forecast_month, forecast_direction)')

    def upsert_forecast(self, index_name: str, forecast_month: str, forecast_direction: str, stock_code: str, stock_name: str, broker_name: str, source_note: str = '') -> None:
        with self.get_connection() as conn:
            conn.execute(
                '''
                INSERT OR REPLACE INTO index_forecasts
                (index_name, forecast_month, forecast_direction, stock_code, stock_name, broker_name, source_note)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''',
                (index_name, forecast_month, forecast_direction, stock_code, stock_name, broker_name, source_note),
            )

    def get_forecasts(self) -> list[dict[str, Any]]:
        with self.get_connection() as conn:
            rows = conn.execute('SELECT * FROM index_forecasts ORDER BY forecast_month DESC, index_name, broker_name, forecast_direction, stock_code').fetchall()
            return self._rows_to_dicts(rows)

    def export_forecasts_csv(self, output_path: str) -> str:
        rows = self.get_forecasts()
        fieldnames = ['index_name', 'forecast_month', 'forecast_direction', 'stock_code', 'stock_name', 'broker_name', 'source_note']
        return export_rows_to_csv(rows, output_path, fieldnames)

    def export_forecasts_excel(self, output_path: str) -> str:
        rows = self.get_forecasts()
        fieldnames = ['index_name', 'forecast_month', 'forecast_direction', 'stock_code', 'stock_name', 'broker_name', 'source_note']
        return export_rows_to_excel(rows, output_path, fieldnames)

    def export_forecasts_pdf(self, output_path: str) -> str:
        rows = self.get_forecasts()
        fieldnames = ['index_name', 'forecast_month', 'forecast_direction', 'stock_code', 'stock_name', 'broker_name', 'source_note']
        return export_rows_to_pdf(rows, output_path, fieldnames)

    def get_table_counts(self) -> dict[str, int]:
        with self.get_connection() as conn:
            return {'index_forecasts': self._count_table(conn, 'index_forecasts')}

    def get_stats(self) -> dict[str, Any]:
        counts = self.get_table_counts()
        db_file = Path(self.db_path)
        return {
            'db_path': str(db_file),
            'db_exists': db_file.exists(),
            'db_size_bytes': db_file.stat().st_size if db_file.exists() else 0,
            'table_counts': counts,
            'forecast_count': counts['index_forecasts'],
        }
