from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any, Optional

from sql_tool.base_database import BaseDatabase

logger = logging.getLogger(__name__)


class EventDatabase(BaseDatabase):
    """节假日、赛事、主题映射等事件数据库"""

    def __init__(self, db_path: str = 'data/event_calendar.db'):
        super().__init__(db_path)

    def _init_db(self) -> None:
        with self.get_connection() as conn:
            conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS holidays (
                    holiday_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    holiday_date TEXT NOT NULL,
                    name TEXT NOT NULL,
                    market_scope TEXT NOT NULL DEFAULT 'CN',
                    is_trading_closed INTEGER NOT NULL DEFAULT 1,
                    notes TEXT,
                    UNIQUE(holiday_date, name)
                )
                '''
            )
            conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS holiday_theme_mappings (
                    mapping_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    holiday_id INTEGER NOT NULL,
                    impact_level TEXT NOT NULL,
                    target_type TEXT NOT NULL,
                    target_name TEXT NOT NULL,
                    code TEXT,
                    notes TEXT,
                    relevance REAL NOT NULL DEFAULT 0.5,
                    FOREIGN KEY(holiday_id) REFERENCES holidays(holiday_id) ON DELETE CASCADE
                )
                '''
            )
            for col_sql in (
                'ALTER TABLE holiday_theme_mappings ADD COLUMN relevance REAL NOT NULL DEFAULT 0.5',
            ):
                try:
                    conn.execute(col_sql)
                except Exception:
                    pass
            conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS major_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_date TEXT NOT NULL,
                    name TEXT NOT NULL,
                    category TEXT NOT NULL,
                    location TEXT,
                    notes TEXT,
                    UNIQUE(event_date, name)
                )
                '''
            )
            conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS event_theme_mappings (
                    mapping_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER NOT NULL,
                    impact_level TEXT NOT NULL,
                    target_type TEXT NOT NULL,
                    target_name TEXT NOT NULL,
                    code TEXT,
                    notes TEXT,
                    relevance REAL NOT NULL DEFAULT 0.5,
                    FOREIGN KEY(event_id) REFERENCES major_events(event_id) ON DELETE CASCADE
                )
                '''
            )
            for col_sql in (
                'ALTER TABLE event_theme_mappings ADD COLUMN relevance REAL NOT NULL DEFAULT 0.5',
            ):
                try:
                    conn.execute(col_sql)
                except Exception:
                    pass
            conn.execute('CREATE INDEX IF NOT EXISTS idx_holidays_date ON holidays(holiday_date DESC)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_holiday_theme ON holiday_theme_mappings(holiday_id, impact_level)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_events_date ON major_events(event_date DESC, category)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_event_theme ON event_theme_mappings(event_id, impact_level)')

    def upsert_holiday(self, holiday_date: str, name: str, market_scope: str = 'CN', is_trading_closed: bool = True, notes: str = '') -> int:
        with self.get_connection() as conn:
            conn.execute(
                '''
                INSERT OR IGNORE INTO holidays (holiday_date, name, market_scope, is_trading_closed, notes)
                VALUES (?, ?, ?, ?, ?)
                ''',
                (holiday_date, name, market_scope, 1 if is_trading_closed else 0, notes),
            )
            row = conn.execute('SELECT holiday_id FROM holidays WHERE holiday_date = ? AND name = ?', (holiday_date, name)).fetchone()
            return int(row[0])

    def replace_holiday_mappings(self, holiday_id: int, mappings: list[dict[str, Any]], allow_empty: bool = False) -> int:
        with self.get_connection() as conn:
            if not mappings and not allow_empty:
                existing = conn.execute(
                    'SELECT COUNT(*) FROM holiday_theme_mappings WHERE holiday_id = ?',
                    (holiday_id,),
                ).fetchone()[0]
                return int(existing)
            conn.execute('DELETE FROM holiday_theme_mappings WHERE holiday_id = ?', (holiday_id,))
            if mappings:
                conn.executemany(
                    '''
                    INSERT INTO holiday_theme_mappings (holiday_id, impact_level, target_type, target_name, code, notes, relevance)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''',
                    [
                        (holiday_id, item['impact_level'], item['target_type'], item['target_name'],
                         item.get('code'), item.get('notes'), float(item.get('relevance', 0.5)))
                        for item in mappings
                    ],
                )
            return len(mappings)

    def upsert_event(self, event_date: str, name: str, category: str, location: str = '', notes: str = '') -> int:
        with self.get_connection() as conn:
            conn.execute(
                '''
                INSERT OR IGNORE INTO major_events (event_date, name, category, location, notes)
                VALUES (?, ?, ?, ?, ?)
                ''',
                (event_date, name, category, location, notes),
            )
            row = conn.execute('SELECT event_id FROM major_events WHERE event_date = ? AND name = ?', (event_date, name)).fetchone()
            return int(row[0])

    def replace_event_mappings(self, event_id: int, mappings: list[dict[str, Any]], allow_empty: bool = False) -> int:
        with self.get_connection() as conn:
            if not mappings and not allow_empty:
                existing = conn.execute(
                    'SELECT COUNT(*) FROM event_theme_mappings WHERE event_id = ?',
                    (event_id,),
                ).fetchone()[0]
                return int(existing)
            conn.execute('DELETE FROM event_theme_mappings WHERE event_id = ?', (event_id,))
            if mappings:
                conn.executemany(
                    '''
                    INSERT INTO event_theme_mappings (event_id, impact_level, target_type, target_name, code, notes, relevance)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''',
                    [
                        (event_id, item['impact_level'], item['target_type'], item['target_name'],
                         item.get('code'), item.get('notes'), float(item.get('relevance', 0.5)))
                        for item in mappings
                    ],
                )
            return len(mappings)
            return len(mappings)

    def get_holidays(self) -> list[dict[str, Any]]:
        with self.get_connection() as conn:
            return self._rows_to_dicts(conn.execute('SELECT * FROM holidays ORDER BY holiday_date DESC').fetchall())

    def get_holiday_mappings(self, holiday_id: int) -> list[dict[str, Any]]:
        with self.get_connection() as conn:
            return self._rows_to_dicts(conn.execute('SELECT * FROM holiday_theme_mappings WHERE holiday_id = ? ORDER BY impact_level, target_type, target_name', (holiday_id,)).fetchall())

    def get_events(self) -> list[dict[str, Any]]:
        with self.get_connection() as conn:
            return self._rows_to_dicts(conn.execute('SELECT * FROM major_events ORDER BY event_date DESC').fetchall())

    def get_event_mappings(self, event_id: int) -> list[dict[str, Any]]:
        with self.get_connection() as conn:
            return self._rows_to_dicts(conn.execute('SELECT * FROM event_theme_mappings WHERE event_id = ? ORDER BY impact_level, target_type, target_name', (event_id,)).fetchall())

    def delete_holiday(self, holiday_id: int) -> bool:
        with self.get_connection() as conn:
            conn.execute('DELETE FROM holiday_theme_mappings WHERE holiday_id = ?', (holiday_id,))
            cur = conn.execute('DELETE FROM holidays WHERE holiday_id = ?', (holiday_id,))
            return cur.rowcount > 0

    def clear_holidays(self) -> int:
        with self.get_connection() as conn:
            conn.execute('DELETE FROM holiday_theme_mappings')
            cur = conn.execute('DELETE FROM holidays')
            return cur.rowcount

    def delete_event(self, event_id: int) -> bool:
        with self.get_connection() as conn:
            conn.execute('DELETE FROM event_theme_mappings WHERE event_id = ?', (event_id,))
            cur = conn.execute('DELETE FROM major_events WHERE event_id = ?', (event_id,))
            return cur.rowcount > 0

    def clear_events(self) -> int:
        with self.get_connection() as conn:
            conn.execute('DELETE FROM event_theme_mappings')
            cur = conn.execute('DELETE FROM major_events')
            return cur.rowcount

    def seed_initial_data(self) -> dict[str, int]:
        holiday_count = 0
        mapping_count = 0
        event_count = 0
        event_mapping_count = 0

        newyear_id = self.upsert_holiday('2026-01-01', '元旦', notes='旅游、消费、传媒跨年主题可能活跃')
        holiday_count += 1
        mapping_count += self.replace_holiday_mappings(newyear_id, [
            {'impact_level': 'direct', 'target_type': 'concept', 'target_name': '旅游酒店'},
            {'impact_level': 'indirect', 'target_type': 'concept', 'target_name': '传媒'},
        ])

        spring_id = self.upsert_holiday('2026-02-17', '春节假期', notes='A股休市，消费、出行、影视、礼品主题通常活跃')
        holiday_count += 1
        mapping_count += self.replace_holiday_mappings(spring_id, [
            {'impact_level': 'direct', 'target_type': 'concept', 'target_name': '旅游酒店', 'notes': '出行消费'},
            {'impact_level': 'direct', 'target_type': 'concept', 'target_name': '食品饮料', 'notes': '春节消费'},
            {'impact_level': 'indirect', 'target_type': 'concept', 'target_name': '影视院线'},
            {'impact_level': 'phonetic', 'target_type': 'stock', 'target_name': '三只松鼠', 'code': '300783'},
        ])

        national_id = self.upsert_holiday('2026-10-01', '国庆假期', notes='出行、景区、免税、酒店主题常被关注')
        holiday_count += 1
        mapping_count += self.replace_holiday_mappings(national_id, [
            {'impact_level': 'direct', 'target_type': 'concept', 'target_name': '旅游酒店'},
            {'impact_level': 'direct', 'target_type': 'concept', 'target_name': '免税'},
            {'impact_level': 'indirect', 'target_type': 'concept', 'target_name': '高铁'},
            {'impact_level': 'indirect', 'target_type': 'stock', 'target_name': '中国中免', 'code': '601888'},
        ])

        qingming_id = self.upsert_holiday('2026-04-05', '清明节', notes='祭扫、出行、鲜花、电商配送等主题可能活跃')
        holiday_count += 1
        mapping_count += self.replace_holiday_mappings(qingming_id, [
            {'impact_level': 'direct', 'target_type': 'concept', 'target_name': '旅游酒店'},
            {'impact_level': 'indirect', 'target_type': 'concept', 'target_name': '物流'},
            {'impact_level': 'indirect', 'target_type': 'stock', 'target_name': '顺丰控股', 'code': '002352'},
        ])

        labor_id = self.upsert_holiday('2026-05-01', '劳动节', notes='旅游、餐饮、交通运输主题通常活跃')
        holiday_count += 1
        mapping_count += self.replace_holiday_mappings(labor_id, [
            {'impact_level': 'direct', 'target_type': 'concept', 'target_name': '旅游酒店'},
            {'impact_level': 'direct', 'target_type': 'concept', 'target_name': '食品饮料'},
            {'impact_level': 'indirect', 'target_type': 'stock', 'target_name': '中国国航', 'code': '601111'},
        ])

        dragonboat_id = self.upsert_holiday('2026-06-19', '端午节', notes='食品、礼品、出行主题可能活跃')
        holiday_count += 1
        mapping_count += self.replace_holiday_mappings(dragonboat_id, [
            {'impact_level': 'direct', 'target_type': 'concept', 'target_name': '食品饮料'},
            {'impact_level': 'indirect', 'target_type': 'concept', 'target_name': '旅游酒店'},
            {'impact_level': 'phonetic', 'target_type': 'stock', 'target_name': '五芳斋', 'code': '603237'},
        ])

        midautumn_id = self.upsert_holiday('2026-09-25', '中秋节', notes='礼品、食品、商超与出行主题可能活跃')
        holiday_count += 1
        mapping_count += self.replace_holiday_mappings(midautumn_id, [
            {'impact_level': 'direct', 'target_type': 'concept', 'target_name': '食品饮料'},
            {'impact_level': 'indirect', 'target_type': 'concept', 'target_name': '商贸零售'},
        ])

        olympic_id = self.upsert_event('2026-02-06', '米兰冬奥会', 'sports', '意大利', '冰雪运动、服饰、转播相关主题可能活跃')
        event_count += 1
        event_mapping_count += self.replace_event_mappings(olympic_id, [
            {'impact_level': 'direct', 'target_type': 'concept', 'target_name': '冰雪产业'},
            {'impact_level': 'indirect', 'target_type': 'concept', 'target_name': '体育产业'},
        ])

        asian_games_id = self.upsert_event('2026-09-19', '名古屋亚运会', 'sports', '日本', '体育消费、赛事转播、装备主题可能活跃')
        event_count += 1
        event_mapping_count += self.replace_event_mappings(asian_games_id, [
            {'impact_level': 'direct', 'target_type': 'concept', 'target_name': '体育产业'},
            {'impact_level': 'indirect', 'target_type': 'concept', 'target_name': '传媒'},
        ])

        worldcup_id = self.upsert_event('2026-06-11', '美加墨世界杯', 'sports', '北美', '啤酒、零食、转播、体育营销相关主题可能活跃')
        event_count += 1
        event_mapping_count += self.replace_event_mappings(worldcup_id, [
            {'impact_level': 'direct', 'target_type': 'concept', 'target_name': '体育产业'},
            {'impact_level': 'indirect', 'target_type': 'concept', 'target_name': '食品饮料'},
            {'impact_level': 'indirect', 'target_type': 'stock', 'target_name': '青岛啤酒', 'code': '600600'},
        ])

        expo_id = self.upsert_event('2026-11-01', '中国国际进口博览会', 'expo', '上海', '会展、跨境贸易、消费主题可能活跃')
        event_count += 1
        event_mapping_count += self.replace_event_mappings(expo_id, [
            {'impact_level': 'direct', 'target_type': 'concept', 'target_name': '会展服务'},
            {'impact_level': 'indirect', 'target_type': 'concept', 'target_name': '跨境电商'},
        ])

        return {
            'holidays': holiday_count,
            'holiday_mappings': mapping_count,
            'events': event_count,
            'event_mappings': event_mapping_count,
        }

    def get_table_counts(self) -> dict[str, int]:
        table_names = ['holidays', 'holiday_theme_mappings', 'major_events', 'event_theme_mappings']
        with self.get_connection() as conn:
            return {table: self._count_table(conn, table) for table in table_names}

    def get_stats(self) -> dict[str, Any]:
        counts = self.get_table_counts()
        db_file = Path(self.db_path)
        return {
            'db_path': str(db_file),
            'db_exists': db_file.exists(),
            'db_size_bytes': db_file.stat().st_size if db_file.exists() else 0,
            'table_counts': counts,
            'holiday_count': counts['holidays'],
            'event_count': counts['major_events'],
        }
