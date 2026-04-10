import pandas as pd
import pytest

from sql_tool.db.index_constituent import IndexConstituentDatabase


@pytest.fixture
def index_db(tmp_path):
    return IndexConstituentDatabase(str(tmp_path / 'index_constituents.db'))


def test_index_db_init(index_db):
    stats = index_db.get_stats()
    assert 'index_count' in stats
    assert 'snapshot_count' in stats
    assert 'change_count' in stats


def test_index_entity_and_snapshot(index_db):
    index_id = index_db.upsert_index_entity(index_name='沪深300', etf_code='510300', benchmark='沪深300指数')
    snapshot_id = index_db.create_snapshot(index_id, '20260401', note='初始快照')
    rows = index_db.replace_snapshot_items(snapshot_id, [
        {'code': '000001', 'name': '平安银行', 'weight': 1.2},
        {'code': '600036', 'name': '招商银行', 'weight': 1.8},
    ])
    assert rows == 2
    entities = index_db.get_index_entities()
    assert len(entities) == 1
    snapshots = index_db.get_snapshots(index_id)
    assert len(snapshots) == 1
    items = index_db.get_snapshot_items(snapshot_id)
    assert len(items) == 2


def test_replace_changes(index_db):
    index_id = index_db.upsert_index_entity(index_name='中证500', etf_code='510500')
    count = index_db.replace_changes(index_id, '20260402', [
        {'change_type': 'added', 'code': '300750', 'name': '宁德时代'},
        {'change_type': 'removed', 'code': '000725', 'name': '京东方A'},
    ], None, None)
    assert count == 2
    changes = index_db.get_changes(index_id)
    assert len(changes) == 2
