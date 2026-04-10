from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Optional

from sql_tool.services.core import SqlToolService


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_FORECAST_DIR = ROOT / 'resource_data' / 'broker_forecasts'
DEFAULT_CONFIG = ROOT / 'config' / 'settings.json'


@dataclass
class BrokerForecastImportSummary:
    file: str
    broker_name: str
    index_name: str
    parsed_rows: int
    imported_rows: int
    skipped_rows: int
    unmatched_names: list[str]
    warnings: list[str]
    dry_run: bool = False


@dataclass
class BrokerForecastRow:
    broker_name: str
    index_name: str
    forecast_month: str
    forecast_direction: str
    stock_name: str
    report_date: str
    source_file: str


def _open_csv_reader(path: Path) -> Iterable[list[str]]:
    last_error: Exception | None = None
    for encoding in ('utf-8-sig', 'utf-8', 'gbk'):
        try:
            with path.open('r', encoding=encoding, newline='') as f:
                yield from csv.reader(f)
            return
        except UnicodeDecodeError as exc:
            last_error = exc
    if last_error:
        raise last_error


def parse_broker_forecast_csv(path: Path) -> tuple[str, str, list[BrokerForecastRow], list[str]]:
    broker_name = path.parent.name
    index_name = path.stem
    warnings: list[str] = []
    rows: list[BrokerForecastRow] = []
    current_direction = ''
    seen: set[tuple[str, str, str]] = set()

    for row_no, row in enumerate(_open_csv_reader(path), start=1):
        cells = [(cell or '').strip() for cell in row]
        if not any(cells):
            continue
        head = cells[0]
        if head in {'调入', '调出'}:
            current_direction = '预测调入' if head == '调入' else '预测调出'
            continue
        if head == '研报日期':
            continue
        if not current_direction:
            warnings.append(f'{path.name}: row {row_no} 未识别分段，已跳过')
            continue
        if len(cells) < 3:
            warnings.append(f'{path.name}: row {row_no} 列数不足，已跳过')
            continue
        report_date, forecast_month, stock_name = cells[:3]
        if not forecast_month or not stock_name:
            warnings.append(f'{path.name}: row {row_no} 缺少预测月份或证券名称，已跳过')
            continue
        dedupe_key = (current_direction, forecast_month, stock_name)
        if dedupe_key in seen:
            warnings.append(f'{path.name}: row {row_no} 重复记录 {stock_name}，已去重')
            continue
        seen.add(dedupe_key)
        rows.append(
            BrokerForecastRow(
                broker_name=broker_name,
                index_name=index_name,
                forecast_month=forecast_month,
                forecast_direction=current_direction,
                stock_name=stock_name,
                report_date=report_date,
                source_file=path.name,
            )
        )

    return broker_name, index_name, rows, warnings


def resolve_stock_code(service: SqlToolService, stock_name: str) -> tuple[str, str] | None:
    page = service.db.get_stock_list_page(page=1, page_size=20, search=stock_name)
    items = page.get('items', [])
    for item in items:
        if item.get('name') == stock_name:
            return item['code'], item['name']
    return None


def import_broker_forecast_file(
    service: SqlToolService,
    path: Path,
    dry_run: bool = False,
) -> BrokerForecastImportSummary:
    broker_name, index_name, rows, warnings = parse_broker_forecast_csv(path)
    imported_rows = 0
    skipped_rows = 0
    unmatched_names: list[str] = []

    for row in rows:
        matched = resolve_stock_code(service, row.stock_name)
        if not matched:
            skipped_rows += 1
            unmatched_names.append(row.stock_name)
            continue
        if dry_run:
            imported_rows += 1
            continue
        stock_code, stock_name = matched
        source_note = f'{row.broker_name}研报日期:{row.report_date};文件:{row.source_file}'
        service.add_index_forecast(
            index_name=row.index_name,
            forecast_month=row.forecast_month,
            forecast_direction=row.forecast_direction,
            stock_code=stock_code,
            stock_name=stock_name,
            broker_name=row.broker_name,
            source_note=source_note,
        )
        imported_rows += 1

    return BrokerForecastImportSummary(
        file=path.name,
        broker_name=broker_name,
        index_name=index_name,
        parsed_rows=len(rows),
        imported_rows=imported_rows,
        skipped_rows=skipped_rows,
        unmatched_names=sorted(set(unmatched_names)),
        warnings=warnings,
        dry_run=dry_run,
    )


def import_directory(
    service: SqlToolService,
    base_dir: Path = DEFAULT_FORECAST_DIR,
    dry_run: bool = False,
    only_broker: Optional[str] = None,
    only_index: Optional[str] = None,
) -> list[BrokerForecastImportSummary]:
    summaries: list[BrokerForecastImportSummary] = []
    for path in sorted(base_dir.glob('*/*.csv')):
        if only_broker and path.parent.name != only_broker:
            continue
        if only_index and path.stem != only_index:
            continue
        summaries.append(import_broker_forecast_file(service, path, dry_run=dry_run))
    return summaries


def main() -> None:
    parser = argparse.ArgumentParser(description='导入 resource_data/broker_forecasts 下的券商指数预测 CSV')
    parser.add_argument('--config', default=str(DEFAULT_CONFIG), help='配置文件路径')
    parser.add_argument('--dry-run', action='store_true', help='只解析和匹配，不写入数据库')
    parser.add_argument('--only-broker', help='只导入指定券商目录')
    parser.add_argument('--only-index', help='只导入指定指数文件名（不含 .csv）')
    args = parser.parse_args()

    service = SqlToolService(config_path=args.config)
    summaries = import_directory(
        service=service,
        dry_run=args.dry_run,
        only_broker=args.only_broker,
        only_index=args.only_index,
    )
    print(json.dumps([asdict(item) for item in summaries], ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
