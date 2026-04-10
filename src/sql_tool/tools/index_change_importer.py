from __future__ import annotations

import csv
import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any
from urllib import request

from sql_tool.tools.pdf_extract import extract_pdf_text_joined


ROOT = Path(__file__).resolve().parents[3]
CSV_DIR = ROOT / 'resource_data' / 'index_change_csvdata'
PDF_DIR = Path(r'C:/Users/Administrator/Desktop/backtests/调整pdf')
DEFAULT_API = 'http://127.0.0.1:8000'
SKIP_FILES = {'中证香港100.csv'}
INDEX_META = {
    '沪深300': {'index_code': '000300', 'category': '宽基', 'benchmark': '沪深300指数'},
    '中证500': {'index_code': '000905', 'category': '宽基', 'benchmark': '中证500指数'},
    '中证1000': {'index_code': '000852', 'category': '宽基', 'benchmark': '中证1000指数'},
    '中证A50': {'index_code': '930050', 'category': '宽基', 'benchmark': '中证A50指数'},
    '中证A100': {'index_code': '000903', 'category': '宽基', 'benchmark': '中证A100指数'},
    '中证A500': {'index_code': '000510', 'category': '宽基', 'benchmark': '中证A500指数'},
}


@dataclass
class ImportSummary:
    file: str
    index_name: str
    created_or_found: bool
    inserted_dates: int
    inserted_changes: int
    warnings: list[str]
    errors: list[str]
    skipped: bool = False


def _to_yyyymmdd(value: str) -> str:
    value = (value or '').strip()
    if not value:
        return ''
    return value.replace('-', '').replace('/', '')


def _post_json(base_url: str, path: str, payload: dict[str, Any]) -> dict[str, Any]:
    data = json.dumps(payload, ensure_ascii=False).encode('utf-8')
    req = request.Request(
        f'{base_url.rstrip("/")}{path}',
        data=data,
        headers={'Content-Type': 'application/json'},
        method='POST',
    )
    with request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode('utf-8'))


def _get_json(base_url: str, path: str) -> Any:
    with request.urlopen(f'{base_url.rstrip("/")}{path}', timeout=30) as resp:
        return json.loads(resp.read().decode('utf-8'))


def parse_index_change_csv(path: Path) -> tuple[str, dict[str, list[dict[str, Any]]], list[str]]:
    index_name = path.stem
    warnings: list[str] = []
    grouped: dict[str, list[dict[str, Any]]] = {}
    current_section = ''
    seen: set[tuple[str, str, str]] = set()

    with path.open('r', encoding='utf-8-sig', newline='') as f:
        reader = csv.reader(f)
        for row_no, row in enumerate(reader, start=1):
            cells = [(c or '').strip() for c in row]
            if not any(cells):
                continue
            head = cells[0].replace(',', '')
            if head.startswith('调入'):
                current_section = 'added'
                continue
            if head.startswith('调出'):
                current_section = 'removed'
                continue
            if cells[:4] == ['公告日', '实施日', '证券代码', '证券名称']:
                continue
            if current_section not in {'added', 'removed'}:
                warnings.append(f'{path.name}: row {row_no} 未识别分段，已跳过')
                continue
            ann = _to_yyyymmdd(cells[0] if len(cells) > 0 else '')
            eff = _to_yyyymmdd(cells[1] if len(cells) > 1 else '')
            code = (cells[2] if len(cells) > 2 else '').strip()
            name = (cells[3] if len(cells) > 3 else '').strip()
            if not code or not name:
                warnings.append(f'{path.name}: row {row_no} 缺少代码或名称，已跳过')
                continue
            trade_date = eff or ann
            note_parts = []
            if not eff and ann:
                note_parts.append('effective_date_missing_fallback_to_announcement_date')
            if not trade_date:
                warnings.append(f'{path.name}: row {row_no} 缺少实施日和公告日，已跳过')
                continue
            key = (trade_date, current_section, code)
            if key in seen:
                warnings.append(f'{path.name}: row {row_no} 重复记录 {code} {trade_date} {current_section}，已去重')
                continue
            seen.add(key)
            grouped.setdefault(trade_date, []).append({
                'announcement_date': ann or None,
                'change_type': current_section,
                'code': code,
                'name': name,
                'source_file': path.name,
                'note': ';'.join(note_parts) if note_parts else '',
            })

    if not grouped:
        warnings.append(f'{path.name}: 无有效变更记录')
    return index_name, grouped, warnings


def scan_pdf_mentions(index_name: str, max_pages: int = 5) -> list[str]:
    findings: list[str] = []
    for pdf in sorted(PDF_DIR.glob('*.pdf')):
        try:
            text = extract_pdf_text_joined(pdf, max_pages=max_pages)
        except Exception as exc:
            findings.append(f'{pdf.name}: 读取失败 {exc}')
            continue
        if index_name in text:
            findings.append(pdf.name)
    return findings


def import_index_change_file(path: Path, base_url: str = DEFAULT_API, dry_run: bool = False) -> ImportSummary:
    index_name, grouped, warnings = parse_index_change_csv(path)
    errors: list[str] = []
    meta = INDEX_META.get(index_name, {'index_code': '', 'category': '宽基', 'benchmark': f'{index_name}指数'})

    if not grouped:
        pdf_hits = scan_pdf_mentions(index_name)
        if pdf_hits:
            warnings.append(f'{path.name}: PDF中检测到相关提及 {pdf_hits}')
        return ImportSummary(path.name, index_name, False, 0, 0, warnings, errors, skipped=True)

    if dry_run:
        return ImportSummary(path.name, index_name, True, len(grouped), sum(len(v) for v in grouped.values()), warnings, errors)

    entity = _post_json(base_url, '/indexes/entities', {
        'index_name': index_name,
        'index_code': meta.get('index_code', ''),
        'category': meta.get('category', '宽基'),
        'benchmark': meta.get('benchmark', f'{index_name}指数'),
    })
    index_id = entity['index_id']

    inserted_dates = 0
    inserted_changes = 0
    for trade_date, changes in sorted(grouped.items()):
        _post_json(base_url, '/indexes/changes', {
            'index_id': index_id,
            'trade_date': trade_date,
            'changes': changes,
            'from_snapshot_id': None,
            'to_snapshot_id': None,
        })
        inserted_dates += 1
        inserted_changes += len(changes)

    return ImportSummary(path.name, index_name, True, inserted_dates, inserted_changes, warnings, errors)


def import_directory(base_url: str = DEFAULT_API, dry_run: bool = False, only_index: str | None = None) -> list[ImportSummary]:
    results: list[ImportSummary] = []
    for path in sorted(CSV_DIR.glob('*.csv')):
        if path.name in SKIP_FILES:
            continue
        if only_index and path.stem != only_index:
            continue
        results.append(import_index_change_file(path, base_url=base_url, dry_run=dry_run))
    return results


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description='通过本地 API 导入指数成分调整 CSV')
    parser.add_argument('--api', default=DEFAULT_API, help='本地 API 基地址')
    parser.add_argument('--dry-run', action='store_true', help='只解析不写入')
    parser.add_argument('--only-index', help='只导入指定指数（如 沪深300）')
    parser.add_argument('--scan-pdf-only', help='只扫描某个指数在 PDF 中是否被提及')
    args = parser.parse_args()

    if args.scan_pdf_only:
        print(json.dumps(scan_pdf_mentions(args.scan_pdf_only), ensure_ascii=False, indent=2))
        return

    results = import_directory(base_url=args.api, dry_run=args.dry_run, only_index=args.only_index)
    print(json.dumps([asdict(item) for item in results], ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
