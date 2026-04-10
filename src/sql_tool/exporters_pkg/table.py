from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable


def export_rows_to_csv(rows: Iterable[dict], output_path: str, fieldnames: list[str]) -> str:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows([{key: row.get(key, '') for key in fieldnames} for row in rows])
    return str(path)


def export_rows_to_excel(rows: Iterable[dict], output_path: str, fieldnames: list[str]) -> str:
    from openpyxl import Workbook

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    wb = Workbook()
    ws = wb.active
    ws.append(fieldnames)
    for row in rows:
        ws.append([row.get(key, '') for key in fieldnames])
    wb.save(path)
    return str(path)


def export_rows_to_pdf(rows: Iterable[dict], output_path: str, fieldnames: list[str]) -> str:
    # 当前环境不强依赖 reportlab，先输出文本型占位 PDF 路径说明文件
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    txt_path = path.with_suffix('.txt')
    with open(txt_path, 'w', encoding='utf-8') as f:
        f.write('PDF exporter placeholder\n')
        f.write(', '.join(fieldnames) + '\n')
        for row in rows:
            f.write(' | '.join(str(row.get(key, '')) for key in fieldnames) + '\n')
    return str(txt_path)
