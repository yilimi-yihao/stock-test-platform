from __future__ import annotations

from pathlib import Path

import pdfplumber


def extract_pdf_text(pdf_path: str | Path, max_pages: int | None = None) -> list[str]:
    path = Path(pdf_path)
    pages: list[str] = []
    with pdfplumber.open(path) as pdf:
        for idx, page in enumerate(pdf.pages):
            if max_pages is not None and idx >= max_pages:
                break
            text = page.extract_text() or ''
            pages.append(text)
    return pages


def extract_pdf_text_joined(pdf_path: str | Path, max_pages: int | None = None) -> str:
    return '\n\n'.join(extract_pdf_text(pdf_path, max_pages=max_pages))
