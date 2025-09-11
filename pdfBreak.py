#!/usr/bin/env python3
"""
split_pdf.py  input.pdf  a  b

Creates:
  input_part1_pages_a_b.pdf          (pages a–b)
  input_part2_pages_b+1_end.pdf      (pages b+1–last)
"""
import sys
from pathlib import Path
from PyPDF2 import PdfReader, PdfWriter

def split_pdf(pdf_path: str, a: int, b: int) -> None:
    src = Path(pdf_path)
    if not src.exists():
        sys.exit(f"File not found: {pdf_path}")

    reader = PdfReader(src)
    total = len(reader.pages)
    if not (1 <= a <= b <= total):
        sys.exit(f"Invalid range: 1 ≤ a ≤ b ≤ {total}")

    # ---- part 1: a -> b ----------------------------------------------------
    writer1 = PdfWriter()
    for i in range(a - 1, b):          # PyPDF2 uses 0-based indexing
        writer1.add_page(reader.pages[i])
    out1 = src.with_name(f"{src.stem}_part1_pages_{a}_{b}.pdf")
    with open(out1, "wb") as f:
        writer1.write(f)
    print("Created:", out1)

    # ---- part 2: b+1 -> last -----------------------------------------------
    writer2 = PdfWriter()
    for i in range(b, total):
        writer2.add_page(reader.pages[i])
    out2 = src.with_name(f"{src.stem}_part2_pages_{b+1}_end.pdf")
    with open(out2, "wb") as f:
        writer2.write(f)
    print("Created:", out2)

if __name__ == "__main__":
    pdf_file = "./Multiple set of Pdfs.pdf"
    try:
        a, b = 10, 12
    except ValueError:
        sys.exit("a and b must be integers")
    split_pdf(pdf_file, a, b)