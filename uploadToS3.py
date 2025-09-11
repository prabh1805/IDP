#!/usr/bin/env python3
"""
split_and_upload.py

Reads the JSON produced earlier (account → page-ranges) and the original PDF,
creates two PDFs per account (extraction + attachments), uploads to S3.

Usage:
  export AWS_PROFILE=your-profile   # optional
  python split_and_upload.py \
         --pdf  "/combinedPdf.pdf" \
         --json out.json \
         --bucket my-bucket \
         --prefix "IDP Docs"
"""

import json
import tempfile
from pathlib import Path
from argparse import ArgumentParser
import pypdfium2 as pdfium
import boto3

s3 = boto3.client("s3")

def parse_range(rng: str):
    """
    '3-5' -> [3,4,5]
    '6'   -> [6]
    ''    -> []
    """
    if not rng:
        return []
    parts = rng.split("-")
    if len(parts) == 1:
        return [int(parts[0])]
    return list(range(int(parts[0]), int(parts[1]) + 1))

def build_pdf(pdf_doc, page_nums, output_path):
    """Write a new PDF containing only the selected 1-based page numbers."""
    page_nums = sorted(set(page_nums))
    src_pages = [pdf_doc[p - 1] for p in page_nums]   # pypdfium is 0-based
    dest_pdf  = pdfium.PdfDocument.new()
    for sp in src_pages:
        dest_pdf.import_pages(sp)
    dest_pdf.save(output_path)

def upload_to_s3(file_path, bucket, key):
    s3.upload_file(str(file_path), bucket, key)
    print(f" Uploaded  ->  s3://{bucket}/{key}")

def main():
    ap = ArgumentParser()
    ap.add_argument("--pdf", required=True, help="original multi-page PDF")
    ap.add_argument("--json", required=True, help="JSON with page ranges")
    ap.add_argument("--bucket", required=True, help="target S3 bucket")
    ap.add_argument("--prefix", default="IDP Docs", help="S3 key prefix")
    args = ap.parse_args()

    pdf_path = Path(args.pdf)
    json_path = Path(args.json)
    if not pdf_path.exists() or not json_path.exists():
        raise SystemExit("PDF or JSON file not found")

    plan = json.loads(json_path.read_text())
    src_pdf = pdfium.PdfDocument(pdf_path.read_bytes())

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        for account, ranges in plan.items():
            extraction_pages = parse_range(ranges.get("extraction", ""))
            attachment_pages = parse_range(ranges.get("attachments", ""))

            # ---- build PDFs ----
            ext_pdf = tmpdir / f"{account}_extraction.pdf"
            att_pdf = tmpdir / f"{account}_attachments.pdf"

            if extraction_pages:
                build_pdf(src_pdf, extraction_pages, ext_pdf)
                upload_to_s3(
                    ext_pdf,
                    args.bucket,
                    f"{args.prefix}/{account}/{account}_extraction.pdf"
                )
            if attachment_pages:
                build_pdf(src_pdf, attachment_pages, att_pdf)
                upload_to_s3(
                    att_pdf,
                    args.bucket,
                    f"{args.prefix}/{account}/{account}_attachments.pdf"
                )

    print("All done.")

if __name__ == "__main__":
    main()