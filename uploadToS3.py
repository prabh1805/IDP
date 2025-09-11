#!/usr/bin/env python3
"""
uploadToS3.py

Imports pdfBreaker, gets the JSON, splits the PDF, uploads to S3.
No CLI arguments – just edit the four constants below.
"""

import tempfile
from pathlib import Path
import pypdfium2 as pdfium
import boto3
from pdfBreaker import build_account_json   # <-- reuse previous logic

# --------------------------------------------------
#  HARD-CODE HERE
# --------------------------------------------------
PDF_FILE   = Path("./combinedPdf.pdf")
S3_BUCKET  = "awsidpdocs"
S3_PREFIX  = ""
AWS_PROFILE= None            # set string if needed
# --------------------------------------------------

if AWS_PROFILE:
    boto3.setup_default_session(profile_name=AWS_PROFILE)
s3 = boto3.client("s3")

def parse_range(rng: str):
    if not rng:
        return []
    parts = rng.split("-")
    if len(parts) == 1:
        return [int(parts[0])]
    return list(range(int(parts[0]), int(parts[1]) + 1))

def build_pdf(pdf_doc, page_nums, output_path):
    page_nums = sorted(set(page_nums))
    src_pages = [pdf_doc[p - 1] for p in page_nums]
    dest_pdf = pdfium.PdfDocument.new()
    for sp in src_pages:
        dest_pdf.import_pages(sp)
    dest_pdf.save(output_path)

def upload(file_path, bucket, key):
    s3.upload_file(str(file_path), bucket, key)
    print(f" Uploaded  ->  s3://{bucket}/{key}")

def main():
    if not PDF_FILE.exists():
        raise SystemExit("PDF file not found")

    plan = build_account_json(PDF_FILE)          # <-- call pdfBreaker
    print("JSON received:", plan)

    src_pdf = pdfium.PdfDocument(PDF_FILE.read_bytes())

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        for account, ranges in plan.items():
            extraction_pages = parse_range(ranges.get("extraction", ""))
            attachment_pages = parse_range(ranges.get("attachments", ""))

            ext_pdf = tmpdir / f"{account}_extraction.pdf"
            att_pdf = tmpdir / f"{account}_attachments.pdf"

            if extraction_pages:
                build_pdf(src_pdf, extraction_pages, ext_pdf)
                upload(ext_pdf, S3_BUCKET, f"{S3_PREFIX}/{account}/{account}_extraction.pdf")

            if attachment_pages:
                build_pdf(src_pdf, attachment_pages, att_pdf)
                upload(att_pdf, S3_BUCKET, f"{S3_PREFIX}/{account}/{account}_attachments.pdf")

    print("All uploads finished.")

if __name__ == "__main__":
    main()