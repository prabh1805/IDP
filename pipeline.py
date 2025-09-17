#!/usr/bin/env python3
"""
Cheap, idempotent pipeline controller.

1.  Decide whether a PDF needs Textract (cache in DynamoDB or local JSON).
2.  Run the correct script (extraction vs attachment).
3.  Upload artefacts to the same account prefix:
        s3://bucket/<account>/textract/<type>/textract_response.json
                                         /extracted_text.txt
                                         /structured_output.json
4.  Keep local mirror in  ./output/<account>/<type>/…
"""

from __future__ import annotations
import os
import json
import tempfile
import shutil
import subprocess
from pathlib import Path
from typing import Literal

import boto3
from botocore.exceptions import ClientError

# ------------------------------------------------ config
BUCKET      = os.getenv("S3_BUCKET_NAME", "awsidpdocs")
AWS_REGION  = os.getenv("AWS_REGION", "us-east-1")
CACHE_TABLE = os.getenv("CACHE_TABLE", "")      # DynamoDB table name (optional)
LOCAL_CACHE = Path("./cache.json")              # fallback JSON file
OUTPUT_ROOT = Path("./output")
# ------------------------------------------------

s3 = boto3.client("s3", region_name=AWS_REGION)
if CACHE_TABLE:
    dynamo = boto3.resource("dynamodb", region_name=AWS_REGION)
    table  = dynamo.Table(CACHE_TABLE)


def _cache_key(s3_key: str) -> str:
    """Normalised key used for cache."""
    return s3_key.lower().strip("/")


# -------------- caching layer ------------------
def already_done(s3_key: str) -> bool:
    """True → we have artefacts for this PDF."""
    key = _cache_key(s3_key)
    if CACHE_TABLE:
        try:
            return "Item" in table.get_item(Key={"pdf_key": key})
        except table.meta.client.exceptions.ResourceNotFoundException:
            pass                                    # table missing → fall back
    # local JSON fallback
    if LOCAL_CACHE.exists():
        return key in json.loads(LOCAL_CACHE.read_text())
    return False


def mark_done(s3_key: str):
    key = _cache_key(s3_key)
    if CACHE_TABLE:
        try:
            table.put_item(Item={"pdf_key": key})
            return
        except Exception:
            pass
    # fallback to local file
    cache = json.loads(LOCAL_CACHE.read_text()) if LOCAL_CACHE.exists() else {}
    cache[key] = True
    LOCAL_CACHE.write_text(json.dumps(cache, indent=2))


# -------------- run script once ----------------
def _run_script(script: Literal["extraction.py", "attachment.py"],
                s3_pdf_key: str, tmpdir: Path) -> dict[str, Path]:
    """Return dict with paths to the 3 artefacts produced."""
    env = os.environ.copy()
    env.update({
        "S3_BUCKET_NAME": BUCKET,
        "S3_KEY": s3_pdf_key,
        "AWS_REGION": AWS_REGION,
        "BEDROCK_MODEL": "anthropic.claude-3-sonnet-20240229-v1:0",
        "OUTPUT_DIR": str(tmpdir),
    })
    subprocess.run([str(Path(__file__).with_name(script))], env=env, check=True)

    return {
        "json":       next(tmpdir.glob("textract_response*.json"), None),
        "txt":        next(tmpdir.glob("extracted_text*.txt"), None),
        "structured": next(tmpdir.glob("structured_output*.json"), None),
    }


# -------------- upload / mirror ---------------
def _upload_and_mirror(account: str, run_type: str, files: dict[str, Path]):
    s3_base   = f"{account}/textract/{run_type}"
    local_dir = OUTPUT_ROOT / account / run_type
    local_dir.mkdir(parents=True, exist_ok=True)

    for name, file in files.items():
        if not file or not file.exists():
            continue
        s3_key = f"{s3_base}/{file.name}"
        s3.upload_file(str(file), BUCKET, s3_key)
        shutil.copy2(file, local_dir / file.name)
        print(f"  ↑ {name}  →  s3://{BUCKET}/{s3_key}")


# -------------- public helper -----------------
def process_pdf(s3_pdf_key: str, account: str, run_type: Literal["extraction", "attachment"]):
    """Idempotent entry-point usable from any other script."""
    if already_done(s3_pdf_key):
        print(f"SKIP {s3_pdf_key} – already processed")
        return

    print(f"\n>>> {run_type.upper()}  –  {s3_pdf_key}")
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        script = "extraction.py" if run_type == "extraction" else "attachment.py"
        artefacts = _run_script(script, s3_pdf_key, tmp)
        _upload_and_mirror(account, run_type, artefacts)

    mark_done(s3_pdf_key)
    print(f"  ✅ done – artefacts in s3://{BUCKET}/{account}/textract/{run_type}/")


# ------------------------------------------------ CLI (optional)
if __name__ == "__main__":
    import sys
    if len(sys.argv) != 4:
        print("usage:  pipeline.py  <s3-key>  <account>  <extraction|attachment>")
        sys.exit(1)
    _, key, acc, typ = sys.argv
    process_pdf(key, acc, typ)