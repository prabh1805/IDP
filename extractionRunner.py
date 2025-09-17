#!/usr/bin/env python3
"""
Stand-alone, loop-safe runner for EXTRACTION-type PDFs.
Call:  run_extraction(bucket, key, account)
All outputs (Textract JSON, plain text, structured JSON) are
uploaded to   s3://bucket/<account>/textract/extraction/
and mirrored locally under ./output/<account>/textract/extraction/
"""

from __future__ import annotations
import json
import time
import re
import tempfile
from pathlib import Path
from typing import Dict, Any

import boto3
from botocore.exceptions import ClientError

# ------------------------------------------------ defaults
DEFAULT_REGION = "us-east-1"
DEFAULT_MODEL  = "anthropic.claude-3-sonnet-20240229-v1:0"
LOCAL_ROOT     = Path(__file__).with_name("output")
LOCAL_ROOT.mkdir(exist_ok=True)

# ------------------------------------------------ clients
def _clients(region: str):
    return (
        boto3.client("textract", region_name=region),
        boto3.client("bedrock-runtime", region_name=region),
    )


# ------------------------------------------------ textract helpers
def _start_job(textract, bucket: str, key: str) -> str | None:
    try:
        resp = textract.start_document_analysis(
            DocumentLocation={"S3Object": {"Bucket": bucket, "Name": key}},
            FeatureTypes=["FORMS", "TABLES"],
        )
        print(f"✅ Textract job started: {resp['JobId']}")
        return resp["JobId"]
    except ClientError as e:
        print("❌ Could not start Textract:", e)
        return None


def _wait_job(textract, job_id: str) -> dict[str, Any] | None:
    while True:
        resp = textract.get_document_analysis(JobId=job_id)
        status = resp["JobStatus"]
        if status in ("SUCCEEDED", "FAILED"):
            print("✅ Job", status)
            return resp if status == "SUCCEEDED" else None
        print("⏳ Waiting …")
        time.sleep(5)


def _download_blocks(textract, job_id: str) -> list[dict[str, Any]]:
    blocks, nt = [], None
    while True:
        r = textract.get_document_analysis(JobId=job_id, NextToken=nt) if nt else textract.get_document_analysis(JobId=job_id)
        blocks.extend(r["Blocks"])
        nt = r.get("NextToken")
        if not nt:
            break
    return blocks


def _linearise(blocks: list[dict[str, Any]]) -> str:
    return "\n".join(b["Text"] for b in blocks if b["BlockType"] == "LINE")


# ------------------------------------------------ bedrock prompt
def _ask_claude(bedrock, text: str, model: str) -> list[dict[str, Any]]:
    prompt = f"""
You are an expert in loan-file indexing.
The following text is raw OCR from a PDF that may contain multiple accounts, multiple signers, and supporting documents.

Return **only** a valid JSON array (do **not** wrap it in ```json or add any commentary).

Indexing rules  
1. One JSON object **per distinct account number**.  
- If no account number is found on a page, link the page to the account whose account holder’s Name + PAN/Aadhaar/DOB already exists; otherwise create a new account object.  

2. Required fields for every account object  
- Account Holder Names – array of strings (primary and co-borrowers there can be multiple names per account)
- AccountNumbers      – array of strings (primary keys)  
- AccountTypes        – array of strings (e.g., "Business", "Personal", "Joint")  
- AccountPurposes     – array of strings (e.g., "Consumer", "Home Loan", "Car Loan", "Education Loan")  
- OwnershipTypes      – array of strings (e.g., "Single", "Joint", "Multiple")  
- DateOpened          – ISO date string or empty string  
- DateRevised         – ISO date string or empty string  
- OpenedBy            – string (name)  
- RevisedBy           – string (name)  
- CustomerName        – primary account holder’s full legal name  
- PAN                 – primary account holder’s PAN  
- Aadhaar             – primary account holder’s Aadhaar  
- DOB                 – dd-mm-yyyy or yyyy-mm-dd  
- CustomerID          – omit if absent  
- Documents           – array of {{"DocumentType":"<type>","PageNumber":<int>}}  
- Stampdate           – any date string found on the page; omit if none  
- Document Types  – array of strings (e.g., "Loan Agreement", "KYC", "Statement", "Form 16", "ITR", "Bank Statement", "Salary Slip", "EMI Receipt", "Lien Letter", "NOC", "Foreclosure Letter", "Property Document",'Marriage Certificate', 'Driver License' etc. give with page numbers)

3. Signers (guarantors, co-borrowers, or joint owners)  
- Create an array Signers.  
- Each signer object must contain:  
        - SignerName  
        - SSN  
        - Address           – full street address  
        - Mailing           – mailing address (if different)  
        - HomePhone  
        - WorkPhone  
        - Employer  
        - Occupation  
        - DOB               – dd-mm-yyyy  
        - BirthPlace  
        - DLNumber  
        - MMN               – mother’s maiden name  

4. General extraction rules  
- Preserve original spelling and casing.  
- If a field is truly absent, supply an empty string or omit the key (never null).  
- Any date-like string (e.g., “12/3/1956”, “03-12-1956”, “1956-12-03”) should be normalized to dd-mm-yyyy.  
- Page numbers are 1-based integers.

OCR text:
{text}
"""
    body = json.dumps(
        {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 4000,
            "temperature": 0,
            "messages": [{"role": "user", "content": prompt}],
        }
    )
    resp = bedrock.invoke_model(
        modelId=model,
        contentType="application/json",
        accept="application/json",
        body=body,
    )
    raw_bytes = resp["body"].read()
    resp_obj = json.loads(raw_bytes)
    model_text = resp_obj["content"][0]["text"]

    # remove ```json … ```
    clean = re.sub(r"^```(?:json)?", "", model_text, flags=re.I)
    clean = re.sub(r"```$", "", clean).strip()
    return json.loads(clean)


# ------------------------------------------------ upload helpers
def _upload_and_mirror(bucket: str, account: str, file_name: str, file_path: Path):
    s3_key = f"{account}/textract/extraction/{file_name}"
    s3 = boto3.client("s3")
    s3.upload_file(str(file_path), bucket, s3_key)
    print(f"  ↑ s3://{bucket}/{s3_key}")

    local = LOCAL_ROOT / account / "textract" / "extraction"
    local.mkdir(parents=True, exist_ok=True)
    shutil.copy2(file_path, local / file_name)


# ------------------------------------------------ public runner
def run_extraction(
    bucket: str,
    key: str,
    account: str,
    *,
    region: str = DEFAULT_REGION,
    model: str = DEFAULT_MODEL,
) -> list[dict[str, Any]]:
    """
    Main entry-point for EXTRACTION-type PDFs.
    Returns the structured JSON list produced by Bedrock.
    """
    textract, bedrock = _clients(region)

    job_id = _start_job(textract, bucket, key)
    if not job_id:
        raise RuntimeError("Textract job could not be started")

    if not _wait_job(textract, job_id):
        raise RuntimeError("Textract job failed")

    blocks = _download_blocks(textract, job_id)
    text = _linearise(blocks)

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)

        # 1. raw Textract JSON
        raw_json = tmp / "textract_response.json"
        raw_json.write_text(json.dumps({"Blocks": blocks}, indent=2, default=str))
        _upload_and_mirror(bucket, account, raw_json.name, raw_json)

        # 2. plain text
        txt_file = tmp / "extracted_text.txt"
        txt_file.write_text(text, encoding="utf-8")
        _upload_and_mirror(bucket, account, txt_file.name, txt_file)

        # 3. structured JSON
        structured = _ask_claude(bedrock, text, model)
        struct_file = tmp / "structured_output.json"
        struct_file.write_text(json.dumps(structured, indent=2), encoding="utf-8")
        _upload_and_mirror(bucket, account, struct_file.name, struct_file)

    print(f"✅ Extraction complete for account {account}")
    return structured


# ------------------------------------------------ CLI (optional)
if __name__ == "__main__":
    import sys, shutil

    if len(sys.argv) != 4:
        print("usage:  extraction_runner.py  <bucket>  <s3-key>  <account>")
        sys.exit(1)

    bucket, key, account = sys.argv[1:4]
    out = run_extraction(bucket, key, account)
    # also save to CWD for quick inspection
    Path("structured_output.json").write_text(json.dumps(out, indent=2))
    print("Local copy saved -> structured_output.json")