#!/usr/bin/env python3
"""
pipeline.py

Complete pipeline that:
1. Uploads PDFs to S3 using uploadToS3.py logic
2. Loops through all account numbers
3. Processes extraction PDFs with testingAWS.py logic
4. Processes attachment PDFs with classifyAttachment.py logic
5. Saves Textract results (.txt and .json) to S3 with distinguishable names
"""

import tempfile
import json
import time
import re
from pathlib import Path
import pypdfium2 as pdfium
import boto3
from botocore.exceptions import ClientError
from pdfBreaker import build_account_json

# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------
PDF_FILE = Path("./combinedPdf.pdf")
S3_BUCKET = "awsidpdocs"
S3_PREFIX = "SplittedPdfs"
AWS_REGION = 'us-east-1'
BEDROCK_MODEL = 'anthropic.claude-3-sonnet-20240229-v1:0'
AWS_PROFILE = None

# Initialize AWS clients
if AWS_PROFILE:
    boto3.setup_default_session(profile_name=AWS_PROFILE)

s3 = boto3.client("s3")
textract = boto3.client('textract', region_name=AWS_REGION)
bedrock = boto3.client('bedrock-runtime', region_name=AWS_REGION)

# --------------------------------------------------
# UTILITY FUNCTIONS FROM EXISTING FILES
# --------------------------------------------------
def parse_range(rng: str):
    """Parse page range string like '1-5' or '7'"""
    if not rng:
        return []
    parts = rng.split("-")
    if len(parts) == 1:
        return [int(parts[0])]
    return list(range(int(parts[0]), int(parts[1]) + 1))

def build_pdf(pdf_doc, page_nums, output_path):
    """Build a new PDF that contains *page_nums* (1-based) from *pdf_doc*."""
    page_nums = sorted(set(page_nums))
    dest_pdf = pdfium.PdfDocument.new()
    dest_pdf.import_pages(pdf_doc, pages=[p - 1 for p in page_nums])
    dest_pdf.save(output_path)

def upload_file(file_path, bucket, key):
    """Upload file to S3"""
    s3.upload_file(str(file_path), bucket, key)
    print(f" Uploaded  ->  s3://{bucket}/{key}")

# --------------------------------------------------
# TEXTRACT PROCESSING FUNCTIONS
# --------------------------------------------------
def start_textract_job(s3_key):
    """Start Textract job for a PDF in S3"""
    try:
        resp = textract.start_document_analysis(
            DocumentLocation={
                'S3Object': {'Bucket': S3_BUCKET, 'Name': s3_key}
            },
            FeatureTypes=['FORMS', 'TABLES'],
        )
        job_id = resp['JobId']
        print(f"✅ Textract job started for {s3_key}: {job_id}")
        return job_id
    except ClientError as e:
        print(f"❌ Could not start Textract for {s3_key}:", e)
        return None

def wait_for_textract_job(job_id):
    """Wait for Textract job to complete"""
    while True:
        resp = textract.get_document_analysis(JobId=job_id)
        status = resp['JobStatus']
        if status in ('SUCCEEDED', 'FAILED'):
            print(f"✅ Textract job {status}")
            return resp if status == 'SUCCEEDED' else None
        print("⏳ Waiting for Textract...")
        time.sleep(5)

def download_all_textract_blocks(job_id):
    """Download all blocks from Textract job"""
    all_blocks = []
    next_token = None
    while True:
        if next_token:
            resp = textract.get_document_analysis(JobId=job_id, NextToken=next_token)
        else:
            resp = textract.get_document_analysis(JobId=job_id)
        all_blocks.extend(resp['Blocks'])
        next_token = resp.get('NextToken')
        if not next_token:
            break
    return all_blocks

def linearize_textract_blocks(blocks):
    """Convert Textract blocks to plain text"""
    lines = [b['Text'] for b in blocks if b['BlockType'] == 'LINE']
    return "\n".join(lines)

# --------------------------------------------------
# BEDROCK/CLAUDE PROCESSING FUNCTIONS
# --------------------------------------------------
def process_extraction_with_claude(text: str) -> dict:
    """Process extraction PDF text with Claude (from testingAWS.py logic)"""
    prompt = f"""
    You are an expert in loan-file indexing.  
    The following text is raw OCR from a PDF that may contain multiple accounts, multiple signers, and supporting documents.

    Return **only** a valid JSON array (do **not** wrap it in ```json or add any commentary).

    Indexing rules  
    1. One JSON object **per distinct account number**.  
    - If no account number is found on a page, link the page to the account whose account holder's Name + PAN/Aadhaar/DOB already exists; otherwise create a new account object.  

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
    - CustomerName        – primary account holder's full legal name  
    - PAN                 – primary account holder's PAN  
    - Aadhaar             – primary account holder's Aadhaar  
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
            - MMN               – mother's maiden name  

    4. General extraction rules  
    - Preserve original spelling and casing.  
    - If a field is truly absent, supply an empty string or omit the key (never null).  
    - Any date-like string (e.g., "12/3/1956", "03-12-1956", "1956-12-03") should be normalized to dd-mm-yyyy.  
    - Page numbers are 1-based integers.

    OCR text:
    {text}
    """
    
    return invoke_claude(prompt)

def process_attachment_with_claude(text: str) -> list:
    """Process attachment PDF text with Claude (from classifyAttachment.py logic)"""
    prompt = f"""
You are an document classification analyst.
Examine **every page** of the text below, identify each distinct document, and return a **single JSON array** with one object per *unique* document.

Rules
1.  Each object **must** contain  
    "documentType" : "Provide the document type"
2.  Add only the fields normally present on that doc type.
4.  Wrap your answer in ```json … ``` fences only.

Example
```json
[
  {{
    "documentType": "drivers-license",
    "state": "CA",
    "licenseNumber": "DL12345678",
    "lastName": "DOE",
    "firstName": "JANE",
    "dateOfBirth": "1988-04-12"
  }},
  {{
    "documentType": "marriage-certificate",
    "county": "Clark County, NV",
    "dateOfMarriage": "2015-06-20",
    "spouse1FullName": "JANE DOE",
    "spouse2FullName": "JOHN DOE"
  }}
]
```

{text}
"""
    
    return invoke_claude(prompt, is_attachment=True)

def invoke_claude(prompt: str, is_attachment: bool = False):
    """Invoke Claude via Bedrock"""
    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 4000,
        "temperature": 0,
        "messages": [{"role": "user", "content": prompt}]
    })

    resp = bedrock.invoke_model(
        modelId=BEDROCK_MODEL,
        contentType="application/json",
        accept="application/json",
        body=body
    )

    raw_bytes = resp["body"].read()
    resp_obj = json.loads(raw_bytes)
    model_text = resp_obj["content"][0]["text"]

    print("---- RAW CLAUDE TEXT START ----")
    print(model_text)
    print("---- RAW CLAUDE TEXT END ----")

    # Clean up the response
    clean = re.sub(r'^```(?:json)?', '', model_text, flags=re.IGNORECASE)
    clean = re.sub(r'```$', '', clean)
    clean = clean.strip()

    try:
        data = json.loads(clean)
        return data
    except json.JSONDecodeError as e:
        print("Invalid JSON:", clean)
        raise

# --------------------------------------------------
# MAIN PIPELINE FUNCTIONS
# --------------------------------------------------
def upload_pdfs_to_s3():
    """Step 1: Upload split PDFs to S3 (from uploadToS3.py logic)"""
    print("=== STEP 1: Uploading PDFs to S3 ===")
    
    if not PDF_FILE.exists():
        raise SystemExit("PDF file not found")

    plan = build_account_json(PDF_FILE)
    print("JSON received:", plan)

    src_pdf = pdfium.PdfDocument(PDF_FILE.read_bytes())
    uploaded_files = {}

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        for account, ranges in plan.items():
            uploaded_files[account] = {}
            
            extraction_pages = parse_range(ranges.get("extraction", ""))
            attachment_pages = parse_range(ranges.get("attachments", ""))

            if extraction_pages:
                ext_pdf = tmpdir / f"{account}_extraction.pdf"
                build_pdf(src_pdf, extraction_pages, ext_pdf)
                s3_key = f"{S3_PREFIX}/{account}/{account}_extraction.pdf"
                upload_file(ext_pdf, S3_BUCKET, s3_key)
                uploaded_files[account]['extraction'] = s3_key

            if attachment_pages:
                att_pdf = tmpdir / f"{account}_attachments.pdf"
                build_pdf(src_pdf, attachment_pages, att_pdf)
                s3_key = f"{S3_PREFIX}/{account}/{account}_attachments.pdf"
                upload_file(att_pdf, S3_BUCKET, s3_key)
                uploaded_files[account]['attachments'] = s3_key

    print("All uploads finished.")
    return uploaded_files

def process_account_pdfs(uploaded_files):
    """Step 2: Process each account's PDFs with Textract and Claude"""
    print("=== STEP 2: Processing Account PDFs ===")
    
    for account, files in uploaded_files.items():
        print(f"\n--- Processing Account: {account} ---")
        
        # Process extraction PDF
        if 'extraction' in files:
            print(f"Processing extraction PDF for {account}")
            process_single_pdf(account, files['extraction'], 'extraction')
        
        # Process attachment PDF  
        if 'attachments' in files:
            print(f"Processing attachment PDF for {account}")
            process_single_pdf(account, files['attachments'], 'attachments')

def process_single_pdf(account, s3_key, pdf_type):
    """Process a single PDF through Textract and Claude"""
    print(f"  Starting Textract for {s3_key}")
    
    # Start Textract job
    job_id = start_textract_job(s3_key)
    if not job_id:
        print(f"  Failed to start Textract for {s3_key}")
        return
    
    # Wait for completion
    result = wait_for_textract_job(job_id)
    if not result:
        print(f"  Textract failed for {s3_key}")
        return
    
    # Download all blocks
    blocks = download_all_textract_blocks(job_id)
    
    # Convert to text
    text = linearize_textract_blocks(blocks)
    
    # Save raw JSON to S3
    raw_json_key = f"{S3_PREFIX}/{account}/{account}_{pdf_type}_textract_raw.json"
    save_to_s3({"Blocks": blocks}, raw_json_key, is_json=True)
    
    # Save plain text to S3
    text_key = f"{S3_PREFIX}/{account}/{account}_{pdf_type}_textract_text.txt"
    save_to_s3(text, text_key, is_json=False)
    
    # Process with Claude based on PDF type
    try:
        if pdf_type == 'extraction':
            structured_data = process_extraction_with_claude(text)
        else:  # attachments
            structured_data = process_attachment_with_claude(text)
        
        # Save structured data to S3
        structured_key = f"{S3_PREFIX}/{account}/{account}_{pdf_type}_structured.json"
        save_to_s3(structured_data, structured_key, is_json=True)
        
        print(f"  ✅ Completed processing {s3_key}")
        
    except Exception as e:
        print(f"  ❌ Claude processing failed for {s3_key}: {e}")

def save_to_s3(data, s3_key, is_json=True):
    """Save data to S3"""
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json' if is_json else '.txt') as tmp_file:
        if is_json:
            json.dump(data, tmp_file, indent=2, default=str)
        else:
            tmp_file.write(data)
        tmp_file.flush()
        
        upload_file(tmp_file.name, S3_BUCKET, s3_key)
        Path(tmp_file.name).unlink()  # Clean up temp file

# --------------------------------------------------
# MAIN EXECUTION
# --------------------------------------------------
def main():
    """Main pipeline execution"""
    print("🚀 Starting PDF Processing Pipeline")
    
    try:
        # Step 1: Upload PDFs to S3
        uploaded_files = upload_pdfs_to_s3()
        
        # Step 2: Process each account's PDFs
        process_account_pdfs(uploaded_files)
        
        print("\n🎉 Pipeline completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()