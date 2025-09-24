import boto3, json, time, os,re, logging
from botocore.exceptions import ClientError

# ----------------------------
# CONFIGURATION
# ----------------------------
S3_BUCKET_NAME = 'awsidpdocs'
S3_KEY         = 'Attachment1.pdf'          # your PDF
AWS_REGION     =  'us-east-1'
BEDROCK_MODEL  = 'anthropic.claude-3-sonnet-20240229-v1:0'

OUTPUT_RAW_JSON   = 'textract_response.json'
OUTPUT_TEXT       = 'extracted_text.txt'
OUTPUT_STRUCTURED = 'structured_output3.json'

textract = boto3.client('textract', region_name=AWS_REGION)
bedrock  = boto3.client('bedrock-runtime', region_name=AWS_REGION)

# ----------------------------
# 1. Kick off Textract (FORMS + TABLES)
# ----------------------------
def start_textract_job():
    try:
        resp = textract.start_document_analysis(
            DocumentLocation={
                'S3Object': {'Bucket': S3_BUCKET_NAME, 'Name': S3_KEY}
            },
            FeatureTypes=['FORMS', 'TABLES'],
        )
        job_id = resp['JobId']
        print(f"✅ Textract job started: {job_id}")
        return job_id
    except ClientError as e:
        print("❌ Could not start Textract:", e)
        return None

# ----------------------------
# 2. Poll until complete
# ----------------------------
def wait_for_job(job_id):
    while True:
        resp = textract.get_document_analysis(JobId=job_id)
        status = resp['JobStatus']
        if status in ('SUCCEEDED', 'FAILED'):
            print("✅ Job", status)
            return resp if status == 'SUCCEEDED' else None
        print("⏳ Waiting …")
        time.sleep(5)

# ----------------------------
# 3. Download *all* pages
# ----------------------------
def download_all_blocks(job_id):
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

# ----------------------------
# 4. Helpers
# ----------------------------
def linearize(blocks):
    lines = [b['Text'] for b in blocks if b['BlockType'] == 'LINE']
    return "\n".join(lines)

def ask_claude(text: str) -> dict:
    prompt = f"""
    You are an expert in loan-file indexing.  
    The following text is raw OCR from a PDF that may contain multiple accounts, multiple signers, and supporting documents.

    Return **only** a valid JSON array (do **not** wrap it in ```json or add any commentary).

    Indexing rules  
    1. One JSON object **per distinct account number**.  
    - If no account number is found on a page, link the page to the account whose account holder’s Name  otherwise create a new account object.  

    2. Required fields for every account object  
    - Account Holder Names – array of strings (primary and co-borrowers there can be multiple names per account)
    - AccountNumber      –  (primary keys)  
    - AccountType        –  (e.g., "Business", "Personal", "Joint")  
    - AccountPurpose    –  (e.g., "Consumer", "Home Loan", "Car Loan", "Education Loan")  
    - OwnershipType     – (e.g., "Single", "Joint", "Multiple")  
    - DateOpened          – ISO date string or empty string  
    - DateRevised         – ISO date string or empty string  
    - OpenedBy            – string (name)  
    - RevisedBy           – string (name)  
    - DOB                 – dd-mm-yyyy or yyyy-mm-dd  
    - CustomerID          – omit if absent  
    - Documents           – array of {{"DocumentType":"<type>","PageNumber":<int>}}  
    - Stampdate           – any date string found on the page; omit if none  
    - Mailing Address     – mailing address
    - SSN                 –Social security number
   
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

    # --- 1. Read the body stream ---
    raw_bytes = resp["body"].read()          # <-- make sure we fully consume it
    resp_obj  = json.loads(raw_bytes)        # <-- Bedrock returns JSON envelope
    model_text = resp_obj["content"][0]["text"]

    # --- 2. Debug print (temporary) ---
    print("---- RAW CLAUDE TEXT START ----")
    print(model_text)
    print("---- RAW CLAUDE TEXT END ----")

    # --- 3. Strip markdown fences if they exist ---
    # --- 3b. Grab the first {...} block ---
    # --- 3. Remove markdown fences ---
    clean = re.sub(r'^```(?:json)?', '', model_text, flags=re.IGNORECASE)
    clean = re.sub(r'```$', '', clean)
    clean = clean.strip()

    # --- 4. Parse whatever JSON Claude returned ---
    try:
        data = json.loads(clean)      # may be a dict or a list
    except json.JSONDecodeError as e:
        print("Invalid JSON:", clean)
        raise

    # --- 5. Save the JSON exactly as Claude produced it ---
    with open("structured_output.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    return data

# ----------------------------
# 5. Main flow
# ----------------------------
if __name__ == "__main__":
    jid = start_textract_job()
    if not jid:
        exit(1)

    first = wait_for_job(jid)
    if not first:
        exit(1)

    blocks = download_all_blocks(jid)

    # --- 5a. Save raw JSON ---
    with open(OUTPUT_RAW_JSON, 'w', encoding='utf-8') as f:
        json.dump({"Blocks": blocks}, f, indent=2, default=str)
    print(f"📄 Saved raw Textract → {OUTPUT_RAW_JSON}")

    # --- 5b. Save plain text ---
    text = linearize(blocks)
    with open(OUTPUT_TEXT, 'w', encoding='utf-8') as f:
        f.write(text)
    print(f"📄 Saved plain text → {OUTPUT_TEXT} ({len(text)} chars)")

    # --- 5c. Structured via Bedrock ---
    try:
        structured = ask_claude(text)
        with open(OUTPUT_STRUCTURED, 'w', encoding='utf-8') as f:
            json.dump(structured, f, indent=2)
        print(f"✨ Structured banking data → {OUTPUT_STRUCTURED}")
    except Exception as e:
        print("❌ Bedrock error:", e)