import boto3, json, time, os,re, logging
from botocore.exceptions import ClientError

# ----------------------------
# CONFIGURATION
# ----------------------------
S3_BUCKET_NAME = 'awsidpdocs'
S3_KEY         = 'Attachment2.pdf'          # your PDF
AWS_REGION     =  'us-east-1'
BEDROCK_MODEL  = 'anthropic.claude-3-sonnet-20240229-v1:0'

OUTPUT_RAW_JSON   = 'textract_response.json'
OUTPUT_TEXT       = 'extracted_text2.txt'
OUTPUT_STRUCTURED = 'structured_output4.json'

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
    You will be shared attachment you have to classify what type of document it is and extract the relevant information from the document and provide the output in JSON format.
    If you are unable to classify the document or extract the information, respond with an empty JSON
    Provide the output in the following format:
    {{
      "document_type": "type of document",
        "document_information" : "",
        ..
        "key1": "value1",
        "key2": "value2",
        ...
    }}
    {text}
    """
    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 4000,
        "temperature": 0,
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": (
                            "You will be given a document. "
                            "Classify its type and extract the relevant information into JSON. "
                            "If you cannot classify it or extract anything, return an empty JSON object.\n\n"
                            "Provide the output in the following format:\n"
                            '{\n'
                            '  "document_type": "type of document",\n'
                            '  "key1": "value1",\n'
                            '  "key2": "value2",\n'
                            '  ...\n'
                            '}\n\n'
                            "Document text:\n" + text
                        )
                    }
                ]
            }
        ]
    })
    # body = json.dumps({
    #     "anthropic_version": "bedrock-2023-05-31",
    #     "max_tokens": 4000,
    #     "temperature": 0,
    #     "messages": [{"role": "user", "content": prompt}]
    # })

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
    with open("structured_output2.json", "w", encoding="utf-8") as f:
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