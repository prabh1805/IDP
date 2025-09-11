import boto3
import json
import time
import os
from io import BytesIO
from pdf2image import convert_from_bytes
from PIL import Image

import re
# Specify your AWS region (e.g., us-east-1)
REGION = 'us-east-1'  # ← Change to your preferred region
# Initialize AWS clients
textract = boto3.client('textract', region_name='us-east-1')
bedrock = boto3.client('bedrock-runtime', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)
# ========================
# CONFIGURATION
# ========================
BUCKET_NAME = 'awsidpdocs'         # Replace with your S3 bucket
DOCUMENT_KEY = 'sample2.pdf'           # Document in S3
MODEL_ID = 'anthropic.claude-v2'      # or 'anthropic.claude-instant-v1'


# ========================
# STEP 1: Extract Data using Amazon Textract
# ========================
def extract_document_data(bucket, document_key):
    print("Bucket:", bucket, "Key:", document_key)
    print("🚀 Starting async Textract job...")

    # 1. Start the async job
    resp = textract.start_document_analysis(
        DocumentLocation={
            'S3Object': {'Bucket': bucket, 'Name': document_key}
        },
        FeatureTypes=['TABLES','FORMS','SIGNATURES','LAYOUT']
    )
    job_id = resp['JobId']
    print("⏳ Textract job-id:", job_id)

    # 2. Wait for completion
    while True:
        resp = textract.get_document_analysis(JobId=job_id)
        status = resp['JobStatus']
        if status in ('SUCCEEDED', 'FAILED'):
            break
        print("   ...waiting 5 s")
        time.sleep(5)
    if status == 'FAILED':
        raise RuntimeError("Textract job failed: " + resp.get('StatusMessage', 'unknown'))

    # 3. Collect **all** blocks (pagination)
    blocks = resp['Blocks']
    while 'NextToken' in resp:
        resp = textract.get_document_analysis(JobId=job_id,
                                              NextToken=resp['NextToken'])
        blocks.extend(resp['Blocks'])

    # 4. Build form_data as before
    block_map = {b['Id']: b for b in blocks}
    form_data = {}
    for b in blocks:
        if b['BlockType'] == 'KEY_VALUE_SET' and 'KEY' in b.get('EntityTypes', []):
            key_words, value_words = [], []

            # key text
            for rel in b.get('Relationships', []):
                if rel['Type'] == 'CHILD':
                    for cid in rel['Ids']:
                        if block_map[cid]['BlockType'] == 'WORD':
                            key_words.append(block_map[cid]['Text'])

            # value text
            for rel in b.get('Relationships', []):
                if rel['Type'] == 'VALUE':
                    for vid in rel['Ids']:
                        vb = block_map[vid]
                        for vrel in vb.get('Relationships', []):
                            if vrel['Type'] == 'CHILD':
                                for cid in vrel['Ids']:
                                    if block_map[cid]['BlockType'] == 'WORD':
                                        value_words.append(block_map[cid]['Text'])
            key = ' '.join(key_words).strip()
            value = ' '.join(value_words).strip()
            if key:
                form_data[key] = value

    print("✅ Textract extraction complete.")
    return form_data, blocks
# ========================
# STEP 2: Validate with Amazon Bedrock
# ========================
def validate_with_bedrock(extracted_data):
    print("🧠 Sending data to Bedrock for validation...")

    prompt = f"""
You are a loan verification assistant at a U.S. bank. Analyze the following payroll data extracted from a pay stub.

Extracted Fields:
{json.dumps(extracted_data, indent=2)}

Answer the following:
1. What is the employee's gross monthly income? Convert from biweekly if needed.
2. Is the income consistent and reasonable?
3. Flag any missing, unclear, or suspicious fields.
4. Respond in strict JSON format only:

{{
  "gross_monthly_income": 5000,
  "income_consistent": true,
  "suspicious_fields": [],
  "confidence_score": 0.95,
  "recommendation": "verified|review_needed"
}}
"""

    body = json.dumps({
        "prompt": f"\n\nHuman:{prompt}\n\nAssistant:",
        "max_tokens_to_sample": 512,
        "temperature": 0.2,
        "top_p": 0.9,
    })

    response = bedrock.invoke_model(
        body=body,
        modelId=MODEL_ID,
        accept='application/json',
        contentType='application/json'
    )

    response_body = json.loads(response['body'].read())
    llm_output = response_body['completion'].strip()

    # Try to extract JSON from response
    try:
    # Look for first { and last }
        start = llm_output.find('{')
        end   = llm_output.rfind('}') + 1
        if start == -1 or end == 0:
            raise ValueError('No JSON brackets')
        json_result = json.loads(llm_output[start:end])
        print("✅ Bedrock validation complete.")
        return json_result
    except Exception as e:
        print("❌ Failed to parse Bedrock JSON:", str(e))
        return {"error": "LLM output not valid JSON", "raw": llm_output}

def export_all_visual_objects(bucket, key, blocks, out_dir="artifacts"):
    """
    Export every low-confidence WORD block + any SELECTION_ELEMENT as PNG crops.
    """
    os.makedirs(out_dir, exist_ok=True)

    # 1. Grab PDF bytes once
    pdf_bytes = s3.get_object(Bucket=bucket, Key=key)['Body'].read()

    # 2. Convert every page to 300-DPI images
    page_imgs = convert_from_bytes(pdf_bytes, dpi=300)

    # 3. Helper to crop & save
    def _crop_and_save(img, bbox, label, idx):
        w, h = img.size
        left   = int(bbox['Left']   * w)
        top    = int(bbox['Top']    * h)
        width  = int(bbox['Width']  * w)
        height = int(bbox['Height'] * h)
        crop = img.crop((left, top, left+width, top+height))
        crop.save(os.path.join(out_dir, f"{label}_{idx:03d}.png"))

    idx = 0
    for block in blocks:
        if block['BlockType'] == 'WORD':
            # heuristic: very low confidence ≈ handwriting
            if block.get('Confidence', 100) < 85:
                page = block['Page'] - 1
                if page < len(page_imgs):
                    _crop_and_save(page_imgs[page], block['Geometry']['BoundingBox'], 'word', idx)
                    idx += 1

        elif block['BlockType'] == 'SELECTION_ELEMENT':
            # checkboxes / stamps / signatures
            page = block['Page'] - 1
            if page < len(page_imgs):
                _crop_and_save(page_imgs[page], block['Geometry']['BoundingBox'], 'selection', idx)
                idx += 1

    # 4. Optionally also save full page images
    for p, img in enumerate(page_imgs):
        img.save(os.path.join(out_dir, f"page_{p:03d}.png"))

    print(f"✅ Exported {idx} visual objects + {len(page_imgs)} full pages → {out_dir}/")
# ========================
# STEP 3: Main Pipeline
# ========================
def main():
    print("🏦 Starting Bank Document IDP Pipeline...")

    try:
        # Step 1: Extract
        extracted, blocks = extract_document_data(BUCKET_NAME, DOCUMENT_KEY)

        # Step 2: Validate
        validation = validate_with_bedrock(extracted)

        # Step 3: Final Output
        result = {
            "document": DOCUMENT_KEY,
            "extracted_data": extracted,
            "validation": validation,
            "processed_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
        }

        # Save to file or send to database/API
        output_file = 'idp_result2.json'
        with open(output_file, 'w') as f:
            json.dump(result, f, indent=2)

        print(f"🎉 IDP Pipeline Complete! Results saved to {output_file}")
        print(json.dumps(result, indent=2))
        export_all_visual_objects(BUCKET_NAME, DOCUMENT_KEY, blocks)

    except Exception as e:
        print("💥 Error in pipeline:", str(e))


# ========================
# RUN IT!
# ========================
if __name__ == "__main__":
    main()