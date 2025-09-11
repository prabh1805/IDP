#!/usr/bin/env python3
"""
Multi-doc classifier – single file
1.  Textract PDF → text
2.  Claude-3 → JSON array with one object per *distinct* document
3.  Save to structured_output4.json
"""

import boto3
import json
import time
import re
from botocore.exceptions import ClientError

# -------------------------------------------------
CONFIG = dict(
    bucket='awsidpdocs',
    key='Attachment2.pdf',
    region='us-east-1',
    model='anthropic.claude-3-sonnet-20240229-v1:0',
    raw_json='textract_response.json',
    plain_text='extracted_text3.txt',
    structured='structured_output4.json'
)

textract = boto3.client('textract', region_name=CONFIG['region'])
bedrock = boto3.client('bedrock-runtime', region_name=CONFIG['region'])

# -------------------------------------------------
def start_textract():
    resp = textract.start_document_analysis(
        DocumentLocation={'S3Object': {'Bucket': CONFIG['bucket'], 'Name': CONFIG['key']}},
        FeatureTypes=['FORMS', 'TABLES']
    )
    print('✅ Textract job started:', resp['JobId'])
    return resp['JobId']

def wait_textract(job_id):
    while True:
        r = textract.get_document_analysis(JobId=job_id)
        st = r['JobStatus']
        if st in ('SUCCEEDED', 'FAILED'):
            print('✅ Job', st)
            return r if st == 'SUCCEEDED' else None
        print('⏳ Waiting …')
        time.sleep(5)

def get_all_blocks(job_id):
    blocks, nt = [], None
    while True:
        r = textract.get_document_analysis(JobId=job_id, NextToken=nt) if nt else \
            textract.get_document_analysis(JobId=job_id)
        blocks.extend(r['Blocks'])
        nt = r.get('NextToken')
        if not nt:
            return blocks

def linearise(blocks):
    return '\n'.join(b['Text'] for b in blocks if b['BlockType'] == 'LINE')

# -------------------------------------------------
def claude_json(text: str) -> list:
    prompt_text = f"""
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
{text}
"""
    
    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 4000,
        "temperature": 0,
        "messages": [{
            "role": "user",
            "content": [{
                "type": "text",
                "text": prompt_text[:15000]  # avoid truncation
            }]
        }]
    })

    resp = bedrock.invoke_model(
        modelId=CONFIG['model'],
        contentType='application/json',
        accept='application/json',
        body=body
    )

    raw = resp['body'].read()
    if not raw:
        raise RuntimeError('Bedrock response body is empty – stream already consumed?')

    claude = json.loads(raw)
    txt = claude['content'][0]['text'].strip()

    # Strip ```json ... ```
    txt = re.sub(r'^```json\s*', '', txt, flags=re.I)
    txt = re.sub(r'```\s*$', '', txt).strip()

    if not txt:
        raise RuntimeError('Claude returned empty text – nothing to parse')
    
    return json.loads(txt)  # -> Python list[dict]

# -------------------------------------------------
def main():
    jid = start_textract()
    if not jid:
        return

    if not wait_textract(jid):
        return

    blocks = get_all_blocks(jid)

    with open(CONFIG['raw_json'], 'w', encoding='utf8') as f:
        json.dump({'Blocks': blocks}, f, indent=2, default=str)
    print('📄 Saved raw Textract →', CONFIG['raw_json'])

    text = linearise(blocks)
    with open(CONFIG['plain_text'], 'w', encoding='utf8') as f:
        f.write(text)
    print('📄 Saved plain text →', CONFIG['plain_text'], f'({len(text)} chars)')

    data = claude_json(text)
    with open(CONFIG['structured'], 'w', encoding='utf8') as f:
        json.dump(data, f, indent=2)
    print('✨ Structured data →', CONFIG['structured'])

# -------------------------------------------------
if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('❌', e)
