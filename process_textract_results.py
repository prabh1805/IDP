#!/usr/bin/env python3
"""
process_textract_results.py

Processes all .txt files from S3 using existing Claude functions from testingAWS.py and classifyAttachment.py
"""

import boto3
import json
import tempfile
from pathlib import Path

# Import Claude processing functions from existing files
from testingAWS import ask_claude as process_extraction_text
from classifyAttachment import claude_json as process_attachment_text

# Configuration
S3_BUCKET = "awsidpdocs"
S3_PREFIX = "SplittedPdfs"

s3 = boto3.client("s3")

def get_txt_files():
    """Get all .txt files from S3"""
    paginator = s3.get_paginator('list_objects_v2')
    return [obj['Key'] for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX) 
            if 'Contents' in page for obj in page['Contents'] 
            if obj['Key'].endswith('_textract_text.txt')]

def parse_filename(s3_key):
    """Extract account and type from filename"""
    filename = s3_key.split('/')[-1].replace('_textract_text.txt', '')
    if '_extraction' in filename:
        return filename.replace('_extraction', ''), 'extraction'
    elif '_attachments' in filename:
        return filename.replace('_attachments', ''), 'attachments'
    return None, None

def download_text(s3_key):
    """Download text from S3"""
    response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
    return response['Body'].read().decode('utf-8')

def upload_json(data, s3_key):
    """Upload JSON to S3"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(data, f, indent=2, default=str)
        f.flush()
        s3.upload_file(f.name, S3_BUCKET, s3_key)
        Path(f.name).unlink()
    print(f"📤 Uploaded: s3://{S3_BUCKET}/{s3_key}")

def process_file(s3_key):
    """Process single .txt file"""
    account, pdf_type = parse_filename(s3_key)
    if not account or not pdf_type:
        print(f"❌ Invalid filename: {s3_key}")
        return
    
    print(f"Processing {account} - {pdf_type}")
    text = download_text(s3_key)
    
    # Use appropriate processing function
    if pdf_type == 'extraction':
        result = process_extraction_text(text)
        suffix = 'loan_indexed'
    else:
        result = process_attachment_text(text)
        suffix = 'documents_classified'
    
    output_key = f"{S3_PREFIX}/{account}/{account}_{pdf_type}_{suffix}.json"
    upload_json(result, output_key)
    print(f"✅ Completed {account} - {pdf_type}")

def main():
    """Main execution"""
    print("🚀 Processing Textract Results")
    txt_files = get_txt_files()
    print(f"Found {len(txt_files)} files")
    
    for txt_file in txt_files:
        try:
            process_file(txt_file)
        except Exception as e:
            print(f"❌ Failed {txt_file}: {e}")
    
    print("🎉 Processing completed!")

if __name__ == "__main__":
    main()