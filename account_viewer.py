#!/usr/bin/env python3
"""
account_viewer.py

Minimalist UI to view all accounts with accordion layout - fetches data directly from AWS S3
"""

from flask import Flask, render_template, jsonify
import json
import boto3
from collections import defaultdict

app = Flask(__name__)

# AWS Configuration
S3_BUCKET = "awsidpdocs"
S3_PREFIX = "SplittedPdfs"
AWS_REGION = 'us-east-1'

# Initialize AWS client
s3 = boto3.client('s3', region_name=AWS_REGION)

@app.route('/')
def index():
    """Main account viewer page"""
    return render_template('account_viewer.html')

@app.route('/api/accounts')
def api_accounts():
    """API endpoint to get all accounts from AWS S3"""
    try:
        # Get all JSON files from S3
        account_data = fetch_accounts_from_s3()
        
        return jsonify({
            'success': True,
            'accounts': account_data,
            'count': len(account_data)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error fetching accounts from S3: {str(e)}',
            'accounts': [],
            'count': 0
        })

def fetch_accounts_from_s3():
    """Fetch all account data from S3"""
    print("🔍 Fetching account data from S3...")
    
    # Group files by account number
    accounts = defaultdict(lambda: {
        'account_number': '',
        'extracted_json': {},
        'attachments': [],
        'last_modified': ''
    })
    
    try:
        # List all objects in S3 bucket
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX)
        
        for page in pages:
            if 'Contents' not in page:
                continue
            
            for obj in page['Contents']:
                s3_key = obj['Key']
                
                # Only process final JSON results
                if (s3_key.endswith('_loan_indexed.json') or 
                    s3_key.endswith('_documents_classified.json')):
                    
                    try:
                        # Parse account number and type from S3 key
                        account_number, file_type = parse_s3_key(s3_key)
                        
                        if account_number:
                            # Download and parse JSON
                            response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
                            content = json.loads(response['Body'].read().decode('utf-8'))
                            
                            # Initialize account if not exists
                            if not accounts[account_number]['account_number']:
                                accounts[account_number]['account_number'] = account_number
                                accounts[account_number]['last_modified'] = obj['LastModified'].isoformat()
                            
                            # Store data based on type
                            if file_type == 'extraction':
                                accounts[account_number]['extracted_json'] = content
                            elif file_type == 'attachments':
                                accounts[account_number]['attachments'] = content if isinstance(content, list) else [content]
                            
                            # Update last modified to the latest
                            if obj['LastModified'].isoformat() > accounts[account_number]['last_modified']:
                                accounts[account_number]['last_modified'] = obj['LastModified'].isoformat()
                                
                    except Exception as e:
                        print(f"❌ Error processing {s3_key}: {e}")
        
        # Convert to list and sort by account number
        account_list = list(accounts.values())
        account_list.sort(key=lambda x: x['account_number'])
        
        print(f"📊 Found {len(account_list)} accounts")
        return account_list
        
    except Exception as e:
        print(f"❌ Error fetching from S3: {e}")
        return []

def parse_s3_key(s3_key):
    """Parse S3 key to extract account number and file type"""
    # Expected format: SplittedPdfs/{account}/{account}_{type}_{suffix}.json
    parts = s3_key.split('/')
    if len(parts) < 3:
        return None, None
    
    filename = parts[-1]
    
    if '_loan_indexed.json' in filename:
        account = filename.replace('_extraction_loan_indexed.json', '')
        return account, 'extraction'
    elif '_documents_classified.json' in filename:
        account = filename.replace('_attachments_documents_classified.json', '')
        return account, 'attachments'
    
    return None, None

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5003)