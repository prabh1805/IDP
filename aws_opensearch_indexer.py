#!/usr/bin/env python3
"""
aws_opensearch_indexer.py

Step-by-step AWS OpenSearch integration for document indexing and search
"""

import boto3
import json
import requests
from requests_aws4auth import AWS4Auth
from datetime import datetime
import hashlib

# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------
OPENSEARCH_CONFIG = {
    'domain_endpoint': '',  # Will be set after domain creation
    'region': 'us-east-1',
    'index_name': 'document-analysis',
    's3_bucket': 'awsidpdocs',
    's3_prefix': 'SplittedPdfs'
}

# AWS clients
opensearch_client = boto3.client('opensearch', region_name=OPENSEARCH_CONFIG['region'])
s3 = boto3.client('s3', region_name=OPENSEARCH_CONFIG['region'])

# --------------------------------------------------
# STEP 1: CREATE OPENSEARCH DOMAIN
# --------------------------------------------------
def create_opensearch_domain():
    """Step 1: Create OpenSearch domain"""
    print("🚀 Step 1: Creating OpenSearch domain...")
    
    domain_name = 'document-search-domain'
    
    try:
        # Check if domain already exists
        try:
            response = opensearch_client.describe_domain(DomainName=domain_name)
            print(f"✅ Domain '{domain_name}' already exists")
            endpoint = response['DomainStatus']['Endpoint']
            print(f"📍 Endpoint: https://{endpoint}")
            return f"https://{endpoint}"
        except opensearch_client.exceptions.ResourceNotFoundException:
            pass
        
        # Create new domain
        response = opensearch_client.create_domain(
            DomainName=domain_name,
            EngineVersion='OpenSearch_2.3',
            ClusterConfig={
                'InstanceType': 't3.small.search',  # Free tier eligible
                'InstanceCount': 1,
                'DedicatedMasterEnabled': False
            },
            EBSOptions={
                'EBSEnabled': True,
                'VolumeType': 'gp3',
                'VolumeSize': 10
            },
            AccessPolicies=json.dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": "*"},
                        "Action": "es:*",
                        "Resource": f"arn:aws:es:{OPENSEARCH_CONFIG['region']}:*:domain/{domain_name}/*"
                    }
                ]
            }),
            DomainEndpointOptions={
                'EnforceHTTPS': True
            }
        )
        
        print(f"✅ OpenSearch domain creation initiated")
        print(f"⏳ Domain will be available in 10-15 minutes")
        print(f"📍 Check status with: aws opensearch describe-domain --domain-name {domain_name}")
        
        return None  # Domain not ready yet
        
    except Exception as e:
        print(f"❌ Error creating OpenSearch domain: {e}")
        return None

# --------------------------------------------------
# STEP 2: SETUP INDEX MAPPING
# --------------------------------------------------
def setup_index_mapping(endpoint):
    """Step 2: Create index with proper mapping"""
    print("🚀 Step 2: Setting up index mapping...")
    
    # AWS authentication
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, 
                       OPENSEARCH_CONFIG['region'], 'es', session_token=credentials.token)
    
    # Index mapping
    mapping = {
        "mappings": {
            "properties": {
                "account_number": {"type": "keyword"},
                "pdf_type": {"type": "keyword"},
                "s3_key": {"type": "keyword"},
                "customer_name": {"type": "text", "analyzer": "standard"},
                "pan": {"type": "keyword"},
                "aadhaar": {"type": "keyword"},
                "account_types": {"type": "keyword"},
                "account_purposes": {"type": "keyword"},
                "document_types": {"type": "keyword"},
                "document_content": {"type": "text", "analyzer": "standard"},
                "signers": {
                    "type": "nested",
                    "properties": {
                        "signer_name": {"type": "text"},
                        "ssn": {"type": "keyword"},
                        "address": {"type": "text"},
                        "phone": {"type": "keyword"}
                    }
                },
                "created_at": {"type": "date"},
                "metadata": {"type": "object", "dynamic": True}
            }
        },
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }
    }
    
    try:
        url = f"{endpoint}/{OPENSEARCH_CONFIG['index_name']}"
        response = requests.put(url, auth=awsauth, json=mapping, 
                              headers={'Content-Type': 'application/json'})
        
        if response.status_code in [200, 400]:  # 400 if index already exists
            print(f"✅ Index '{OPENSEARCH_CONFIG['index_name']}' created/updated")
            return True
        else:
            print(f"❌ Error creating index: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error setting up index: {e}")
        return False

# --------------------------------------------------
# STEP 3: INDEX DOCUMENTS FROM S3
# --------------------------------------------------
def index_documents_from_s3(endpoint):
    """Step 3: Index all documents from S3"""
    print("🚀 Step 3: Indexing documents from S3...")
    
    # AWS authentication
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, 
                       OPENSEARCH_CONFIG['region'], 'es', session_token=credentials.token)
    
    try:
        # List all processed JSON files
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=OPENSEARCH_CONFIG['s3_bucket'], 
                                 Prefix=OPENSEARCH_CONFIG['s3_prefix'])
        
        indexed_count = 0
        
        for page in pages:
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                s3_key = obj['Key']
                
                # Process structured JSON files
                if (s3_key.endswith('_loan_indexed.json') or 
                    s3_key.endswith('_documents_classified.json')):
                    
                    success = index_single_document(endpoint, s3_key, awsauth)
                    if success:
                        indexed_count += 1
        
        print(f"✅ Indexed {indexed_count} documents")
        return indexed_count
        
    except Exception as e:
        print(f"❌ Error indexing documents: {e}")
        return 0

def index_single_document(endpoint, s3_key, awsauth):
    """Index a single document"""
    try:
        # Download document from S3
        response = s3.get_object(Bucket=OPENSEARCH_CONFIG['s3_bucket'], Key=s3_key)
        document_data = json.loads(response['Body'].read().decode('utf-8'))
        
        # Parse metadata from S3 key
        account_number, pdf_type = parse_s3_key(s3_key)
        if not account_number:
            return False
        
        # Create document for indexing
        doc_id = hashlib.md5(s3_key.encode()).hexdigest()
        
        opensearch_doc = {
            "account_number": account_number,
            "pdf_type": pdf_type,
            "s3_key": s3_key,
            "document_content": json.dumps(document_data),
            "created_at": datetime.now().isoformat(),
            "metadata": document_data
        }
        
        # Extract specific fields based on document type
        if pdf_type == 'extraction' and isinstance(document_data, list):
            for account_info in document_data:
                if isinstance(account_info, dict):
                    opensearch_doc.update({
                        "customer_name": account_info.get('CustomerName', ''),
                        "pan": account_info.get('PAN', ''),
                        "aadhaar": account_info.get('Aadhaar', ''),
                        "account_types": account_info.get('AccountTypes', []),
                        "account_purposes": account_info.get('AccountPurposes', []),
                        "document_types": [doc.get('DocumentType', '') for doc in account_info.get('Documents', [])],
                        "signers": account_info.get('Signers', [])
                    })
                    break
        elif pdf_type == 'attachments' and isinstance(document_data, list):
            opensearch_doc["document_types"] = [doc.get('documentType', '') for doc in document_data]
        
        # Index the document
        url = f"{endpoint}/{OPENSEARCH_CONFIG['index_name']}/_doc/{doc_id}"
        response = requests.put(url, auth=awsauth, json=opensearch_doc,
                              headers={'Content-Type': 'application/json'})
        
        if response.status_code in [200, 201]:
            print(f"✅ Indexed: {s3_key}")
            return True
        else:
            print(f"❌ Failed to index {s3_key}: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error indexing {s3_key}: {e}")
        return False

def parse_s3_key(s3_key):
    """Parse S3 key to extract account and type"""
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

# --------------------------------------------------
# STEP 4: SEARCH FUNCTIONS
# --------------------------------------------------
def search_documents(endpoint, query, filters=None, size=10):
    """Step 4: Search documents"""
    print(f"🔍 Searching for: '{query}'")
    
    # AWS authentication
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, 
                       OPENSEARCH_CONFIG['region'], 'es', session_token=credentials.token)
    
    # Build search query
    search_body = {
        "query": {
            "bool": {
                "must": [],
                "filter": []
            }
        },
        "size": size,
        "highlight": {
            "fields": {
                "customer_name": {},
                "document_content": {}
            }
        }
    }
    
    if query:
        search_body["query"]["bool"]["must"].append({
            "multi_match": {
                "query": query,
                "fields": ["customer_name^2", "document_content", "account_number^3", "pan", "document_types"],
                "type": "best_fields"
            }
        })
    
    if filters:
        for key, value in filters.items():
            search_body["query"]["bool"]["filter"].append({
                "term": {key: value}
            })
    
    try:
        url = f"{endpoint}/{OPENSEARCH_CONFIG['index_name']}/_search"
        response = requests.post(url, auth=awsauth, json=search_body,
                               headers={'Content-Type': 'application/json'})
        
        if response.status_code == 200:
            results = response.json()
            print(f"✅ Found {results['hits']['total']['value']} results")
            return results['hits']['hits']
        else:
            print(f"❌ Search error: {response.text}")
            return []
            
    except Exception as e:
        print(f"❌ Search error: {e}")
        return []

# --------------------------------------------------
# STEP 5: TEST FUNCTIONS
# --------------------------------------------------
def test_search_scenarios(endpoint):
    """Step 5: Test different search scenarios"""
    print("🧪 Step 5: Testing search scenarios...")
    
    test_queries = [
        ("home loan", {}),
        ("ABC123", {}),
        ("loan", {"pdf_type": "extraction"}),
        ("driver license", {"pdf_type": "attachments"}),
        ("ABCDE1234F", {})  # PAN search
    ]
    
    for query, filters in test_queries:
        print(f"\n--- Testing: '{query}' with filters {filters} ---")
        results = search_documents(endpoint, query, filters, size=3)
        
        for i, result in enumerate(results[:2]):  # Show top 2 results
            source = result['_source']
            print(f"  Result {i+1}:")
            print(f"    Account: {source.get('account_number', 'N/A')}")
            print(f"    Customer: {source.get('customer_name', 'N/A')}")
            print(f"    Type: {source.get('pdf_type', 'N/A')}")
            print(f"    Score: {result['_score']}")

# --------------------------------------------------
# MAIN EXECUTION STEPS
# --------------------------------------------------
def main():
    """Execute all steps"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python aws_opensearch_indexer.py create-domain")
        print("  python aws_opensearch_indexer.py setup-index <endpoint>")
        print("  python aws_opensearch_indexer.py index-docs <endpoint>")
        print("  python aws_opensearch_indexer.py search <endpoint> <query>")
        print("  python aws_opensearch_indexer.py test <endpoint>")
        return
    
    command = sys.argv[1]
    
    if command == 'create-domain':
        endpoint = create_opensearch_domain()
        if endpoint:
            print(f"✅ Domain ready at: {endpoint}")
        else:
            print("⏳ Domain creation in progress. Check AWS console.")
    
    elif command == 'setup-index':
        if len(sys.argv) < 3:
            print("❌ Please provide endpoint: python aws_opensearch_indexer.py setup-index <endpoint>")
            return
        endpoint = sys.argv[2]
        setup_index_mapping(endpoint)
    
    elif command == 'index-docs':
        if len(sys.argv) < 3:
            print("❌ Please provide endpoint: python aws_opensearch_indexer.py index-docs <endpoint>")
            return
        endpoint = sys.argv[2]
        index_documents_from_s3(endpoint)
    
    elif command == 'search':
        if len(sys.argv) < 4:
            print("❌ Please provide endpoint and query: python aws_opensearch_indexer.py search <endpoint> <query>")
            return
        endpoint = sys.argv[2]
        query = ' '.join(sys.argv[3:])
        results = search_documents(endpoint, query)
        
        print(f"\n📋 Search Results for '{query}':")
        for i, result in enumerate(results):
            source = result['_source']
            print(f"\n{i+1}. Account: {source.get('account_number', 'N/A')}")
            print(f"   Customer: {source.get('customer_name', 'N/A')}")
            print(f"   Type: {source.get('pdf_type', 'N/A')}")
            print(f"   Score: {result['_score']}")
    
    elif command == 'test':
        if len(sys.argv) < 3:
            print("❌ Please provide endpoint: python aws_opensearch_indexer.py test <endpoint>")
            return
        endpoint = sys.argv[2]
        test_search_scenarios(endpoint)
    
    else:
        print(f"❌ Unknown command: {command}")

if __name__ == "__main__":
    main()