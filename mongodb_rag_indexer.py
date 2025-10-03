#!/usr/bin/env python3
"""
mongodb_rag_indexer.py

MongoDB-based document indexing with RAG capabilities:
1. Fetches JSON results from S3 bucket
2. Stores documents in MongoDB organized by account number
3. Creates vector embeddings for semantic search
4. Enables both traditional MongoDB queries and RAG-based search
"""

import json
import boto3
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
import hashlib
import re

# MongoDB and vector search imports
try:
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure, DuplicateKeyError
    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False
    print("⚠️ pymongo not installed. Run: pip install pymongo")

try:
    from sentence_transformers import SentenceTransformer
    import numpy as np
    EMBEDDINGS_AVAILABLE = True
except ImportError:
    EMBEDDINGS_AVAILABLE = False
    print("⚠️ sentence-transformers not installed. Run: pip install sentence-transformers")

# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------
MONGODB_CONFIG = {
    'connection_string': 'mongodb+srv://prabhatjha_db_user:prabh@cluster0.qlto7ng.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0',
    'database_name': 'document_analysis',
    'collections': {
        'accounts': 'accounts',
        'documents': 'documents', 
        'embeddings': 'embeddings'
    },
    's3_bucket': 'awsidpdocs',
    's3_prefix': 'SplittedPdfs',
    'aws_region': 'us-east-1',
    'embedding_model': 'all-MiniLM-L6-v2',
    'vector_dimension': 384  # Dimension for all-MiniLM-L6-v2
}

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize AWS client
s3 = boto3.client('s3', region_name=MONGODB_CONFIG['aws_region'])

# --------------------------------------------------
# MONGODB RAG INDEXER
# --------------------------------------------------
class MongoDBRAGIndexer:
    """MongoDB-based indexer with RAG capabilities"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.client = None
        self.db = None
        self.embedding_model = None
        
        # Initialize connections
        self.connect_mongodb()
        self.init_embedding_model()
        self.setup_collections()
    
    def connect_mongodb(self):
        """Connect to MongoDB Atlas"""
        if not MONGODB_AVAILABLE:
            logger.error("❌ MongoDB not available - pymongo not installed")
            return False
        
        try:
            self.client = MongoClient(
                self.config['connection_string'],
                serverSelectionTimeoutMS=5000,  # 5 second timeout
                connectTimeoutMS=10000,         # 10 second connection timeout
                socketTimeoutMS=20000           # 20 second socket timeout
            )
            
            # Test the connection
            self.client.admin.command('ping')
            self.db = self.client[self.config['database_name']]
            
            logger.info("✅ Connected to MongoDB Atlas")
            return True
            
        except ConnectionFailure as e:
            logger.error(f"❌ MongoDB connection failed: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ MongoDB setup error: {e}")
            return False
    
    def init_embedding_model(self):
        """Initialize the embedding model"""
        if not EMBEDDINGS_AVAILABLE:
            logger.warning("⚠️ Embeddings disabled - sentence-transformers not available")
            return
        
        try:
            self.embedding_model = SentenceTransformer(self.config['embedding_model'])
            logger.info(f"✅ Loaded embedding model: {self.config['embedding_model']}")
        except Exception as e:
            logger.error(f"❌ Failed to load embedding model: {e}")
            self.embedding_model = None
    
    def setup_collections(self):
        """Setup MongoDB collections and indexes"""
        if self.db is None:
            return
        
        try:
            # Create collections
            collections = self.config['collections']
            
            # Accounts collection indexes
            accounts_col = self.db[collections['accounts']]
            accounts_col.create_index("account_number", unique=True)
            accounts_col.create_index("customer_name")
            accounts_col.create_index("pan")
            accounts_col.create_index("aadhaar")
            
            # Documents collection indexes
            documents_col = self.db[collections['documents']]
            documents_col.create_index("account_number", unique=True)  # One document per account
            documents_col.create_index("document_type")
            documents_col.create_index("created_at")
            
            # Embeddings collection indexes
            embeddings_col = self.db[collections['embeddings']]
            embeddings_col.create_index("document_id", unique=True)
            embeddings_col.create_index("account_number")
            
            # Create vector search index for embeddings (Atlas Search)
            # Note: This needs to be created manually in MongoDB Atlas UI or via API
            logger.info("✅ MongoDB collections and indexes setup complete")
            logger.info("📝 Note: Create vector search index manually in Atlas for embeddings collection")
            
        except Exception as e:
            logger.error(f"❌ Error setting up collections: {e}")
    
    def fetch_s3_documents(self) -> List[Dict]:
        """Fetch all processed JSON documents from S3 and group by account"""
        logger.info("📥 Fetching documents from S3...")
        
        account_documents = {}  # Group by account number
        
        try:
            # List all objects in S3 bucket
            paginator = s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(
                Bucket=self.config['s3_bucket'], 
                Prefix=self.config['s3_prefix']
            )
            
            for page in pages:
                if 'Contents' not in page:
                    continue
                
                for obj in page['Contents']:
                    s3_key = obj['Key']
                    
                    # Only process final JSON results
                    if (s3_key.endswith('_loan_indexed.json') or 
                        s3_key.endswith('_documents_classified.json')):
                        
                        try:
                            # Download and parse JSON
                            response = s3.get_object(
                                Bucket=self.config['s3_bucket'], 
                                Key=s3_key
                            )
                            content = json.loads(response['Body'].read().decode('utf-8'))
                            
                            # Parse metadata from S3 key
                            account_number, pdf_type = self._parse_s3_key(s3_key)
                            
                            if account_number and pdf_type:
                                # Initialize account if not exists
                                if account_number not in account_documents:
                                    account_documents[account_number] = {
                                        'account_number': account_number,
                                        'extraction_data': None,
                                        'attachment_data': None,
                                        's3_keys': [],
                                        'last_modified': obj['LastModified']
                                    }
                                
                                # Store data by type
                                if pdf_type == 'extraction':
                                    account_documents[account_number]['extraction_data'] = content
                                elif pdf_type == 'attachments':
                                    account_documents[account_number]['attachment_data'] = content
                                
                                account_documents[account_number]['s3_keys'].append(s3_key)
                                
                                # Update last modified to the latest
                                if obj['LastModified'] > account_documents[account_number]['last_modified']:
                                    account_documents[account_number]['last_modified'] = obj['LastModified']
                                
                        except Exception as e:
                            logger.error(f"❌ Error processing {s3_key}: {e}")
            
            # Convert to list and merge data
            merged_documents = []
            for account_number, account_data in account_documents.items():
                merged_content = self._merge_account_data(
                    account_data['extraction_data'], 
                    account_data['attachment_data']
                )
                
                merged_documents.append({
                    'account_number': account_number,
                    'merged_content': merged_content,
                    's3_keys': account_data['s3_keys'],
                    'last_modified': account_data['last_modified']
                })
            
            logger.info(f"📄 Found {len(merged_documents)} accounts with merged documents")
            return merged_documents
            
        except Exception as e:
            logger.error(f"❌ Error fetching S3 documents: {e}")
            return []
    
    def _parse_s3_key(self, s3_key: str) -> tuple:
        """Parse S3 key to extract account number and PDF type"""
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
    
    def _merge_account_data(self, extraction_data: Any, attachment_data: Any) -> Dict:
        """Merge extraction and attachment data into a single document"""
        merged = {
            'account_info': {},
            'loan_details': {},
            'signers': [],
            'documents': [],
            'attachments': []
        }
        
        # Process extraction data (loan information)
        if extraction_data and isinstance(extraction_data, list):
            for item in extraction_data:
                if isinstance(item, dict):
                    # Extract account information
                    merged['account_info'] = {
                        'customer_name': item.get('CustomerName', ''),
                        'pan': item.get('PAN', ''),
                        'aadhaar': item.get('Aadhaar', ''),
                        'dob': item.get('DOB', ''),
                        'customer_id': item.get('CustomerID', ''),
                        'account_numbers': item.get('AccountNumbers', []),
                        'account_types': item.get('AccountTypes', []),
                        'account_purposes': item.get('AccountPurposes', []),
                        'ownership_types': item.get('OwnershipTypes', []),
                        'date_opened': item.get('DateOpened', ''),
                        'date_revised': item.get('DateRevised', ''),
                        'opened_by': item.get('OpenedBy', ''),
                        'revised_by': item.get('RevisedBy', '')
                    }
                    
                    # Extract signers
                    merged['signers'] = item.get('Signers', [])
                    
                    # Extract documents
                    merged['documents'] = item.get('Documents', [])
                    
                    # Store raw loan details
                    merged['loan_details'] = item
                    break
        
        # Process attachment data (classified documents)
        if attachment_data and isinstance(attachment_data, list):
            merged['attachments'] = attachment_data
        
        return merged
    
    def index_documents(self, documents: List[Dict]):
        """Index merged documents in MongoDB"""
        if self.db is None:
            logger.error("❌ MongoDB not connected")
            return
        
        logger.info("📚 Indexing merged documents in MongoDB...")
        
        collections = self.config['collections']
        accounts_col = self.db[collections['accounts']]
        documents_col = self.db[collections['documents']]
        embeddings_col = self.db[collections['embeddings']]
        
        indexed_count = 0
        
        for doc in documents:
            try:
                account_number = doc['account_number']
                merged_content = doc['merged_content']
                s3_keys = doc['s3_keys']
                
                # Create document ID based on account number
                doc_id = hashlib.md5(account_number.encode()).hexdigest()
                
                # Index account information
                account_info = merged_content.get('account_info', {})
                if account_info:
                    account_doc = {
                        'account_number': account_number,
                        'customer_name': account_info.get('customer_name', ''),
                        'pan': account_info.get('pan', ''),
                        'aadhaar': account_info.get('aadhaar', ''),
                        'dob': account_info.get('dob', ''),
                        'customer_id': account_info.get('customer_id', ''),
                        'account_numbers': account_info.get('account_numbers', []),
                        'account_types': account_info.get('account_types', []),
                        'account_purposes': account_info.get('account_purposes', []),
                        'ownership_types': account_info.get('ownership_types', []),
                        'date_opened': account_info.get('date_opened', ''),
                        'date_revised': account_info.get('date_revised', ''),
                        'opened_by': account_info.get('opened_by', ''),
                        'revised_by': account_info.get('revised_by', ''),
                        'signers': merged_content.get('signers', []),
                        'documents': merged_content.get('documents', []),
                        'attachments': merged_content.get('attachments', []),
                        'updated_at': datetime.now()
                    }
                    
                    # Upsert account information
                    accounts_col.replace_one(
                        {'account_number': account_number},
                        account_doc,
                        upsert=True
                    )
                
                # Index merged document
                document_doc = {
                    '_id': doc_id,
                    'account_number': account_number,
                    'document_type': 'merged_account_data',
                    's3_keys': s3_keys,
                    'content': merged_content,
                    'text_content': self._extract_text_content(merged_content),
                    'metadata': self._extract_metadata(merged_content),
                    'created_at': datetime.now(),
                    'last_modified': doc['last_modified']
                }
                
                # Upsert document
                documents_col.replace_one(
                    {'_id': doc_id},
                    document_doc,
                    upsert=True
                )
                
                # Create embeddings
                if self.embedding_model:
                    self._create_embeddings(embeddings_col, doc_id, account_number, document_doc)
                
                indexed_count += 1
                logger.info(f"✅ Indexed merged data for account: {account_number}")
                
            except Exception as e:
                logger.error(f"❌ Error indexing account {doc.get('account_number', 'unknown')}: {e}")
        
        logger.info(f"🎉 Successfully indexed {indexed_count} merged documents")
    
    def _determine_document_type(self, content: Any) -> str:
        """Determine the primary document type for merged content"""
        return 'merged_account_data'
    
    def _extract_text_content(self, content: Any) -> str:
        """Extract searchable text from document content"""
        text_parts = []
        
        def extract_text_recursive(obj):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if isinstance(value, str) and value.strip():
                        text_parts.append(f"{key}: {value}")
                    elif isinstance(value, (list, dict)):
                        extract_text_recursive(value)
            elif isinstance(obj, list):
                for item in obj:
                    extract_text_recursive(item)
            elif isinstance(obj, str):
                text_parts.append(obj)
        
        extract_text_recursive(content)
        return ' '.join(text_parts)
    
    def _extract_metadata(self, content: Any) -> Dict:
        """Extract structured metadata for filtering"""
        metadata = {}
        
        if isinstance(content, list):
            for item in content:
                if isinstance(item, dict):
                    # Extract key fields for filtering
                    for key in ['documentType', 'CustomerName', 'PAN', 'AccountTypes', 
                               'licenseNumber', 'state', 'dateOfBirth']:
                        if key in item:
                            metadata[key] = item[key]
        elif isinstance(content, dict):
            metadata = {k: v for k, v in content.items() 
                       if isinstance(v, (str, int, float, list))}
        
        return metadata
    
    def _create_embeddings(self, embeddings_col, doc_id: str, account_number: str, document_doc: Dict):
        """Create and store vector embeddings"""
        try:
            text_content = document_doc['text_content']
            
            # Generate embedding
            embedding = self.embedding_model.encode(text_content).tolist()
            
            # Store embedding
            embedding_doc = {
                '_id': doc_id,
                'document_id': doc_id,
                'account_number': account_number,
                'embedding': embedding,
                'text_preview': text_content[:200],  # First 200 chars for reference
                'created_at': datetime.now()
            }
            
            embeddings_col.replace_one(
                {'_id': doc_id},
                embedding_doc,
                upsert=True
            )
            
        except Exception as e:
            logger.error(f"❌ Error creating embedding for {doc_id}: {e}")
    
    def search_documents(self, query: str, filters: Dict = None, limit: int = 10) -> List[Dict]:
        """Search documents using case-insensitive MongoDB queries"""
        if self.db is None:
            return []
        
        try:
            documents_col = self.db[self.config['collections']['documents']]
            accounts_col = self.db[self.config['collections']['accounts']]
            
            # Build MongoDB query - search across all fields without prefilled keys
            mongo_query = {}
            
            # Case-insensitive text search across all relevant fields
            if query:
                search_fields = [
                    {'text_content': {'$regex': query, '$options': 'i'}},
                    {'account_number': {'$regex': query, '$options': 'i'}},
                    # Search in nested content fields
                    {'content.account_info.customer_name': {'$regex': query, '$options': 'i'}},
                    {'content.account_info.pan': {'$regex': query, '$options': 'i'}},
                    {'content.account_info.aadhaar': {'$regex': query, '$options': 'i'}},
                    {'content.account_info.customer_id': {'$regex': query, '$options': 'i'}},
                    # Search in signers
                    {'content.signers.SignerName': {'$regex': query, '$options': 'i'}},
                    {'content.signers.SSN': {'$regex': query, '$options': 'i'}},
                    {'content.signers.Address': {'$regex': query, '$options': 'i'}},
                    {'content.signers.Employer': {'$regex': query, '$options': 'i'}},
                    # Search in attachments
                    {'content.attachments.documentType': {'$regex': query, '$options': 'i'}},
                    {'content.attachments.firstName': {'$regex': query, '$options': 'i'}},
                    {'content.attachments.lastName': {'$regex': query, '$options': 'i'}},
                    {'content.attachments.licenseNumber': {'$regex': query, '$options': 'i'}},
                    # Search in account types and purposes
                    {'content.account_info.account_types': {'$regex': query, '$options': 'i'}},
                    {'content.account_info.account_purposes': {'$regex': query, '$options': 'i'}}
                ]
                
                mongo_query['$or'] = search_fields
            
            # Apply additional filters if provided
            if filters:
                for key, value in filters.items():
                    if key == 'account_number':
                        mongo_query['account_number'] = {'$regex': value, '$options': 'i'}
                    elif key == 'document_type':
                        mongo_query['document_type'] = {'$regex': value, '$options': 'i'}
                    else:
                        # Generic filter for any field
                        mongo_query[key] = {'$regex': str(value), '$options': 'i'}
            
            # Execute search
            results = list(documents_col.find(mongo_query).limit(limit))
            
            # Convert ObjectId to string and format dates
            for result in results:
                result['_id'] = str(result['_id'])
                if 'created_at' in result:
                    result['created_at'] = result['created_at'].isoformat()
                if 'last_modified' in result:
                    result['last_modified'] = result['last_modified'].isoformat()
            
            logger.info(f"🔍 Found {len(results)} documents for query: '{query}'")
            return results
            
        except Exception as e:
            logger.error(f"❌ Search error: {e}")
            return []
    
    def semantic_search(self, query: str, limit: int = 10, similarity_threshold: float = 0.3) -> List[Dict]:
        """Perform semantic search using vector embeddings with prompt support"""
        if not self.embedding_model or self.db is None:
            logger.warning("⚠️ Semantic search not available")
            return []
        
        try:
            # Enhanced query processing for prompts
            processed_query = self._process_search_prompt(query)
            logger.info(f"🔍 Processing semantic search: '{query}' -> '{processed_query}'")
            
            # Generate query embedding
            query_embedding = self.embedding_model.encode(processed_query)
            
            # Get all embeddings
            embeddings_col = self.db[self.config['collections']['embeddings']]
            documents_col = self.db[self.config['collections']['documents']]
            
            all_embeddings = list(embeddings_col.find({}))
            
            if not all_embeddings:
                logger.warning("⚠️ No embeddings found in database")
                return []
            
            # Calculate similarities
            similarities = []
            for emb_doc in all_embeddings:
                try:
                    doc_embedding = np.array(emb_doc['embedding'])
                    similarity = np.dot(query_embedding, doc_embedding) / (
                        np.linalg.norm(query_embedding) * np.linalg.norm(doc_embedding)
                    )
                    similarities.append((similarity, emb_doc['document_id']))
                except Exception as e:
                    logger.warning(f"⚠️ Error calculating similarity for {emb_doc.get('document_id', 'unknown')}: {e}")
                    continue
            
            # Sort by similarity and take top results (lowered threshold)
            similarities.sort(key=lambda x: x[0], reverse=True)
            
            # Filter by threshold but be more lenient
            filtered_similarities = [(sim, doc_id) for sim, doc_id in similarities if sim >= similarity_threshold]
            
            # If no results with threshold, take top results anyway
            if not filtered_similarities and similarities:
                filtered_similarities = similarities[:limit]
                logger.info(f"📊 No results above threshold {similarity_threshold}, showing top {len(filtered_similarities)} results")
            
            # Get corresponding documents
            results = []
            for similarity, doc_id in filtered_similarities[:limit]:
                doc = documents_col.find_one({'_id': doc_id})
                if doc:
                    doc['_id'] = str(doc['_id'])
                    doc['similarity_score'] = float(similarity)
                    if 'created_at' in doc:
                        doc['created_at'] = doc['created_at'].isoformat()
                    if 'last_modified' in doc:
                        doc['last_modified'] = doc['last_modified'].isoformat()
                    results.append(doc)
            
            logger.info(f"🎯 Found {len(results)} semantically similar documents (threshold: {similarity_threshold})")
            return results
            
        except Exception as e:
            logger.error(f"❌ Semantic search error: {e}")
            return []
    
    def _process_search_prompt(self, query: str) -> str:
        """Process natural language prompts into better search queries"""
        # Convert common prompts to better search terms
        prompt_mappings = {
            # Document type prompts
            "show me all loan documents": "loan agreement personal consumer",
            "find mortgage papers": "mortgage loan home property",
            "get driver license info": "drivers license identification",
            "show identity documents": "identity verification drivers license",
            "find power of attorney": "power attorney legal document",
            "show bank statements": "bank statement financial",
            "get tax documents": "tax form 1040 w2 income",
            
            # Customer prompts
            "find customers in delaware": "delaware DE address state",
            "show elderly customers": "1940 1950 1960 age elderly senior",
            "find young customers": "1980 1990 2000 young",
            "customers with multiple accounts": "joint multiple accounts",
            
            # Account type prompts
            "show business accounts": "business commercial company",
            "find personal accounts": "personal individual consumer",
            "joint account holders": "joint multiple signers",
            
            # Financial prompts
            "high value accounts": "loan amount balance high value",
            "recent accounts": "2020 2021 2022 2023 2024 recent new",
            "old accounts": "2010 2011 2012 2013 2014 old established",
            
            # Location prompts
            "customers in new castle": "new castle delaware DE",
            "customers in newark": "newark delaware DE",
            "east coast customers": "delaware DE maryland MD new jersey NJ",
        }
        
        # Check for exact prompt matches
        query_lower = query.lower().strip()
        for prompt, enhanced_query in prompt_mappings.items():
            if prompt in query_lower:
                return enhanced_query
        
        # Enhance query with related terms
        enhanced_terms = []
        
        # Add document type enhancements
        if any(term in query_lower for term in ["license", "id", "identification"]):
            enhanced_terms.extend(["drivers license", "identification", "state id"])
        
        if any(term in query_lower for term in ["loan", "mortgage", "credit"]):
            enhanced_terms.extend(["loan agreement", "mortgage", "credit", "financial"])
        
        if any(term in query_lower for term in ["power", "attorney", "legal"]):
            enhanced_terms.extend(["power of attorney", "legal document", "authorization"])
        
        # Add location enhancements
        if any(term in query_lower for term in ["delaware", "de"]):
            enhanced_terms.extend(["delaware", "DE", "new castle", "newark"])
        
        # Combine original query with enhancements
        if enhanced_terms:
            return f"{query} {' '.join(enhanced_terms)}"
        
        return query
    
    def get_account_summary(self, account_number: str) -> Dict:
        """Get comprehensive account summary"""
        if self.db is None:
            return {}
        
        try:
            collections = self.config['collections']
            
            # Get account info
            account = self.db[collections['accounts']].find_one(
                {'account_number': account_number}
            )
            
            # Get all documents for this account
            documents = list(self.db[collections['documents']].find(
                {'account_number': account_number}
            ))
            
            # Convert ObjectIds to strings
            if account:
                account['_id'] = str(account['_id'])
                if 'updated_at' in account:
                    account['updated_at'] = account['updated_at'].isoformat()
            
            for doc in documents:
                doc['_id'] = str(doc['_id'])
                if 'created_at' in doc:
                    doc['created_at'] = doc['created_at'].isoformat()
                if 'last_modified' in doc:
                    doc['last_modified'] = doc['last_modified'].isoformat()
            
            return {
                'account_info': account,
                'documents': documents,
                'document_count': len(documents)
            }
            
        except Exception as e:
            logger.error(f"❌ Error getting account summary: {e}")
            return {}
    
    def index_s3_documents(self):
        """Main method to fetch from S3 and index in MongoDB"""
        logger.info("🚀 Starting S3 to MongoDB indexing process...")
        
        # Fetch documents from S3
        documents = self.fetch_s3_documents()
        
        if not documents:
            logger.warning("⚠️ No documents found in S3")
            return
        
        # Index in MongoDB
        self.index_documents(documents)
        
        logger.info("✅ S3 to MongoDB indexing completed!")

# --------------------------------------------------
# SEARCH API
# --------------------------------------------------
def create_mongodb_search_api():
    """Create Flask API for MongoDB search"""
    try:
        from flask import Flask, request, jsonify
        
        app = Flask(__name__)
        indexer = MongoDBRAGIndexer(MONGODB_CONFIG)
        
        @app.route('/search', methods=['GET'])
        def search():
            query = request.args.get('q', '')
            search_type = request.args.get('type', 'traditional')  # traditional or semantic
            limit = int(request.args.get('limit', 10))
            
            # No prefilled filters - let users search freely
            filters = {}
            
            if search_type == 'semantic':
                results = indexer.semantic_search(query, limit)
            else:
                results = indexer.search_documents(query, filters, limit)
            
            return jsonify({
                'query': query,
                'search_type': search_type,
                'results': results,
                'count': len(results)
            })
        
        @app.route('/account/<account_number>', methods=['GET'])
        def get_account(account_number):
            summary = indexer.get_account_summary(account_number)
            return jsonify(summary)
        
        @app.route('/reindex', methods=['POST'])
        def reindex():
            indexer.index_s3_documents()
            return jsonify({"status": "success", "message": "Reindexing completed"})
        
        @app.route('/health', methods=['GET'])
        def health():
            return jsonify({
                "status": "healthy",
                "mongodb_connected": indexer.db is not None,
                "embeddings_available": indexer.embedding_model is not None
            })
        
        return app
        
    except ImportError:
        logger.warning("⚠️ Flask not available - search API disabled")
        return None

# --------------------------------------------------
# MAIN EXECUTION
# --------------------------------------------------
def main():
    """Main execution function"""
    import sys
    
    indexer = MongoDBRAGIndexer(MONGODB_CONFIG)
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == 'index':
            indexer.index_s3_documents()
        elif command == 'search':
            if len(sys.argv) > 2:
                query = ' '.join(sys.argv[2:])
                results = indexer.search_documents(query)
                print(json.dumps(results, indent=2, default=str))
            else:
                print("Usage: python mongodb_rag_indexer.py search <query>")
        elif command == 'semantic':
            if len(sys.argv) > 2:
                query = ' '.join(sys.argv[2:])
                results = indexer.semantic_search(query)
                print(json.dumps(results, indent=2, default=str))
            else:
                print("Usage: python mongodb_rag_indexer.py semantic <query>")
        elif command == 'account':
            if len(sys.argv) > 2:
                account = sys.argv[2]
                summary = indexer.get_account_summary(account)
                print(json.dumps(summary, indent=2, default=str))
            else:
                print("Usage: python mongodb_rag_indexer.py account <account_number>")
        elif command == 'api':
            app = create_mongodb_search_api()
            if app:
                print("🚀 Starting MongoDB search API on http://localhost:5000")
                app.run(debug=True)
        else:
            print("Usage: python mongodb_rag_indexer.py [index|search|semantic|account|api]")
    else:
        print("Available commands:")
        print("  index              - Index all documents from S3")
        print("  search <query>     - Traditional search")
        print("  semantic <query>   - Semantic/RAG search")
        print("  account <number>   - Get account summary")
        print("  api               - Start search API server")

if __name__ == "__main__":
    main()