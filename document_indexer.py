#!/usr/bin/env python3
"""
document_indexer.py

Comprehensive document indexing system that supports multiple search approaches:
1. Vector embeddings for semantic search (RAG)
2. Elasticsearch for structured search
3. Local SQLite for metadata queries
4. S3 metadata tagging for AWS-native search
"""

import json
import sqlite3
import boto3
import numpy as np
from pathlib import Path
from datetime import datetime
import hashlib
import logging
from typing import Dict, List, Any, Optional
import re

# Optional imports (install as needed)
try:
    from sentence_transformers import SentenceTransformer
    EMBEDDINGS_AVAILABLE = True
except ImportError:
    EMBEDDINGS_AVAILABLE = False
    print("⚠️ sentence-transformers not installed. Vector search will be disabled.")

try:
    from elasticsearch import Elasticsearch
    ELASTICSEARCH_AVAILABLE = True
except ImportError:
    ELASTICSEARCH_AVAILABLE = False
    print("⚠️ elasticsearch not installed. Elasticsearch indexing will be disabled.")

# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------
INDEXING_CONFIG = {
    'sqlite_db': 'document_index.db',
    's3_bucket': 'awsidpdocs',
    's3_prefix': 'SplittedPdfs',
    'embedding_model': 'all-MiniLM-L6-v2',  # Lightweight model
    'elasticsearch_host': 'localhost:9200',
    'elasticsearch_index': 'document_analysis',
    'vector_db_path': 'vector_embeddings.json',
    'aws_region': 'us-east-1'
}

# Initialize AWS client
s3 = boto3.client('s3', region_name=INDEXING_CONFIG['aws_region'])

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --------------------------------------------------
# SQLITE DATABASE INDEXER
# --------------------------------------------------
class SQLiteIndexer:
    """SQLite-based indexer for structured metadata queries"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize SQLite database with tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Documents table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_number TEXT NOT NULL,
                document_type TEXT NOT NULL,
                pdf_type TEXT NOT NULL,
                s3_key TEXT NOT NULL,
                file_hash TEXT UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Account information table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_number TEXT UNIQUE NOT NULL,
                customer_name TEXT,
                pan TEXT,
                aadhaar TEXT,
                dob TEXT,
                account_type TEXT,
                account_purpose TEXT,
                ownership_type TEXT,
                date_opened TEXT,
                date_revised TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Document metadata table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS document_metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id INTEGER,
                key TEXT NOT NULL,
                value TEXT,
                FOREIGN KEY (document_id) REFERENCES documents (id)
            )
        ''')
        
        # Signers table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS signers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_number TEXT NOT NULL,
                signer_name TEXT,
                ssn TEXT,
                address TEXT,
                phone TEXT,
                employer TEXT,
                occupation TEXT,
                dob TEXT,
                dl_number TEXT
            )
        ''')
        
        # Create indexes for better performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_account_number ON documents(account_number)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_document_type ON documents(document_type)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_pdf_type ON documents(pdf_type)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_customer_name ON accounts(customer_name)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_pan ON accounts(pan)')
        
        conn.commit()
        conn.close()
        logger.info("✅ SQLite database initialized")
    
    def index_document(self, account_number: str, document_data: Dict, s3_key: str, pdf_type: str):
        """Index a document in SQLite"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Create file hash for deduplication
            file_hash = hashlib.md5(json.dumps(document_data, sort_keys=True).encode()).hexdigest()
            
            # Insert document
            cursor.execute('''
                INSERT OR REPLACE INTO documents 
                (account_number, document_type, pdf_type, s3_key, file_hash, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (account_number, pdf_type, pdf_type, s3_key, file_hash, datetime.now()))
            
            document_id = cursor.lastrowid
            
            # Index account information if it's extraction type
            if pdf_type == 'extraction' and isinstance(document_data, list):
                for account_info in document_data:
                    if isinstance(account_info, dict):
                        cursor.execute('''
                            INSERT OR REPLACE INTO accounts 
                            (account_number, customer_name, pan, aadhaar, dob, account_type, 
                             account_purpose, ownership_type, date_opened, date_revised)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            account_number,
                            account_info.get('CustomerName', ''),
                            account_info.get('PAN', ''),
                            account_info.get('Aadhaar', ''),
                            account_info.get('DOB', ''),
                            ', '.join(account_info.get('AccountTypes', [])),
                            ', '.join(account_info.get('AccountPurposes', [])),
                            ', '.join(account_info.get('OwnershipTypes', [])),
                            account_info.get('DateOpened', ''),
                            account_info.get('DateRevised', '')
                        ))
                        
                        # Index signers
                        for signer in account_info.get('Signers', []):
                            cursor.execute('''
                                INSERT INTO signers 
                                (account_number, signer_name, ssn, address, phone, employer, occupation, dob, dl_number)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                account_number,
                                signer.get('SignerName', ''),
                                signer.get('SSN', ''),
                                signer.get('Address', ''),
                                signer.get('HomePhone', ''),
                                signer.get('Employer', ''),
                                signer.get('Occupation', ''),
                                signer.get('DOB', ''),
                                signer.get('DLNumber', '')
                            ))
            
            # Index metadata
            self._index_metadata(cursor, document_id, document_data)
            
            conn.commit()
            logger.info(f"✅ Indexed document {s3_key} in SQLite")
            
        except Exception as e:
            logger.error(f"❌ SQLite indexing error: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def _index_metadata(self, cursor, document_id: int, data: Any, prefix: str = ''):
        """Recursively index metadata"""
        if isinstance(data, dict):
            for key, value in data.items():
                full_key = f"{prefix}.{key}" if prefix else key
                if isinstance(value, (str, int, float)):
                    cursor.execute('''
                        INSERT INTO document_metadata (document_id, key, value)
                        VALUES (?, ?, ?)
                    ''', (document_id, full_key, str(value)))
                elif isinstance(value, list):
                    cursor.execute('''
                        INSERT INTO document_metadata (document_id, key, value)
                        VALUES (?, ?, ?)
                    ''', (document_id, full_key, json.dumps(value)))
        elif isinstance(data, list):
            for i, item in enumerate(data):
                self._index_metadata(cursor, document_id, item, f"{prefix}[{i}]")
    
    def search(self, query: str, filters: Dict = None) -> List[Dict]:
        """Search documents in SQLite"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Build search query
        sql = '''
            SELECT DISTINCT d.*, a.customer_name, a.pan, a.account_type
            FROM documents d
            LEFT JOIN accounts a ON d.account_number = a.account_number
            LEFT JOIN document_metadata dm ON d.id = dm.document_id
            WHERE 1=1
        '''
        params = []
        
        if query:
            sql += ' AND (d.account_number LIKE ? OR a.customer_name LIKE ? OR dm.value LIKE ?)'
            params.extend([f'%{query}%', f'%{query}%', f'%{query}%'])
        
        if filters:
            for key, value in filters.items():
                if key == 'account_number':
                    sql += ' AND d.account_number = ?'
                    params.append(value)
                elif key == 'document_type':
                    sql += ' AND d.document_type = ?'
                    params.append(value)
                elif key == 'pdf_type':
                    sql += ' AND d.pdf_type = ?'
                    params.append(value)
        
        cursor.execute(sql, params)
        results = cursor.fetchall()
        conn.close()
        
        # Convert to dictionaries
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in results]

# --------------------------------------------------
# VECTOR EMBEDDINGS INDEXER (RAG)
# --------------------------------------------------
class VectorIndexer:
    """Vector embeddings indexer for semantic search"""
    
    def __init__(self, model_name: str, db_path: str):
        self.db_path = db_path
        self.embeddings_data = []
        
        if EMBEDDINGS_AVAILABLE:
            self.model = SentenceTransformer(model_name)
            self.load_embeddings()
        else:
            logger.warning("⚠️ Vector indexing disabled - sentence-transformers not available")
    
    def load_embeddings(self):
        """Load existing embeddings from file"""
        if Path(self.db_path).exists():
            with open(self.db_path, 'r') as f:
                self.embeddings_data = json.load(f)
            logger.info(f"📚 Loaded {len(self.embeddings_data)} existing embeddings")
    
    def save_embeddings(self):
        """Save embeddings to file"""
        with open(self.db_path, 'w') as f:
            json.dump(self.embeddings_data, f, indent=2, default=str)
        logger.info(f"💾 Saved {len(self.embeddings_data)} embeddings")
    
    def index_document(self, account_number: str, document_data: Dict, s3_key: str, pdf_type: str):
        """Create embeddings for document"""
        if not EMBEDDINGS_AVAILABLE:
            return
        
        try:
            # Create text representation of the document
            text_content = self._extract_text_for_embedding(document_data)
            
            # Generate embedding
            embedding = self.model.encode(text_content).tolist()
            
            # Store embedding with metadata
            embedding_record = {
                'id': hashlib.md5(s3_key.encode()).hexdigest(),
                'account_number': account_number,
                'pdf_type': pdf_type,
                's3_key': s3_key,
                'text_content': text_content[:500],  # Store first 500 chars for reference
                'embedding': embedding,
                'created_at': datetime.now().isoformat()
            }
            
            # Remove existing record if it exists
            self.embeddings_data = [e for e in self.embeddings_data if e['id'] != embedding_record['id']]
            
            # Add new record
            self.embeddings_data.append(embedding_record)
            self.save_embeddings()
            
            logger.info(f"🔍 Created embedding for {s3_key}")
            
        except Exception as e:
            logger.error(f"❌ Vector indexing error: {e}")
    
    def _extract_text_for_embedding(self, data: Any) -> str:
        """Extract meaningful text from document data for embedding"""
        text_parts = []
        
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, str) and value.strip():
                    text_parts.append(f"{key}: {value}")
                elif isinstance(value, list):
                    text_parts.append(f"{key}: {', '.join(str(v) for v in value)}")
        elif isinstance(data, list):
            for item in data:
                text_parts.append(self._extract_text_for_embedding(item))
        
        return ' '.join(text_parts)
    
    def semantic_search(self, query: str, top_k: int = 10) -> List[Dict]:
        """Perform semantic search using embeddings"""
        if not EMBEDDINGS_AVAILABLE or not self.embeddings_data:
            return []
        
        try:
            # Generate query embedding
            query_embedding = self.model.encode(query)
            
            # Calculate similarities
            similarities = []
            for record in self.embeddings_data:
                doc_embedding = np.array(record['embedding'])
                similarity = np.dot(query_embedding, doc_embedding) / (
                    np.linalg.norm(query_embedding) * np.linalg.norm(doc_embedding)
                )
                similarities.append((similarity, record))
            
            # Sort by similarity and return top results
            similarities.sort(key=lambda x: x[0], reverse=True)
            return [record for _, record in similarities[:top_k]]
            
        except Exception as e:
            logger.error(f"❌ Semantic search error: {e}")
            return []

# --------------------------------------------------
# ELASTICSEARCH INDEXER
# --------------------------------------------------
class ElasticsearchIndexer:
    """Elasticsearch indexer for full-text and structured search"""
    
    def __init__(self, host: str, index_name: str):
        self.index_name = index_name
        
        if ELASTICSEARCH_AVAILABLE:
            try:
                self.es = Elasticsearch([host])
                self.create_index()
            except Exception as e:
                logger.warning(f"⚠️ Elasticsearch connection failed: {e}")
                self.es = None
        else:
            logger.warning("⚠️ Elasticsearch indexing disabled - elasticsearch not available")
            self.es = None
    
    def create_index(self):
        """Create Elasticsearch index with mapping"""
        if not self.es:
            return
        
        mapping = {
            "mappings": {
                "properties": {
                    "account_number": {"type": "keyword"},
                    "pdf_type": {"type": "keyword"},
                    "document_type": {"type": "keyword"},
                    "s3_key": {"type": "keyword"},
                    "customer_name": {"type": "text", "analyzer": "standard"},
                    "pan": {"type": "keyword"},
                    "aadhaar": {"type": "keyword"},
                    "account_types": {"type": "keyword"},
                    "document_content": {"type": "text", "analyzer": "standard"},
                    "created_at": {"type": "date"},
                    "metadata": {"type": "object", "dynamic": True}
                }
            }
        }
        
        try:
            if not self.es.indices.exists(index=self.index_name):
                self.es.indices.create(index=self.index_name, body=mapping)
                logger.info(f"✅ Created Elasticsearch index: {self.index_name}")
        except Exception as e:
            logger.error(f"❌ Elasticsearch index creation error: {e}")
    
    def index_document(self, account_number: str, document_data: Dict, s3_key: str, pdf_type: str):
        """Index document in Elasticsearch"""
        if not self.es:
            return
        
        try:
            doc_id = hashlib.md5(s3_key.encode()).hexdigest()
            
            # Prepare document for indexing
            es_doc = {
                "account_number": account_number,
                "pdf_type": pdf_type,
                "s3_key": s3_key,
                "document_content": json.dumps(document_data),
                "created_at": datetime.now(),
                "metadata": document_data
            }
            
            # Extract specific fields for extraction documents
            if pdf_type == 'extraction' and isinstance(document_data, list):
                for account_info in document_data:
                    if isinstance(account_info, dict):
                        es_doc.update({
                            "customer_name": account_info.get('CustomerName', ''),
                            "pan": account_info.get('PAN', ''),
                            "aadhaar": account_info.get('Aadhaar', ''),
                            "account_types": account_info.get('AccountTypes', [])
                        })
                        break
            
            # Index the document
            self.es.index(index=self.index_name, id=doc_id, body=es_doc)
            logger.info(f"🔍 Indexed document {s3_key} in Elasticsearch")
            
        except Exception as e:
            logger.error(f"❌ Elasticsearch indexing error: {e}")
    
    def search(self, query: str, filters: Dict = None, size: int = 10) -> List[Dict]:
        """Search documents in Elasticsearch"""
        if not self.es:
            return []
        
        try:
            # Build search query
            search_body = {
                "query": {
                    "bool": {
                        "must": [],
                        "filter": []
                    }
                },
                "size": size
            }
            
            if query:
                search_body["query"]["bool"]["must"].append({
                    "multi_match": {
                        "query": query,
                        "fields": ["customer_name", "document_content", "account_number"]
                    }
                })
            
            if filters:
                for key, value in filters.items():
                    search_body["query"]["bool"]["filter"].append({
                        "term": {key: value}
                    })
            
            # Execute search
            response = self.es.search(index=self.index_name, body=search_body)
            return [hit["_source"] for hit in response["hits"]["hits"]]
            
        except Exception as e:
            logger.error(f"❌ Elasticsearch search error: {e}")
            return []

# --------------------------------------------------
# MAIN DOCUMENT INDEXER
# --------------------------------------------------
class DocumentIndexer:
    """Main indexer that coordinates all indexing approaches"""
    
    def __init__(self, config: Dict):
        self.config = config
        
        # Initialize indexers
        self.sqlite_indexer = SQLiteIndexer(config['sqlite_db'])
        self.vector_indexer = VectorIndexer(config['embedding_model'], config['vector_db_path'])
        self.es_indexer = ElasticsearchIndexer(config['elasticsearch_host'], config['elasticsearch_index'])
        
        logger.info("🚀 Document indexer initialized")
    
    def index_s3_documents(self):
        """Index all documents from S3"""
        logger.info("📚 Starting S3 document indexing...")
        
        try:
            # List all JSON files in S3
            paginator = s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.config['s3_bucket'], Prefix=self.config['s3_prefix'])
            
            indexed_count = 0
            
            for page in pages:
                if 'Contents' not in page:
                    continue
                
                for obj in page['Contents']:
                    s3_key = obj['Key']
                    
                    # Process structured JSON files
                    if (s3_key.endswith('_loan_indexed.json') or 
                        s3_key.endswith('_documents_classified.json')):
                        
                        success = self.index_single_document(s3_key)
                        if success:
                            indexed_count += 1
            
            logger.info(f"✅ Indexed {indexed_count} documents from S3")
            
        except Exception as e:
            logger.error(f"❌ S3 indexing error: {e}")
    
    def index_single_document(self, s3_key: str) -> bool:
        """Index a single document from S3"""
        try:
            # Download document from S3
            response = s3.get_object(Bucket=self.config['s3_bucket'], Key=s3_key)
            document_data = json.loads(response['Body'].read().decode('utf-8'))
            
            # Parse S3 key to extract metadata
            account_number, pdf_type = self._parse_s3_key(s3_key)
            if not account_number or not pdf_type:
                logger.warning(f"⚠️ Could not parse S3 key: {s3_key}")
                return False
            
            logger.info(f"📄 Indexing {s3_key} (Account: {account_number}, Type: {pdf_type})")
            
            # Index in all systems
            self.sqlite_indexer.index_document(account_number, document_data, s3_key, pdf_type)
            self.vector_indexer.index_document(account_number, document_data, s3_key, pdf_type)
            self.es_indexer.index_document(account_number, document_data, s3_key, pdf_type)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error indexing {s3_key}: {e}")
            return False
    
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
    
    def search_all(self, query: str, search_type: str = 'all', filters: Dict = None) -> Dict:
        """Search across all indexing systems"""
        results = {}
        
        if search_type in ['all', 'sqlite']:
            results['sqlite'] = self.sqlite_indexer.search(query, filters)
        
        if search_type in ['all', 'vector']:
            results['vector'] = self.vector_indexer.semantic_search(query)
        
        if search_type in ['all', 'elasticsearch']:
            results['elasticsearch'] = self.es_indexer.search(query, filters)
        
        return results

# --------------------------------------------------
# SEARCH API
# --------------------------------------------------
def create_search_api():
    """Create a simple search API using Flask"""
    try:
        from flask import Flask, request, jsonify
        
        app = Flask(__name__)
        indexer = DocumentIndexer(INDEXING_CONFIG)
        
        @app.route('/search', methods=['GET'])
        def search():
            query = request.args.get('q', '')
            search_type = request.args.get('type', 'all')
            
            # Parse filters
            filters = {}
            if request.args.get('account'):
                filters['account_number'] = request.args.get('account')
            if request.args.get('pdf_type'):
                filters['pdf_type'] = request.args.get('pdf_type')
            
            results = indexer.search_all(query, search_type, filters)
            return jsonify(results)
        
        @app.route('/reindex', methods=['POST'])
        def reindex():
            indexer.index_s3_documents()
            return jsonify({"status": "success", "message": "Reindexing completed"})
        
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
    
    indexer = DocumentIndexer(INDEXING_CONFIG)
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == 'index':
            indexer.index_s3_documents()
        elif command == 'search':
            if len(sys.argv) > 2:
                query = ' '.join(sys.argv[2:])
                results = indexer.search_all(query)
                print(json.dumps(results, indent=2, default=str))
            else:
                print("Usage: python document_indexer.py search <query>")
        elif command == 'api':
            app = create_search_api()
            if app:
                print("🚀 Starting search API on http://localhost:5000")
                app.run(debug=True)
        else:
            print("Usage: python document_indexer.py [index|search|api]")
    else:
        print("Available commands:")
        print("  index  - Index all documents from S3")
        print("  search <query> - Search documents")
        print("  api    - Start search API server")

if __name__ == "__main__":
    main()