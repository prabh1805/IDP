#!/usr/bin/env python3
"""
setup_mongodb.py

Setup script for MongoDB RAG indexing system
"""

import subprocess
import sys

def install_dependencies():
    """Install required dependencies"""
    print("📦 Installing MongoDB dependencies...")
    
    try:
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", 
            "pymongo>=4.6.0",
            "sentence-transformers>=2.2.2", 
            "numpy>=1.21.0",
            "flask>=2.3.0",
            "dnspython>=2.4.0"
        ])
        print("✅ Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install dependencies: {e}")
        return False

def test_mongodb_connection():
    """Test MongoDB connection"""
    print("🔗 Testing MongoDB connection...")
    
    try:
        from mongodb_rag_indexer import MongoDBRAGIndexer, MONGODB_CONFIG
        
        indexer = MongoDBRAGIndexer(MONGODB_CONFIG)
        
        if indexer.db:
            print("✅ MongoDB connection successful")
            
            # Test basic operations
            collections = indexer.db.list_collection_names()
            print(f"📚 Available collections: {collections}")
            
            return True
        else:
            print("❌ MongoDB connection failed")
            return False
            
    except Exception as e:
        print(f"❌ MongoDB connection error: {e}")
        return False

def run_initial_indexing():
    """Run initial indexing of S3 documents"""
    print("🚀 Running initial indexing...")
    
    try:
        from mongodb_rag_indexer import MongoDBRAGIndexer, MONGODB_CONFIG
        
        indexer = MongoDBRAGIndexer(MONGODB_CONFIG)
        indexer.index_s3_documents()
        
        print("✅ Initial indexing completed")
        return True
        
    except Exception as e:
        print(f"❌ Initial indexing failed: {e}")
        return False

def main():
    """Main setup process"""
    print("🚀 MongoDB RAG Indexing Setup")
    print("=" * 40)
    
    # Step 1: Install dependencies
    if not install_dependencies():
        return
    
    # Step 2: Test connection
    if not test_mongodb_connection():
        print("\n❌ Setup failed - MongoDB connection issues")
        print("Please check your connection string and network connectivity")
        return
    
    # Step 3: Ask about initial indexing
    response = input("\n🔍 Run initial indexing of S3 documents? (y/n): ")
    if response.lower() in ['y', 'yes']:
        run_initial_indexing()
    
    print("\n✅ Setup completed!")
    print("\nNext steps:")
    print("1. Run indexing: python mongodb_rag_indexer.py index")
    print("2. Search documents: python mongodb_rag_indexer.py search 'your query'")
    print("3. Semantic search: python mongodb_rag_indexer.py semantic 'your query'")
    print("4. Start API: python mongodb_rag_indexer.py api")

if __name__ == "__main__":
    main()