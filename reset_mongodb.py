#!/usr/bin/env python3
"""
reset_mongodb.py

Reset MongoDB collections to start fresh with new structure
"""

from mongodb_rag_indexer import MongoDBRAGIndexer, MONGODB_CONFIG

def reset_collections():
    """Drop all collections and recreate them"""
    indexer = MongoDBRAGIndexer.__new__(MongoDBRAGIndexer)
    indexer.config = MONGODB_CONFIG
    indexer.connect_mongodb()
    
    if indexer.db is None:
        print("❌ Could not connect to MongoDB")
        return
    
    collections = MONGODB_CONFIG['collections']
    
    # Drop existing collections
    for collection_name in collections.values():
        try:
            indexer.db[collection_name].drop()
            print(f"🗑️ Dropped collection: {collection_name}")
        except Exception as e:
            print(f"⚠️ Could not drop {collection_name}: {e}")
    
    print("✅ Collections reset complete")
    print("Run: python mongodb_rag_indexer.py index")

if __name__ == "__main__":
    reset_collections()