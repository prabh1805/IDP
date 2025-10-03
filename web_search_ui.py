#!/usr/bin/env python3
"""
web_search_ui.py

Web UI for searching MongoDB document data with case-insensitive search
"""

from flask import Flask, render_template, request, jsonify
import json
from mongodb_rag_indexer import MongoDBRAGIndexer, MONGODB_CONFIG

app = Flask(__name__)
indexer = MongoDBRAGIndexer(MONGODB_CONFIG)

@app.route('/')
def index():
    """Main search page"""
    return render_template('search.html')

@app.route('/api/search', methods=['POST'])
def api_search():
    """API endpoint for search"""
    data = request.get_json()
    query = data.get('query', '').strip()
    search_type = data.get('search_type', 'traditional')
    limit = int(data.get('limit', 10))
    
    if not query:
        return jsonify({
            'success': False,
            'message': 'Please enter a search query',
            'results': [],
            'count': 0
        })
    
    try:
        print(f"🔍 Search request: '{query}' (type: {search_type})")
        
        if search_type == 'semantic':
            results = indexer.semantic_search(query, limit, similarity_threshold=0.2)
        else:
            results = indexer.search_documents(query, {}, limit)
        
        print(f"📊 Found {len(results)} results")
        
        return jsonify({
            'success': True,
            'query': query,
            'search_type': search_type,
            'results': results,
            'count': len(results)
        })
        
    except Exception as e:
        print(f"❌ Search error: {e}")
        return jsonify({
            'success': False,
            'message': f'Search error: {str(e)}',
            'results': [],
            'count': 0
        })

@app.route('/api/account/<account_number>')
def api_account(account_number):
    """API endpoint for account details"""
    try:
        summary = indexer.get_account_summary(account_number)
        return jsonify({
            'success': True,
            'account_number': account_number,
            'data': summary
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error fetching account: {str(e)}',
            'data': {}
        })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5002)