#!/usr/bin/env python3
"""
search_ui.py

Simple GUI for testing MongoDB search functionality
Allows searching with key-value parameters like account_number: 123456
"""

import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import json
from datetime import datetime
import threading

# Import MongoDB indexer
try:
    from mongodb_rag_indexer import MongoDBRAGIndexer, MONGODB_CONFIG
    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False

class DocumentSearchUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Document Search Interface")
        self.root.geometry("1000x700")
        
        # Initialize MongoDB indexer
        self.indexer = None
        if MONGODB_AVAILABLE:
            try:
                self.indexer = MongoDBRAGIndexer(MONGODB_CONFIG)
                self.connection_status = "✅ Connected to MongoDB"
            except Exception as e:
                self.connection_status = f"❌ MongoDB Error: {str(e)}"
        else:
            self.connection_status = "❌ MongoDB not available"
        
        self.setup_ui()
    
    def setup_ui(self):
        """Setup the user interface"""
        # Main frame
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(6, weight=1)
        
        # Title
        title_label = ttk.Label(main_frame, text="Document Search Interface", 
                               font=('Arial', 16, 'bold'))
        title_label.grid(row=0, column=0, columnspan=3, pady=(0, 10))
        
        # Connection status
        status_label = ttk.Label(main_frame, text=self.connection_status)
        status_label.grid(row=1, column=0, columnspan=3, pady=(0, 10))
        
        # Search type selection
        ttk.Label(main_frame, text="Search Type:").grid(row=2, column=0, sticky=tk.W, pady=5)
        self.search_type = tk.StringVar(value="traditional")
        search_frame = ttk.Frame(main_frame)
        search_frame.grid(row=2, column=1, sticky=(tk.W, tk.E), pady=5)
        
        ttk.Radiobutton(search_frame, text="Traditional Search", 
                       variable=self.search_type, value="traditional").pack(side=tk.LEFT)
        ttk.Radiobutton(search_frame, text="Semantic Search (RAG)", 
                       variable=self.search_type, value="semantic").pack(side=tk.LEFT, padx=(20, 0))
        
        # Query input
        ttk.Label(main_frame, text="Search Query:").grid(row=3, column=0, sticky=tk.W, pady=5)
        self.query_entry = ttk.Entry(main_frame, width=50)
        self.query_entry.grid(row=3, column=1, sticky=(tk.W, tk.E), pady=5, padx=(0, 10))
        
        # Filter section
        filter_frame = ttk.LabelFrame(main_frame, text="Filters (Key-Value Pairs)", padding="10")
        filter_frame.grid(row=4, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=10)
        filter_frame.columnconfigure(1, weight=1)
        filter_frame.columnconfigure(3, weight=1)
        
        # Filter 1
        ttk.Label(filter_frame, text="Key 1:").grid(row=0, column=0, sticky=tk.W, pady=2)
        self.key1_combo = ttk.Combobox(filter_frame, values=[
            "account_number", "pdf_type", "document_type", "customer_name", 
            "pan", "aadhaar", "account_type"
        ], width=15)
        self.key1_combo.grid(row=0, column=1, sticky=(tk.W, tk.E), pady=2, padx=(5, 10))
        
        ttk.Label(filter_frame, text="Value 1:").grid(row=0, column=2, sticky=tk.W, pady=2)
        self.value1_entry = ttk.Entry(filter_frame, width=20)
        self.value1_entry.grid(row=0, column=3, sticky=(tk.W, tk.E), pady=2, padx=(5, 0))
        
        # Filter 2
        ttk.Label(filter_frame, text="Key 2:").grid(row=1, column=0, sticky=tk.W, pady=2)
        self.key2_combo = ttk.Combobox(filter_frame, values=[
            "account_number", "pdf_type", "document_type", "customer_name", 
            "pan", "aadhaar", "account_type"
        ], width=15)
        self.key2_combo.grid(row=1, column=1, sticky=(tk.W, tk.E), pady=2, padx=(5, 10))
        
        ttk.Label(filter_frame, text="Value 2:").grid(row=1, column=2, sticky=tk.W, pady=2)
        self.value2_entry = ttk.Entry(filter_frame, width=20)
        self.value2_entry.grid(row=1, column=3, sticky=(tk.W, tk.E), pady=2, padx=(5, 0))
        
        # Buttons
        button_frame = ttk.Frame(main_frame)
        button_frame.grid(row=5, column=0, columnspan=3, pady=10)
        
        self.search_button = ttk.Button(button_frame, text="🔍 Search", command=self.perform_search)
        self.search_button.pack(side=tk.LEFT, padx=(0, 10))
        
        ttk.Button(button_frame, text="🗑️ Clear", command=self.clear_form).pack(side=tk.LEFT, padx=(0, 10))
        
        ttk.Button(button_frame, text="📊 Get Account Summary", 
                  command=self.get_account_summary).pack(side=tk.LEFT, padx=(0, 10))
        
        # Results area
        results_frame = ttk.LabelFrame(main_frame, text="Search Results", padding="10")
        results_frame.grid(row=6, column=0, columnspan=3, sticky=(tk.W, tk.E, tk.N, tk.S), pady=10)
        results_frame.columnconfigure(0, weight=1)
        results_frame.rowconfigure(0, weight=1)
        
        # Results text area with scrollbar
        self.results_text = scrolledtext.ScrolledText(results_frame, wrap=tk.WORD, 
                                                     width=80, height=20, font=('Courier', 10))
        self.results_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Status bar
        self.status_var = tk.StringVar(value="Ready")
        status_bar = ttk.Label(main_frame, textvariable=self.status_var, relief=tk.SUNKEN)
        status_bar.grid(row=7, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(10, 0))
        
        # Example values
        self.set_example_values()
    
    def set_example_values(self):
        """Set example values for testing"""
        self.query_entry.insert(0, "loan")
        self.key1_combo.set("account_number")
        self.value1_entry.insert(0, "210612271")
    
    def clear_form(self):
        """Clear all form fields"""
        self.query_entry.delete(0, tk.END)
        self.key1_combo.set("")
        self.value1_entry.delete(0, tk.END)
        self.key2_combo.set("")
        self.value2_entry.delete(0, tk.END)
        self.results_text.delete(1.0, tk.END)
        self.status_var.set("Form cleared")
    
    def get_filters(self):
        """Get filters from the form"""
        filters = {}
        
        # Filter 1
        key1 = self.key1_combo.get().strip()
        value1 = self.value1_entry.get().strip()
        if key1 and value1:
            filters[key1] = value1
        
        # Filter 2
        key2 = self.key2_combo.get().strip()
        value2 = self.value2_entry.get().strip()
        if key2 and value2:
            filters[key2] = value2
        
        return filters
    
    def perform_search(self):
        """Perform the search operation"""
        if not self.indexer:
            messagebox.showerror("Error", "MongoDB not connected!")
            return
        
        # Disable search button during search
        self.search_button.config(state='disabled')
        self.status_var.set("Searching...")
        
        # Run search in separate thread to prevent UI freezing
        search_thread = threading.Thread(target=self._search_worker)
        search_thread.daemon = True
        search_thread.start()
    
    def _search_worker(self):
        """Worker method for search (runs in separate thread)"""
        try:
            query = self.query_entry.get().strip()
            filters = self.get_filters()
            search_type = self.search_type.get()
            
            # Perform search
            if search_type == "semantic":
                results = self.indexer.semantic_search(query, limit=20)
            else:
                results = self.indexer.search_documents(query, filters, limit=20)
            
            # Update UI in main thread
            self.root.after(0, self._update_results, results, query, filters, search_type)
            
        except Exception as e:
            self.root.after(0, self._show_error, str(e))
    
    def _update_results(self, results, query, filters, search_type):
        """Update results in the UI (runs in main thread)"""
        # Clear previous results
        self.results_text.delete(1.0, tk.END)
        
        # Format and display results
        output = []
        output.append(f"Search Results ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
        output.append("=" * 60)
        output.append(f"Query: '{query}'")
        output.append(f"Search Type: {search_type}")
        output.append(f"Filters: {filters}")
        output.append(f"Results Found: {len(results)}")
        output.append("=" * 60)
        output.append("")
        
        if results:
            for i, result in enumerate(results, 1):
                output.append(f"Result {i}:")
                output.append("-" * 20)
                
                # Display key information
                if 'account_number' in result:
                    output.append(f"Account Number: {result['account_number']}")
                if 'pdf_type' in result:
                    output.append(f"PDF Type: {result['pdf_type']}")
                if 'document_type' in result:
                    output.append(f"Document Type: {result['document_type']}")
                if 's3_key' in result:
                    output.append(f"S3 Key: {result['s3_key']}")
                if 'similarity_score' in result:
                    output.append(f"Similarity Score: {result['similarity_score']:.4f}")
                
                # Display metadata if available
                if 'metadata' in result and result['metadata']:
                    output.append("Metadata:")
                    for key, value in result['metadata'].items():
                        if isinstance(value, (str, int, float)) and str(value).strip():
                            output.append(f"  {key}: {value}")
                
                # Display text preview
                if 'text_content' in result:
                    preview = result['text_content'][:200] + "..." if len(result['text_content']) > 200 else result['text_content']
                    output.append(f"Text Preview: {preview}")
                
                output.append("")
        else:
            output.append("No results found.")
            output.append("")
            output.append("Try:")
            output.append("- Different search terms")
            output.append("- Removing filters")
            output.append("- Using semantic search for concept-based queries")
        
        # Display results
        self.results_text.insert(tk.END, "\n".join(output))
        
        # Re-enable search button and update status
        self.search_button.config(state='normal')
        self.status_var.set(f"Search completed - {len(results)} results found")
    
    def _show_error(self, error_message):
        """Show error message (runs in main thread)"""
        self.search_button.config(state='normal')
        self.status_var.set("Search failed")
        messagebox.showerror("Search Error", f"Search failed:\n{error_message}")
    
    def get_account_summary(self):
        """Get account summary for a specific account"""
        if not self.indexer:
            messagebox.showerror("Error", "MongoDB not connected!")
            return
        
        # Get account number from filters or ask user
        account_number = None
        if self.key1_combo.get() == "account_number" and self.value1_entry.get():
            account_number = self.value1_entry.get().strip()
        elif self.key2_combo.get() == "account_number" and self.value2_entry.get():
            account_number = self.value2_entry.get().strip()
        else:
            # Ask user for account number
            account_number = tk.simpledialog.askstring("Account Summary", 
                                                      "Enter Account Number:")
        
        if not account_number:
            return
        
        try:
            self.status_var.set("Getting account summary...")
            summary = self.indexer.get_account_summary(account_number)
            
            # Clear and display summary
            self.results_text.delete(1.0, tk.END)
            
            output = []
            output.append(f"Account Summary for: {account_number}")
            output.append("=" * 60)
            
            if summary.get('account_info'):
                account_info = summary['account_info']
                output.append("ACCOUNT INFORMATION:")
                output.append("-" * 30)
                for key, value in account_info.items():
                    if key != '_id' and value:
                        output.append(f"{key}: {value}")
                output.append("")
            
            if summary.get('documents'):
                output.append(f"DOCUMENTS ({len(summary['documents'])}):")
                output.append("-" * 30)
                for i, doc in enumerate(summary['documents'], 1):
                    output.append(f"{i}. {doc.get('pdf_type', 'Unknown')} - {doc.get('document_type', 'Unknown')}")
                    output.append(f"   S3 Key: {doc.get('s3_key', 'Unknown')}")
                    if doc.get('created_at'):
                        output.append(f"   Created: {doc['created_at']}")
                    output.append("")
            else:
                output.append("No documents found for this account.")
            
            self.results_text.insert(tk.END, "\n".join(output))
            self.status_var.set(f"Account summary loaded for {account_number}")
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to get account summary:\n{str(e)}")
            self.status_var.set("Account summary failed")

def main():
    """Main function to run the UI"""
    # Import tkinter.simpledialog for account summary input
    import tkinter.simpledialog
    
    root = tk.Tk()
    app = DocumentSearchUI(root)
    
    # Center the window
    root.update_idletasks()
    x = (root.winfo_screenwidth() // 2) - (root.winfo_width() // 2)
    y = (root.winfo_screenheight() // 2) - (root.winfo_height() // 2)
    root.geometry(f"+{x}+{y}")
    
    root.mainloop()

if __name__ == "__main__":
    main()