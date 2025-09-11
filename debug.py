# debug.py
import fitz          # PyMuPDF
import sys
from pathlib import Path

pdf = Path("./combinedPdf.pdf")     # <-- make sure this file is in the same folder
print("Python executable :", sys.executable)
print("PyMuPDF version   :", fitz.__doc__.split()[1])  # quick version check
print("File exists?      :", pdf.exists())

try:
    doc = fitz.open(pdf)
    print("Pages in PDF      :", doc.page_count)
    page = doc[0]
    text = page.get_text()
    print("Text from page 1  :", repr(text[:200]))  # first 200 chars
    doc.close()
except Exception as e:
    print("ERROR ->", e, type(e))