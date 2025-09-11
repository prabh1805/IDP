"""
Split PDF at every page that introduces a *new* account number.
Account format:
    ACCOUNT NUMBER:
    210863254
"""

import fitz  # PyMuPDF
from pathlib import Path

INPUT_PDF = Path(r"./combinedPdf.pdf")   # <--- change to your file

def split_on_new_account(pdf_path: Path):
    doc = fitz.open(pdf_path)
    seen_accounts = set()

    current_start = 0      # first page of current chunk
    part_no = 1

    for page_index in range(doc.page_count):
        text = doc[page_index].get_text("text")
        account = extract_account_number(text)

        if account and account not in seen_accounts:
            # split at this page
            if page_index > current_start:            # avoid empty chunk
                save_part(doc, current_start, page_index, part_no)
                part_no += 1
            current_start = page_index
            seen_accounts.add(account)

    # save whatever is left
    save_part(doc, current_start, doc.page_count, part_no)
    doc.close()

def extract_account_number(text: str) -> str | None:
    """
    Return the account number if the page contains the marker,
    otherwise None.
    """
    lines = [ln.strip() for ln in text.splitlines()]
    for idx, line in enumerate(lines):
        if line.rstrip(":") == "ACCOUNT NUMBER" and idx + 1 < len(lines):
            return lines[idx + 1].strip()
    return None

def save_part(doc, start: int, end: int, part_no: int):
    out = fitz.open()
    out.insert_pdf(doc, from_page=start, to_page=end - 1)
    filename = f"part_{part_no:03d}.pdf"
    out.save(filename)
    out.close()
    print(f"Saved {filename}  (pages {start+1}-{end})")

if __name__ == "__main__":
    split_on_new_account(INPUT_PDF)