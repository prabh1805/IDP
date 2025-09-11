import re
import boto3

textract = boto3.client("textract")

ACCOUNT_LABELS = {
    "account number", "account no", "account #", "acct number", "acct no", "acct #"
}

def _normalise_key(text: str) -> str:
    """'Account  Number :' -> 'accountnumber'"""
    return re.sub(r"[^a-z0-9]", "", text.lower())

def _get_kv_map(blocks):
    """Return dict {normalised_key: VALUE block} built from Textract FORMS."""
    kv = {}
    for block in blocks:
        if block["BlockType"] == "KEY_VALUE_SET" and "KEY" in block["EntityTypes"]:
            key_child = None
            val_child = None
            for rel in block.get("Relationships", []):
                if rel["Type"] == "CHILD":
                    key_child = rel["Ids"]
                elif rel["Type"] == "VALUE":
                    val_child = rel["Ids"]
            if key_child and val_child:
                key_text = " ".join(
                    b["Text"] for b in blocks
                    if b["Id"] in key_child and b["BlockType"] == "WORD"
                )
                val_text = " ".join(
                    b["Text"] for b in blocks
                    if b["Id"] in val_child and b["BlockType"] == "WORD"
                )
                kv[_normalise_key(key_text)] = val_text
    return kv

def find_account_numbers(image_bytes: bytes):
    """
    1. Try Textract FORMS first (handles multi-line key/value).
    2. Fallback to plain regex on raw text.
    Returns set of unique account numbers.
    """
    # --- 1. FORMS call ---
    resp = textract.analyze_document(
        Document={"Bytes": image_bytes},
        FeatureTypes=["FORMS"]
    )
    kv = _get_kv_map(resp["Blocks"])

    accounts = set()
    for k, v in kv.items():
        if any(label in k for label in ACCOUNT_LABELS):
            # keep only digits
            digits = re.sub(r"\D", "", v)
            if 6 <= len(digits) <= 20:
                accounts.add(digits)

    # --- 2. Fallback regex on raw text ---
    if not accounts:
        raw_text = " ".join(
            b["Text"] for b in resp["Blocks"]
            if b["BlockType"] == "LINE"
        )
        accounts = set(re.findall(r"(?i)account\s*(?:#|no|number)\s*[:.-]?\s*(\d{6,20})", raw_text))

    return accounts

if __name__ == "__main__":
    """
    CLI:  python find_account.py
    Processes '/Multiple set of Pdfs.pdf' and prints
    {
      "xyzz": {
          "extraction": "1-2",
          "attachments": "3-5"
      },
      "ab12": {
          "extraction": "6",
          "attachments": "7-10"
      }
    }
    """
    import sys
    from io import BytesIO
    from pathlib import Path
    import pypdfium2 as pdfium
    import json

    file_path = Path('./Multiple set of Pdfs.pdf')
    if not file_path.exists():
        print("File not found")
        sys.exit(1)

    # ---- small helpers ----
    def _png_from_page(page):
        bitmap = page.render(scale=2)
        buf = BytesIO()
        bitmap.to_pil().save(buf, format='PNG')
        buf.seek(0)
        return buf.getvalue()

    def _range_str(pages):
        """[1,2,3] -> '1-3'  |  [6] -> '6'"""
        if not pages:
            return ""
        return f"{min(pages)}-{max(pages)}" if len(pages) > 1 else str(pages[0])

    # ---- main loop ----
    pdf = pdfium.PdfDocument(file_path.read_bytes())
    out = {}                      # final result
    current_acct = None           # account number in force
    extraction_pages = []         # pages where we *saw* the account number
    attachment_pages = []         # trailing pages for that account

    for idx, page in enumerate(pdf, start=1):
        png_bytes = _png_from_page(page)
        accounts_on_page = find_account_numbers(png_bytes)

        # ---- 1. new account detected ----
        if accounts_on_page and accounts_on_page != {current_acct}:
            # flush previous account block
            if current_acct is not None:
                out[current_acct] = {
                    "extraction": _range_str(extraction_pages),
                    "attachments": _range_str(attachment_pages)
                }
            # start new account
            current_acct = accounts_on_page.pop()   # take first new number
            extraction_pages = [idx]
            attachment_pages = []
            print(f"New account {current_acct} starts at page {idx}")

        # ---- 2. same account continues ----
        elif current_acct is not None:
            if accounts_on_page:                  # same number again
                extraction_pages.append(idx)
            else:                                 # no number → attachment
                attachment_pages.append(idx)

        # ---- 3. no account seen yet ----
        else:
            print(f"Page {idx}: no account number found – skipping")

    # ---- 4. flush last account ----
    if current_acct is not None:
        out[current_acct] = {
            "extraction": _range_str(extraction_pages),
            "attachments": _range_str(attachment_pages)
        }

    print(json.dumps(out, indent=2))