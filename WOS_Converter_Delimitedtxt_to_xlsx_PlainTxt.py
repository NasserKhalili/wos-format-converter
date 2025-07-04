import pandas as pd
import csv  
 
# ── CONFIG ─────────────────────────────────────────────────────────────────────
tabdelim_path    = "TabDelimited.txt"    # input #1: tab-delimited WOS file
excel_path       = "WOS.xlsx"            # output #1: Excel file
plaintext_path   = "PlainText.txt"       # output #2: Web of Science formatted text
# ────────────────────────────────────────────────────────────────────────────────

# 1) WOS short codes → Excel full headers
col_map = {
    "PT": "Publication Type",
    "AU": "Authors",
    "BA": "Book Authors",
    "BE": "Book Editors",
    "GP": "Book Group Authors",
    "AF": "Author Full Names",
    "BF": "Book Author Full Names",
    "CA": "Group Authors",
    "TI": "Article Title",
    "SO": "Source Title",
    "SE": "Book Series Title",
    "BS": "Book Series Subtitle",
    "LA": "Language",
    "DT": "Document Type",
    "CT": "Conference Title",
    "CY": "Conference Date",
    "CL": "Conference Location",
    "SP": "Conference Sponsor",
    "HO": "Conference Host",
    "DE": "Author Keywords",
    "ID": "Keywords Plus",
    "AB": "Abstract",
    "C1": "Addresses",
    "C3": "Affiliations",
    "RP": "Reprint Addresses",
    "EM": "Email Addresses",
    "RI": "Researcher Ids",
    "OI": "ORCIDs",
    "FG": "Funding Orgs",
    "FP": "Funding Name Preferred",
    "FX": "Funding Text",
    "CR": "Cited References",
    "NR": "Cited Reference Count",
    "TC": "Times Cited, WoS Core",
    "Z9": "Times Cited, All Databases",
    "U1": "180 Day Usage Count",
    "U2": "Since 2013 Usage Count",
    "PU": "Publisher",
    "PI": "Publisher City",
    "PA": "Publisher Address",
    "SN": "ISSN",
    "EI": "eISSN",
    "BN": "ISBN",
    "J9": "Journal Abbreviation",
    "JI": "Journal ISO Abbreviation",
    "PD": "Publication Date",
    "PY": "Publication Year",
    "VL": "Volume",
    "IS": "Issue",
    "PN": "Part Number",
    "SU": "Supplement",
    "SI": "Special Issue",
    "MA": "Meeting Abstract",
    "BP": "Start Page",
    "EP": "End Page",
    "AR": "Article Number",
    "DI": "DOI",
    "DL": "DOI Link",
    "D2": "Book DOI",
    "EA": "Early Access Date",
    "PG": "Number of Pages",
    "WC": "WoS Categories",
    "WE": "Web of Science Index",
    "SC": "Research Areas",
    "GA": "IDS Number",
    "PM": "Pubmed Id",
    "OA": "Open Access Designations",
    "HC": "Highly Cited Status",
    "HP": "Hot Paper Status",
    "DA": "Date of Export",
    "UT": "UT (Unique WOS ID)"
}

# 2) Read the TabDelimited.txt
df = pd.read_csv(
    tabdelim_path,
    sep="\t",
    dtype=str,
    engine="python",
    quoting=csv.QUOTE_NONE,
    on_bad_lines="warn",    # or "skip" to drop bad lines silently
).fillna("")

# 3) Rename columns back to Excel headers
df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

# 4) Save to Excel
df.to_excel(excel_path, index=False)
print(f"→ Excel file written to {excel_path}")

# 5) Convert to PlainText.txt (like before)
def format_list_field(s, tag):
    parts = [p.strip() for p in s.split(";") if p.strip()]
    lines = [f"{tag} {parts[0]}"] if parts else []
    lines += [f"   {p}" for p in parts[1:]]
    return lines

# Use short WOS tags (reverse map)
reverse_col_map = {v: k for k, v in col_map.items()}
tag_order = [t for t in reverse_col_map.values() if t in df.columns]

lines = ["FN Clarivate Analytics Web of Science", "VR 1.0"]

for _, row in df.iterrows():
    record = []
    for full_name, val in row.items():
        tag = reverse_col_map.get(full_name)
        if not tag or pd.isna(val) or val == "":
            continue
        if tag in {"AU", "AF", "C1", "CR"}:
            record += format_list_field(val, tag)
        else:
            record.append(f"{tag} {val}")
    if record:
        lines.extend(record)
        lines.append("ER")
        lines.append("")  # blank line between records

with open(plaintext_path, "w", encoding="utf-8") as f:
    f.write("\n".join(lines))

print(f"→ Plain-text tagged file written to {plaintext_path}")
