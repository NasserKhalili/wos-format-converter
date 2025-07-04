import pandas as pd
import csv 

# ── CONFIG ─────────────────────────────────────────────────────────────────────
excel_path = "merged_WOS_format.xlsx"
tabdelim_path = "TabDelimited_Filtered3.txt"
plaintext_path = "PlainText_Filtered3.txt"

# 1) Excel header → WOS tag
col_map = {
    "Publication Type": "PT",
    "Authors": "AU",
    "Book Authors": "BA",
    "Book Editors": "BE",
    "Book Group Authors": "ZA",
    "Author Full Names": "AF",
    "Book Author Full Names": "ZF",
    "Group Authors": "CA",
    "Article Title": "TI",
    "Source Title": "SO",
    "Book Series Title": "SE",
    "Book Series Subtitle": "BS",
    "Language": "LA",
    "Document Type": "DT",
    "Conference Title": "CT",
    "Conference Date": "CY",
    "Conference Location": "CL",
    "Conference Sponsor": "SP",
    "Conference Host": "HO",
    "Author Keywords": "DE",
    "Keywords Plus": "ID",
    "Abstract": "AB",
    "Addresses": "C1",
    "Affiliations": "C3",
    "Reprint Addresses": "RP",
    "Email Addresses": "EM",
    "Researcher Ids": "RI",
    "ORCIDs": "OI",
    "Funding Orgs": "FG",
    "Funding Name Preferred": "FP",
    "Funding Text": "FX",
    "Cited References": "CR",
    "Cited Reference Count": "NR",
    "Times Cited, WoS Core": "TC",
    "Times Cited, All Databases": "Z9",
    "180 Day Usage Count": "U1",
    "Since 2013 Usage Count": "U2",
    "Publisher": "PU",
    "Publisher City": "PI",
    "Publisher Address": "PA",
    "ISSN": "SN",
    "eISSN": "EI",
    "ISBN": "BN",
    "Journal Abbreviation": "J9",
    "Journal ISO Abbreviation": "JI",
    "Publication Date": "PD",
    "Publication Year": "PY",
    "Volume": "VL",
    "Issue": "IS",
    "Part Number": "PN",
    "Supplement": "SU",
    "Special Issue": "SI",
    "Meeting Abstract": "MA",
    "Start Page": "BP",
    "End Page": "EP",
    "Article Number": "AR",
    "DOI": "DI",
    "DOI Link": "DL",
    "Book DOI": "D2",
    "Early Access Date": "EA",
    "Number of Pages": "PG",
    "WoS Categories": "WC",
    "Web of Science Index": "WE",
    "Research Areas": "SC",
    "IDS Number": "GA",
    "Pubmed Id": "PM",
    "Open Access Designations": "OA",
    "Highly Cited Status": "HC",
    "Hot Paper Status": "HP",
    "Date of Export": "DA",
    "UT (Unique WOS ID)": "UT"
}

reverse_map = {v: k for k, v in col_map.items()}

# 2) Load Excel and rename to tags
df = pd.read_excel(excel_path, dtype=str)
df.rename(columns={full: tag for full, tag in col_map.items() if full in df.columns},
          inplace=True)

# 3) Ensure UT column exists and fill placeholders
if "UT" not in df.columns:
    df["UT"] = [f"ID{idx:06d}" for idx in range(len(df))]
else:
    def make_ut(row, idx):
        ut = row.get("UT", "")
        if pd.notna(ut) and str(ut).strip():
            return str(ut).strip()
        doi = row.get("DI", "")
        if pd.notna(doi) and str(doi).strip():
            return f"SCOPUS:{str(doi).strip()}"
        return f"ID{idx:06d}"
    df["UT"] = [make_ut(row, i) for i, row in df.iterrows()]

# 4) Skip deduplication (already done in WOS_Scopus_Merger.py)
print(f"Rows: {len(df)}")

# 5) Clean newlines in all fields
def clean_val(x):
    return "" if pd.isna(x) else str(x).replace("\r", " ").replace("\n", " ").strip()
df = df.applymap(clean_val)

# 6) Normalize list fields except addresses/affiliations, CR, AU, and AF
for col in ["DE", "ID"]:
    if col in df.columns:
        df[col] = df[col].str.replace(",", ";")

# 7) Write Tab-Delimited for VOSviewer (quote non-numeric fields)
df.to_csv(
    tabdelim_path,
    sep="\t",
    index=False,
    encoding="utf-8",
    quoting=csv.QUOTE_NONNUMERIC,
    escapechar="\\"
)
print(f"Tab-delimited written: {tabdelim_path}")

# 8) Generate PlainText for Biblioshiny
tags = [
    "PT", "AU", "AF", "TI", "SO", "LA", "DT", "DE", "ID", "AB", "C1", "C3", "RP", "EM",
    "RI", "OI", "CR", "NR", "TC", "Z9", "U1", "U2", "PU", "PI", "PA", "SN", "EI", "J9", "JI",
    "PD", "PY", "DI", "EA", "PG", "WC", "WE", "SC", "GA", "UT", "OA", "DA"
]

lines = ["FN Clarivate Analytics Web of Science", "VR 1.0"]

for idx, row in df.iterrows():
    if not row.get("TI", "").strip():
        ab = row.get("AB", "")
        if ab:
            df.at[idx, "TI"] = ab[:80].strip()
            row["TI"] = df.at[idx, "TI"]
    rec = [f"PT {row.get('PT', '').strip()}"]
    for tag in tags[1:]:
        val = row.get(tag, "")
        if not val:
            continue
        if tag in {"AU", "AF", "C1", "CR"}:
            parts = [p.strip() for p in val.split(";") if p.strip()]
            rec.append(f"{tag} {parts[0]}")
            rec += [f"   {p}" for p in parts[1:]]
        else:
            rec.append(f"{tag} {val}")
    rec.append("ER")
    lines.extend(rec)
    lines.append("")

with open(plaintext_path, "w", encoding="utf-8") as f:
    f.write("\n".join(lines))

print(f"Plain-text written: {plaintext_path}")
print(f"Total records written: {len(df)}")
