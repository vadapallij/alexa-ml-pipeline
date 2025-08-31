import os, glob, re
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()
from src.utils.io_s3 import upload_file_to_s3
from src.utils.io_adls import upload_file_to_adls

AWS_REGION = os.getenv("AWS_REGION", "<REGION>")
S3_BUCKET  = os.getenv("S3_BUCKET", "alexa-ml-<ACCOUNT_ID>-<REGION>")
AZ_STORAGE_ACCOUNT = os.getenv("AZ_STORAGE_ACCOUNT", "<STORAGE_ACCOUNT>")
AZ_ADLS_CONTAINER  = os.getenv("AZ_ADLS_CONTAINER", "alexa-ml14")
AZ_SAS             = os.getenv("AZ_SAS", "")

RAW_DIR = Path("data/raw/kaggle_alexa")
PROC_DIR = Path("data/processed")
PROC_DIR.mkdir(parents=True, exist_ok=True)

def to_snake(name: str) -> str:
    name = name.strip().replace("-", "_").replace(" ", "_")
    name = re.sub(r"[^0-9a-zA-Z_]", "_", name)
    name = re.sub(r"_+", "_", name).lower()
    return name

def find_table_file(path: Path):
    # Accept common text table types
    patterns = ["*.csv", "*.tsv", "*.txt"]
    files = []
    for pat in patterns:
        files.extend(glob.glob(str(path / pat)))
    if not files:
        raise FileNotFoundError(f"No CSV/TSV/TXT found in {path}")
    files.sort(key=lambda p: os.path.getsize(p), reverse=True)
    return files[0]

def read_table_auto(path: str):
    """
    Auto-detect delimiter: pandas can infer when sep=None (engine='python').
    Works for CSV and TSV.
    """
    try:
        df = pd.read_csv(path, encoding="utf-8", sep=None, engine="python")
        return df
    except UnicodeDecodeError:
        # Fallback with errors='ignore' if weird chars
        df = pd.read_csv(path, encoding="utf-8", sep=None, engine="python", on_bad_lines="skip")
        return df

def main():
    table_path = find_table_file(RAW_DIR)
    print(f"Found table: {table_path}")

    df = read_table_auto(table_path)
    # Standardize columns
    df.columns = [to_snake(c) for c in df.columns]

    # Coerce common types
    for c in df.columns:
        if c.endswith("rating") or c in {"rating", "stars"}:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        if c in {"date", "review_date", "timestamp"}:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    # Drop fully empty rows and duplicates
    df = df.dropna(how="all").drop_duplicates()

    # Detect a text column
    text_col_guess = None
    for guess in ["verified_reviews","review_text","text","content","feedback","reviews","summary","body","comment"]:
        if guess in df.columns:
            text_col_guess = guess; break
    if text_col_guess is None:
        raise ValueError(f"Could not find a text column in {table_path}. Columns example: {list(df.columns)[:20]}")

    out_path = PROC_DIR / "alexa_reviews_clean.csv"
    df.to_csv(out_path, index=False)

    # Upload RAW + PROCESSED to S3
    # Keep original extension for raw key
    raw_filename = os.path.basename(table_path)
    raw_key = f"data/raw/kaggle_alexa/{raw_filename}"
    proc_key = "data/processed/alexa_reviews_clean.csv"
    s3_raw_uri  = upload_file_to_s3(table_path, S3_BUCKET, raw_key, region=AWS_REGION)
    s3_proc_uri = upload_file_to_s3(str(out_path), S3_BUCKET, proc_key, region=AWS_REGION)

    # Upload to ADLS
    adls_raw_path  = f"data/raw/kaggle_alexa/{raw_filename}"
    adls_proc_path = "data/processed/alexa_reviews_clean.csv"
    adls_raw_uri  = upload_file_to_adls(table_path, AZ_STORAGE_ACCOUNT, AZ_ADLS_CONTAINER, adls_raw_path, AZ_SAS)
    adls_proc_uri = upload_file_to_adls(str(out_path), AZ_STORAGE_ACCOUNT, AZ_ADLS_CONTAINER, adls_proc_path, AZ_SAS)

    print("OK:")
    print("  S3 RAW:", s3_raw_uri)
    print("  S3 PROC:", s3_proc_uri)
    print("  ADLS RAW:", adls_raw_uri)
    print("  ADLS PROC:", adls_proc_uri)
    print("  Columns:", list(df.columns)[:20])

if __name__ == "__main__":
    main()
