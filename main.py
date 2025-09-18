import os
import re
import csv
import ftplib
import unicodedata
import pandas as pd
from google.cloud import storage
from flask import jsonify

# ============================
# CONFIG & ENV
# ============================

bucket_name = os.environ.get("BUCKET_NAME")       # e.g., "sitemaps.leeladiamond.com"
bucket_folder = os.environ.get("BUCKET_FOLDER")   # e.g., "Googlefinal"

local_output_directory = os.environ.get("LOCAL_OUTPUT_DIRECTORY", "/tmp/output")
ftp_download_dir = os.environ.get("FTP_DOWNLOAD_DIR", "/tmp/ftp")

os.makedirs(local_output_directory, exist_ok=True)
os.makedirs(ftp_download_dir, exist_ok=True)

FTP_SERVER = os.environ.get("FTP_SERVER", "ftp.nivoda.net")
FTP_PORT = int(os.environ.get("FTP_PORT", "21"))
FTP_USERNAME = os.environ.get("FTP_USERNAME", "leeladiamondscorporate@gmail.com")
FTP_PASSWORD = os.environ.get("FTP_PASSWORD", "1yH£lG4n0Mq")

ftp_files = {
    "natural":   {"remote_filename": "Leela Diamond_natural.csv",   "local_path": os.path.join(ftp_download_dir, "Natural.csv")},
    "lab_grown": {"remote_filename": "Leela Diamond_labgrown.csv",  "local_path": os.path.join(ftp_download_dir, "Labgrown.csv")},
    "gemstone":  {"remote_filename": "Leela Diamond_gemstones.csv", "local_path": os.path.join(ftp_download_dir, "gemstones.csv")},
}

# ============================
# CONSTANTS
# ============================

SHAPE_IMAGE_URLS = {
    "ASSCHER": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/ASSCHER.jpg",
    "BAGUETTE": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/BAGUETTE.jpg",
    "BRIOLETTE": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/BRIOLETTE.webp",
    "BULLET": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/BULLET.jpeg",
    "CALF": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/CALF.webp",
    "CUSHION": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/CUSHION.jpg",
    "CUSHION BRILLIANT": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/CUSHION%20BRILLIANT.webp",
    "CUSHION MODIFIED": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/CUSHION%20MODIFIED.jpg",
    "EMERALD": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/EMERALD.jpg",
    "EUROPEAN CUT": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/EUROPEAN%20CUT.webp",
    "HALF MOON": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/HALF%20MOON.jpg",
    "HEART": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/HEART.png",
    "HEPTAGONAL": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/HEPTAGONAL.webp",
    "HEXAGONAL": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/HEXAGONAL.webp",
    "KITE": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/KITE.jpg",
    "LOZENGE": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/LOZENGE.jpg",
    "MARQUISE": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/MARQUISE.jpg",
    "NONAGONAL": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/NONAGONAL.jpg",
    "OCTAGONAL": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/OCTAGONAL.jpg",
    "OLD MINER": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/OLD%20MINER.webp",
    "OTHER": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/OTHER.webp",
    "OVAL": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/OVAL.webp",
    "PEAR": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/PEAR.jpg",
    "PEAR MODIFIED BRILLIANT": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/PEAR%20MODIFIED%20BRILLIANT.webp",
    "PENTAGONAL": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/PENTAGONAL.jpg",
    "PRINCESS": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/PRINCESS.jpg",
    "RADIANT": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/RADIANT.jpg",
    "RECTANGULAR": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/RECTANGULAR.webp",
    "RHOMBOID": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/RHOMBOID.jpg",
    "ROSE": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/ROSE.webp",
    "ROUND": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/ROUND.png",
    "SHIELD": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/SHIELD.webp",
    "SQUARE": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/SQUARE%20EMERALD.webp",
    "SQUARE EMERALD": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/SQUARE%20EMERALD.webp",
    "SQUARE RADIANT": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/SQUARE%20RADIANT.webp",
    "STAR": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/STAR.jpg",
    "TAPERED BAGUETTE": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/TAPERED%20BAGUETTE.jpg",
    "TRAPEZOID": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/TRAPEZOID.jpg",
    "TRIANGULAR": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/TRIANGULAR.webp",
    "TRILLIANT": "https://storage.googleapis.com/sitemaps/leeladiamond.com/shapes/TRILLIANT.jpg",
}

# ============================
# HELPERS
# ============================

def usd_to_cad_rate() -> float:
    try:
        return float(os.environ.get("USD_TO_CAD", "1.41"))
    except Exception:
        return 1.41

def safe_read_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    try:
        return pd.read_csv(path, dtype=str, low_memory=False).fillna('')
    except UnicodeDecodeError:
        return pd.read_csv(path, dtype=str, low_memory=False, encoding="utf-8-sig").fillna('')

def parse_money_to_float(series: pd.Series) -> pd.Series:
    """Convert money-like strings to floats. Handles commas, currency words/symbols."""
    def _clean(v):
        if pd.isna(v):
            return None
        s = str(v).strip()
        s = s.replace(",", "")
        s = re.sub(r"(usd|cad|inr|aud|eur|£|€|\$)", "", s, flags=re.IGNORECASE)
        s = re.sub(r"[^0-9.\-]", "", s)
        if s.count(".") > 1:
            parts = s.split(".")
            s = parts[0] + "." + "".join(parts[1:])
        try:
            return float(s) if s != "" else None
        except Exception:
            return None
    out = series.map(_clean)
    return pd.to_numeric(out, errors="coerce")

def build_measurements(row) -> str:
    L, W, H = row.get("length"), row.get("width"), row.get("height")
    if (L and W and H):
        return f"{L}-{W}x{H} mm"
    if (L and W):
        return f"{L}x{W} mm"
    return ""

def first_col(df: pd.DataFrame, names: list) -> str | None:
    for n in names:
        if n in df.columns:
            return n
    return None

def _extract_image_series(df: pd.DataFrame, col_name: str) -> pd.Series:
    """
    Always return a pandas Series of image URLs (or NaN/''), never an ndarray.
    Handles cases where str.extract could yield ndarray/DataFrame.
    """
    if col_name not in df.columns:
        return pd.Series([""] * len(df), index=df.index)

    # Extract direct URL with known image extensions
    extracted = df[col_name].astype(str).str.extract(
        r'(https?://[^\s",>]+?\.(?:jpg|jpeg|png|webp))', expand=True
    )

    # If expand=True, 'extracted' is a DataFrame with a single column at index 0
    if isinstance(extracted, pd.DataFrame):
        series = extracted.iloc[:, 0]
    else:
        # In weird pandas versions/inputs, guard and coerce to Series
        series = pd.Series(extracted, index=df.index)

    # Ensure it's a Series aligned with df and supports fillna
    series = pd.Series(series, index=df.index)
    return series

# ============================
# FTP
# ============================

def download_file_from_ftp(remote_filename, local_path):
    try:
        with ftplib.FTP() as ftp:
            ftp.connect(FTP_SERVER, FTP_PORT, timeout=30)
            ftp.login(FTP_USERNAME, FTP_PASSWORD)
            with open(local_path, 'wb') as f:
                ftp.retrbinary(f"RETR {remote_filename}", f.write)
        print(f"[FTP] {remote_filename} → {local_path}")
    except Exception as e:
        print(f"[FTP][ERROR] {remote_filename}: {e}")

def download_all_files():
    for _, file_info in ftp_files.items():
        download_file_from_ftp(file_info["remote_filename"], file_info["local_path"])

# ============================
# PROCESSING
# ============================

def process_files_to_cad(files_to_load, output_file):
    try:
        all_frames = []

        for product_type, file_info in files_to_load.items():
            input_file = file_info["file_path"]
            if not os.path.exists(input_file):
                print(f"[WARN] Missing input file: {input_file}")
                continue

            df = safe_read_csv(input_file)

            # Ensure these columns exist (based on your provided schema)
            required_cols = [
                "shape","carats","col","clar","cut","pol","symm","flo","floCol",
                "length","width","height","depth","table","culet","lab","girdle",
                "ReportNo","image","video","pdf","markupPrice","markupCurrency",
                "price","pricePerCarat","mineOfOrigin","canadaMarkEligible","isReturnable",
                "deliveredPrice","minDeliveryDays","maxDeliveryDays","diamondId","stockId","ID"
            ]
            for col in required_cols:
                if col not in df.columns:
                    df[col] = ""

            # SHAPE normalization
            df["shape"] = df["shape"].astype(str).str.strip().str.upper()

            # --------- IMAGE handling (fixed) ----------
            img_series = _extract_image_series(df, "image")
            # Use shape fallback where missing
            df["image_link"] = img_series
            df["image_link"] = df["image_link"].fillna('')
            df.loc[df["image_link"] == "", "image_link"] = df["shape"].map(
                lambda s: SHAPE_IMAGE_URLS.get(str(s).upper(), "")
            )

            # --------- PRICE handling ----------
            usd_series = parse_money_to_float(df["markupPrice"])
            curr = df["markupCurrency"].astype(str).str.strip().str.upper().replace({"": "USD"})
            rate = usd_to_cad_rate()

            def _to_cad(p, c):
                if pd.isna(p):
                    return 0.0
                if c == "CAD":
                    return round(float(p), 2)
                # treat everything else as USD
                return round(float(p) * rate, 2)

            df["price_cad"] = [ _to_cad(p, c) for p, c in zip(usd_series, curr) ]
            df["price"] = pd.to_numeric(df["price_cad"], errors="coerce").fillna(0.0).map(lambda x: f"{x:.2f} CAD")
            df["markupCurrency"] = "CAD"

            priced_rows = int((pd.to_numeric(df["price_cad"], errors="coerce").fillna(0.0) > 0).sum())
            print(f"[PRICE][{product_type}] rows={len(df)} | priced(>0 CAD)={priced_rows}")

            # --------- IDs / meta ----------
            df["ReportNo"] = df["ReportNo"].astype(str).str.strip()
            # fallback to diamondId if ReportNo missing
            df["id"] = (df["ReportNo"].where(df["ReportNo"] != "", df["diamondId"].astype(str)) + "CA").fillna("")

            df["availability"] = "in_stock"
            df["google_product_category"] = "188"
            df["brand"] = "Leela Diamonds"
            df["mpn"] = df["id"]
            df["condition"] = "new"
            df["color"] = "white/yellow/rose gold"
            df["age_group"] = "adult"
            df["gender"] = "unisex"

            # --------- Titles / Descriptions / Links ----------
            def natural_title(r):
                return f"{r.get('shape','')}-{r.get('carats','')} Carats-{r.get('col','')} Color-{r.get('clar','')} Clarity-{r.get('lab','')} Certified-{r.get('shape','')}-Natural Diamond"

            def natural_desc(r):
                meas = build_measurements(r)
                return (
                    f"Natural {r.get('shape','')} diamond: {r.get('carats','')} carats, "
                    f"{r.get('col','')} color, {r.get('clar','')} clarity. "
                    f"Measurements: {meas}. "
                    f"Cut: {r.get('cut','')}, Polish: {r.get('pol','')}, Symmetry: {r.get('symm','')}, "
                    f"Table: {r.get('table','')}%, Depth: {r.get('depth','')}%, Fluorescence: {r.get('flo','') or r.get('floCol','')}. "
                    f"{r.get('lab','')} certified."
                )

            def natural_link(r):
                cert = str(r.get("ReportNo","")).strip()
                return f"https://leeladiamond.com/pages/natural-diamond-catalog?id={cert}"

            def lab_title(r):
                return f"{r.get('shape','')}-{r.get('carats','')} Carats-{r.get('col','')} Color-{r.get('clar','')} Clarity-{r.get('lab','')} Certified-{r.get('shape','')}-Lab Grown Diamond"

            def lab_desc(r):
                meas = build_measurements(r)
                return (
                    f"Lab-grown {r.get('shape','')} diamond: {r.get('carats','')} carats, "
                    f"{r.get('col','')} color, {r.get('clar','')} clarity. "
                    f"Measurements: {meas}. "
                    f"Cut: {r.get('cut','')}, Polish: {r.get('pol','')}, Symmetry: {r.get('symm','')}, "
                    f"Table: {r.get('table','')}%, Depth: {r.get('depth','')}%, Fluorescence: {r.get('flo','') or r.get('floCol','')}. "
                    f"{r.get('lab','')} certified."
                )

            def lab_link(r):
                return (
                    "https://leeladiamond.com/pages/lab-grown-diamonds/"
                    f"{str(r.get('shape','')).strip().lower()}-"
                    f"{str(r.get('carats','')).replace('.', '-')}-carat-"
                    f"{str(r.get('col','')).strip().lower()}-color-"
                    f"{str(r.get('clar','')).strip().lower()}-clarity-"
                    f"{str(r.get('lab','')).strip().lower()}-certified-"
                    f"{str(r.get('ReportNo','')).strip()}"
                )

            def gem_title(r):
                return f"{r.get('shape','')} {r.get('col','')} Gemstone - {r.get('carats','')} Carats, {r.get('clar','')} Clarity, {r.get('cut','')} Cut, {r.get('lab','')} Certified"

            def gem_desc(r):
                meas = build_measurements(r)
                return (
                    f"{r.get('shape','')} gemstone: {r.get('carats','')} carats, "
                    f"color: {r.get('col','')}, clarity: {r.get('clar','')}, cut: {r.get('cut','')}. "
                    f"Measurements: {meas}. Lab: {r.get('lab','')}."
                )

            def gem_link(r):
                cert = str(r.get("ReportNo","")).strip()
                return f"https://leeladiamond.com/pages/gemstone-catalog?id={cert}"

            if product_type == "natural":
                df["title"] = df.apply(natural_title, axis=1)
                df["description"] = df.apply(natural_desc, axis=1)
                df["link"] = df.apply(natural_link, axis=1)
            elif product_type == "lab_grown":
                df["title"] = df.apply(lab_title, axis=1)
                df["description"] = df.apply(lab_desc, axis=1)
                df["link"] = df.apply(lab_link, axis=1)
            elif product_type == "gemstone":
                df["title"] = df.apply(gem_title, axis=1)
                df["description"] = df.apply(gem_desc, axis=1)
                df["link"] = df.apply(gem_link, axis=1)
            else:
                print(f"[WARN] Unknown product type: {product_type}")
                continue

            final_cols = [
                "id","title","description","link","image_link","availability","price",
                "google_product_category","brand","mpn","condition","color","age_group","gender"
            ]
            for c in final_cols:
                if c not in df.columns:
                    df[c] = ""
            all_frames.append(df[final_cols])

        if not all_frames:
            raise RuntimeError("No input files were processed; combined feed not created.")

        combined = pd.concat(all_frames, ignore_index=True)
        combined.to_csv(
            output_file,
            index=False,
            quoting=csv.QUOTE_MINIMAL,
            quotechar='"',
            escapechar='\\'
        )
        print(f"[OK] Combined feed → {output_file}")

    except Exception as e:
        print(f"[PROCESS][ERROR] {e}")

# ============================
# GCS UPLOAD
# ============================

def upload_files_to_bucket(bucket_name, bucket_folder, local_directory):
    try:
        if not bucket_name or not bucket_folder:
            print("[GCS] Skipping upload: BUCKET_NAME or BUCKET_FOLDER not set.")
            return
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        for file_name in os.listdir(local_directory):
            file_path = os.path.join(local_directory, file_name)
            if os.path.isfile(file_path):
                destination_blob_name = f"{bucket_folder.rstrip('/')}/{file_name}"
                blob = bucket.blob(destination_blob_name)
                if file_name.lower().endswith('.csv'):
                    blob.content_type = 'text/csv'
                blob.upload_from_filename(file_path)
                print(f"[GCS] Uploaded {file_name} → {destination_blob_name}")
    except Exception as e:
        print(f"[GCS][ERROR] {e}")

# ============================
# ORCHESTRATION
# ============================

def run_workflow():
    download_all_files()

    files_to_load = {
        "natural":   {"file_path": os.path.join(ftp_download_dir, "Natural.csv")},
        "lab_grown": {"file_path": os.path.join(ftp_download_dir, "Labgrown.csv")},
        "gemstone":  {"file_path": os.path.join(ftp_download_dir, "gemstones.csv")},
    }
    out_file = os.path.join(local_output_directory, "combined_google_merchant_feed.csv")

    process_files_to_cad(files_to_load, out_file)
    upload_files_to_bucket(bucket_name, bucket_folder, local_output_directory)
    return "Workflow executed successfully."

# ============================
# CLOUD FUNCTION ENTRY
# ============================

def cloud_function_entry(request):
    try:
        result = run_workflow()
        return jsonify({"status": "success", "message": result})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# ============================
# LOCAL RUN
# ============================

if __name__ == "__main__":
    print(run_workflow())
