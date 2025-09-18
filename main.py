import os
import re
import csv
import ftplib
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
    "CUSHION": "https://storage.googleapis.com/sitemaps/leeladiamond.com/shapes/CUSHION.jpg",
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
    "SQUARE EMERALD": "https://storage.googleapis.com/sitemaps/leeladiamond.com/shapes/SQUARE%20EMERALD.webp",
    "SQUARE RADIANT": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/SQUARE%20RADIANT.webp",
    "STAR": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/STAR.jpg",
    "TAPERED BAGUETTE": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/TAPERED%20BAGUETTE.jpg",
    "TRAPEZOID": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/TRAPEZOID.jpg",
    "TRIANGULAR": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/TRIANGULAR.webp",
    "TRILLIANT": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/TRILLIANT.jpg",
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

def extract_image_series(df: pd.DataFrame, col_name: str) -> pd.Series:
    """Always return a Series from image column (never ndarray), then we can fillna."""
    if col_name not in df.columns:
        return pd.Series([""] * len(df), index=df.index)
    extracted = df[col_name].astype(str).str.extract(
        r'(https?://[^\s",>]+?\.(?:jpg|jpeg|png|webp))', expand=True
    )
    if isinstance(extracted, pd.DataFrame):
        series = extracted.iloc[:, 0]
    else:
        series = pd.Series(extracted, index=df.index)
    return pd.Series(series, index=df.index)

def get_first_col(df: pd.DataFrame, names: list) -> str | None:
    for n in names:
        if n in df.columns:
            return n
    return None

def fallback_measurements(row) -> str:
    L = row.get("length") or row.get("Length")
    W = row.get("width")  or row.get("Width")
    H = row.get("height") or row.get("Height") or row.get("depth") or row.get("Depth")
    if L and W and H: return f"{L}-{W}x{H} mm"
    if L and W:       return f"{L}x{W} mm"
    return ""

def compute_price_cad(df: pd.DataFrame, product_type: str) -> pd.Series:
    """
    Detect price for each product type using smart fallbacks:
    1) markup_price / markupPrice
    2) delivered_price / deliveredPrice
    3) price
    4) price_per_carat * carats (or pricePerCarat * carats)
    Currency: markup_currency / markupCurrency (default USD)
    """
    rate = usd_to_cad_rate()

    # Normalize obvious candidates
    cols = df.columns

    # Price candidates (ordered)
    price_candidates = []
    if product_type == "lab_grown":
        price_candidates = ["markupPrice", "deliveredPrice", "price"]
        ppc_col = "pricePerCarat"
        carats_col = "carats"
        currency_col = "markupCurrency"
    elif product_type == "natural":
        price_candidates = ["markup_price", "delivered_price", "price"]
        ppc_col = "price_per_carat"
        carats_col = "carats"
        currency_col = "markup_currency"
    else:  # gemstone
        price_candidates = ["markup_price", "price", "price_per_carat"]
        ppc_col = "price_per_carat"
        carats_col = "carats"
        currency_col = "markup_currency"

    # Ensure missing columns exist as empty strings
    for c in set(price_candidates + [ppc_col, carats_col, currency_col]):
        if c and c not in cols:
            df[c] = ""

    # Parse primary price
    used_source = pd.Series([""] * len(df), index=df.index)
    price_usd = pd.Series([None] * len(df), index=df.index, dtype="float64")

    for cand in price_candidates:
        series = parse_money_to_float(df[cand]) if cand in df.columns else pd.Series([None]*len(df))
        take = price_usd.isna() & series.notna() & (series > 0)
        price_usd.loc[take] = series.loc[take]
        used_source.loc[take] = cand

    # If still missing/zero, try PPC * carats
    missing = price_usd.isna() | (price_usd <= 0)
    if missing.any():
        ppc = parse_money_to_float(df[ppc_col]) if ppc_col in df.columns else pd.Series([None]*len(df))
        carats = pd.to_numeric(df[carats_col], errors="coerce")
        ppc_total = (ppc * carats).where(ppc.notna() & carats.notna(), other=None)
        take_ppc = missing & ppc_total.notna() & (ppc_total > 0)
        price_usd.loc[take_ppc] = ppc_total.loc[take_ppc]
        # mark source
        used_source.loc[take_ppc] = f"{ppc_col}*{carats_col}"

    # Currency handling
    curr = df[currency_col].astype(str).str.strip().str.upper().replace({"": "USD"}) if currency_col in df.columns else pd.Series(["USD"]*len(df), index=df.index)

    # Convert to CAD
    def to_cad(p, c):
        if p is None or pd.isna(p) or p <= 0:
            return 0.0
        if c == "CAD":
            return round(float(p), 2)
        # treat everything else as USD
        return round(float(p) * rate, 2)

    price_cad = [to_cad(p, c) for p, c in zip(price_usd, curr)]
    price_cad = pd.to_numeric(price_cad, errors="coerce").fillna(0.0)

    # Logs
    print(f"[PRICE][{product_type}] rows={len(df)} | priced(>0 CAD)={(price_cad > 0).sum()} | sources used: "
          f"{used_source.value_counts(dropna=False).to_dict()}")

    return price_cad

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

            # Normalize columns that we reference later
            base_cols = ["shape","carats","col","clar","cut","pol","symm","flo","floCol",
                         "length","width","height","depth","table","culet","lab","girdle",
                         "ReportNo","image","video","pdf","diamondId","stockId","ID"]
            gemstone_only = ["gemType","Treatment","Mine of Origin","mine_of_origin","mineOfOrigin","pdfUrl","price_per_carat","markup_price","markup_currency"]
            natural_only  = ["price_per_carat","markup_price","markup_currency","mine_of_origin","canada_mark_eligible","is_returnable"]
            lab_only      = ["pricePerCarat","markupPrice","markupCurrency","deliveredPrice","minDeliveryDays","maxDeliveryDays","mineOfOrigin"]

            ensure_cols = set(base_cols + gemstone_only + natural_only + lab_only)
            for c in ensure_cols:
                if c not in df.columns:
                    df[c] = ""

            # Shape uppercase
            df["shape"] = df["shape"].astype(str).str.strip().str.upper()

            # Image link (safe)
            img_series = extract_image_series(df, "image")
            df["image_link"] = img_series.fillna('')
            df.loc[df["image_link"] == "", "image_link"] = df["shape"].map(lambda s: SHAPE_IMAGE_URLS.get(str(s).upper(), ""))

            # Price (CAD)
            df["price_cad"] = compute_price_cad(df, product_type)
            df["price"] = df["price_cad"].map(lambda x: f"{x:.2f} CAD")

            # IDs / meta
            df["ReportNo"] = df["ReportNo"].astype(str).str.strip()
            if product_type == "gemstone" and (df["ReportNo"] == "").all():
                # gemstone sometimes uses ID as identifier
                df["ReportNo"] = df["ID"].astype(str)

            df["id"] = (df["ReportNo"].where(df["ReportNo"] != "", df["diamondId"].astype(str)) + "CA").fillna("")

            df["availability"] = "in_stock"
            df["google_product_category"] = "188"
            df["brand"] = "Leela Diamonds"
            df["mpn"] = df["id"]
            df["condition"] = "new"
            df["color"] = "white/yellow/rose gold"
            df["age_group"] = "adult"
            df["gender"] = "unisex"

            # Measurements
            def meas(r): return fallback_measurements(r)

            # Titles / Descriptions / Links
            if product_type == "natural":
                def title(r):
                    return f"{r.get('shape','')}-{r.get('carats','')} Carats-{r.get('col','')} Color-{r.get('clar','')} Clarity-{r.get('lab','')} Certified-{r.get('shape','')}-Natural Diamond"
                def desc(r):
                    return (f"Natural {r.get('shape','')} diamond: {r.get('carats','')} carats, "
                            f"{r.get('col','')} color, {r.get('clar','')} clarity. "
                            f"Measurements: {meas(r)}. Cut: {r.get('cut','')}, Polish: {r.get('pol','')}, Symmetry: {r.get('symm','')}, "
                            f"Table: {r.get('table','')}%, Depth: {r.get('depth','')}%, Fluorescence: {r.get('flo','') or r.get('floCol','')}. "
                            f"{r.get('lab','')} certified.")
                def link(r):
                    cert = str(r.get("ReportNo","")).strip()
                    return f"https://leeladiamond.com/pages/natural-diamond-catalog?id={cert}"

            elif product_type == "lab_grown":
                def title(r):
                    return f"{r.get('shape','')}-{r.get('carats','')} Carats-{r.get('col','')} Color-{r.get('clar','')} Clarity-{r.get('lab','')} Certified-{r.get('shape','')}-Lab Grown Diamond"
                def desc(r):
                    return (f"Lab-grown {r.get('shape','')} diamond: {r.get('carats','')} carats, "
                            f"{r.get('col','')} color, {r.get('clar','')} clarity. "
                            f"Measurements: {meas(r)}. Cut: {r.get('cut','')}, Polish: {r.get('pol','')}, Symmetry: {r.get('symm','')}, "
                            f"Table: {r.get('table','')}%, Depth: {r.get('depth','')}%, Fluorescence: {r.get('flo','') or r.get('floCol','')}. "
                            f"{r.get('lab','')} certified.")
                def link(r):
                    return ("https://leeladiamond.com/pages/lab-grown-diamonds/"
                            f"{str(r.get('shape','')).strip().lower()}-"
                            f"{str(r.get('carats','')).replace('.', '-')}-carat-"
                            f"{str(r.get('col','')).strip().lower()}-color-"
                            f"{str(r.get('clar','')).strip().lower()}-clarity-"
                            f"{str(r.get('lab','')).strip().lower()}-certified-"
                            f"{str(r.get('ReportNo','')).strip()}")

            else:  # gemstone
                # Use gemType + Color + shape where present; include Treatment & Origin in description
                def title(r):
                    return f"{r.get('gemType','')} {r.get('Color','')} {r.get('shape','')} – {r.get('carats','')} Carats, {r.get('Clarity','')} Clarity, {r.get('Cut','')} Cut, {r.get('Lab','')} Certified"
                def desc(r):
                    treatment = r.get('Treatment','') or 'N/A'
                    origin = (r.get('Mine of Origin','') or r.get('mine_of_origin','') or r.get('mineOfOrigin','') or '').strip()
                    origin_txt = f" Origin: {origin}." if origin else ""
                    return (f"{r.get('gemType','')} gemstone in {r.get('Color','')} color, "
                            f"{r.get('carats','')} carats. Measurements: {meas(r)}. "
                            f"Clarity: {r.get('Clarity','')}, Cut: {r.get('Cut','')}, Lab: {r.get('Lab','')}. "
                            f"Treatment: {treatment}.{origin_txt}")
                def link(r):
                    cert = str(r.get("ReportNo","")).strip() or str(r.get("ID","")).strip()
                    return f"https://leeladiamond.com/pages/gemstone-catalog?id={cert}"

            df["title"] = df.apply(title, axis=1)
            df["description"] = df.apply(desc, axis=1)
            df["link"] = df.apply(link, axis=1)

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
