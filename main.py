import os
import csv
import ftplib
import pandas as pd
from google.cloud import storage
from flask import jsonify

# ----------------------------
# CONFIGURATION & CREDENTIALS
# ----------------------------

# The GOOGLE_APPLICATION_CREDENTIALS environment variable should be set externally.
# Bucket details are loaded from environment variables.
bucket_name = os.environ.get("BUCKET_NAME")       # e.g., "sitemaps.leeladiamond.com"
bucket_folder = os.environ.get("BUCKET_FOLDER")   # e.g., "Googlefinal"

# Directories for file storage (using Linux paths)
local_output_directory = os.environ.get("LOCAL_OUTPUT_DIRECTORY", "/tmp/output")
ftp_download_dir = os.environ.get("FTP_DOWNLOAD_DIR", "/tmp/ftp")

# Create directories if they don't exist
os.makedirs(local_output_directory, exist_ok=True)
os.makedirs(ftp_download_dir, exist_ok=True)

# FTP Server Details
FTP_SERVER = "ftp.nivoda.net"
FTP_PORT = 21
FTP_USERNAME = "leeladiamondscorporate@gmail.com"
FTP_PASSWORD = "1yH£lG4n0Mq"

# Mapping product types to FTP file details and local save paths
ftp_files = {
    "natural": {
        "remote_filename": "Leela Diamond_natural.csv",
        "local_path": os.path.join(ftp_download_dir, "Natural.csv"),
    },
    "lab_grown": {
        "remote_filename": "Leela Diamond_labgrown.csv",
        "local_path": os.path.join(ftp_download_dir, "Labgrown.csv"),
    },
    "gemstone": {
        "remote_filename": "Leela Diamond_gemstones.csv",
        "local_path": os.path.join(ftp_download_dir, "gemstones.csv"),
    },
}

# ----------------------------
# FTP DOWNLOAD FUNCTIONS
# ----------------------------

def download_file_from_ftp(remote_filename, local_path):
    """Download a file from the FTP server to a local path."""
    try:
        with ftplib.FTP() as ftp:
            ftp.connect(FTP_SERVER, FTP_PORT, timeout=30)
            ftp.login(FTP_USERNAME, FTP_PASSWORD)
            with open(local_path, 'wb') as f:
                ftp.retrbinary(f"RETR {remote_filename}", f.write)
        print(f"Downloaded {remote_filename} to {local_path}")
    except Exception as e:
        print(f"Error downloading {remote_filename}: {e}")

def download_all_files():
    """Download all defined files from the FTP server."""
    for _, file_info in ftp_files.items():
        download_file_from_ftp(file_info["remote_filename"], file_info["local_path"])

# ----------------------------
# IMAGE FALLBACKS
# ----------------------------

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
    "PEAR": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/PEAR%20MODIFIED%20BRILLIANT.webp",
    "PEAR MODIFIED BRILLIANT": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/PEAR.jpg",
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
    "TRILLIANT": "https://storage.googleapis.com/sitemaps.leeladiamond.com/shapes/TRILLIANT.jpg",
}

# ----------------------------
# PRICING FUNCTIONS
# ----------------------------

def convert_to_cad(price_usd):
    """Convert price from USD to CAD using a fixed exchange rate."""
    cad_rate = 1.41  # set via ENV or config if needed
    try:
        price_usd = float(price_usd)  # ensure numeric
        return round(price_usd * cad_rate, 2)
    except (ValueError, TypeError) as e:
        print(f"[WARN] Currency conversion skipped for invalid value {price_usd}: {e}")
        return 0.0  # safer fallback

# ----------------------------
# DATA PROCESSING
# ----------------------------

def process_files_to_cad(files_to_load, output_file):
    """Process input CSV files, apply transformations, and save to a combined file."""
    try:
        all_data = []

        for product_type, file_info in files_to_load.items():
            input_file = file_info["file_path"]
            if not os.path.exists(input_file):
                print(f"Missing input file: {input_file}")
                continue

            # Load the CSV file
            df = pd.read_csv(input_file, dtype=str).fillna('')

            # Clean shape
            if 'shape' in df.columns:
                df['shape'] = df['shape'].str.strip().str.upper()

            # Extract direct image URLs if present
            if 'image' in df.columns:
                df['image'] = df['image'].str.extract(r'(https?://.*\.(?:jpg|jpeg|png|webp))', expand=False)

            # Assign default image if no valid URL is found
            df['image_link'] = df.apply(
                lambda row: row['image'] if pd.notna(row.get('image')) and row.get('image')
                else SHAPE_IMAGE_URLS.get(row.get('shape', ''), ''),
                axis=1
            )

            # ---- PRICE: convert existing markupPrice (assumed USD) -> CAD; show on 'price' ----
            df['markupPrice'] = pd.to_numeric(df.get('markupPrice', 0), errors='coerce').fillna(0)
            df['markupPrice'] = df['markupPrice'].apply(convert_to_cad)
            df['price'] = df['markupPrice'].map(lambda x: f"{x:.2f} CAD")
            df['markupCurrency'] = 'CAD'

            # Add required feed columns (with safe fallbacks)
            df['ReportNo'] = df.get('ReportNo', '')
            df['id'] = df['ReportNo'].astype(str) + "CA"
            df['availability'] = 'in_stock'
            df['google_product_category'] = '188'
            df['brand'] = 'Leela Diamonds'
            df['mpn'] = df['id']
            df['condition'] = 'new'
            df['color'] = 'white/yellow/rose gold'
            df['age_group'] = 'adult'
            df['gender'] = 'unisex'

            # Product-specific templates
            product_templates = {
                "natural": {
                    "title": lambda row: f"{row.get('shape','')}-{row.get('carats','')} Carats-{row.get('col','')} Color-{row.get('clar','')} Clarity-{row.get('lab','')} Certified-{row.get('shape','')}-Natural Diamond",
                    "description": lambda row: (
                        f"Discover sustainable luxury with our natural {row.get('shape','')} diamond: "
                        f"{row.get('carats','')} carats, {row.get('col','')} color, and {row.get('clar','')} clarity. "
                        f"Measurements: {row.get('length','')}-{row.get('width','')}x{row.get('height','')} mm. "
                        f"Cut: {row.get('cut','')}, Polish: {row.get('pol','')}, Symmetry: {row.get('symm','')}, "
                        f"Table: {row.get('table','')}%, Depth: {row.get('depth','')}%, Fluorescence: {row.get('flo','')}. "
                        f"{row.get('lab','')} certified {row.get('shape','')}"
                    ),
                    "link": lambda row: f"https://leeladiamond.com/pages/natural-diamond-catalog?id={row.get('ReportNo','')}"
                },
                "lab_grown": {
                    "title": lambda row: f"{row.get('shape','')}-{row.get('carats','')} Carats-{row.get('col','')} Color-{row.get('clar','')} Clarity-{row.get('lab','')} Certified-{row.get('shape','')}-Lab Grown Diamond",
                    "description": lambda row: (
                        f"Discover sustainable luxury with our lab-grown {row.get('shape','')} diamond: "
                        f"{row.get('carats','')} carats, {row.get('col','')} color, and {row.get('clar','')} clarity. "
                        f"Measurements: {row.get('length','')}-{row.get('width','')}x{row.get('height','')} mm. "
                        f"Cut: {row.get('cut','')}, Polish: {row.get('pol','')}, Symmetry: {row.get('symm','')}, "
                        f"Table: {row.get('table','')}%, Depth: {row.get('depth','')}%, Fluorescence: {row.get('flo','')}. "
                        f"{row.get('lab','')} certified {row.get('shape','')}"
                    ),
                    # SEO-friendly link (carats 1.50 -> 1-50, lowercased, hyphenated)
                    "link": lambda row: (
                        "https://leeladiamond.com/pages/lab-grown-diamonds/"
                        f"{str(row.get('shape','')).lower()}-"
                        f"{str(row.get('carats','')).replace('.', '-')}-carat-"
                        f"{str(row.get('cut','')).replace(' ', '-').lower()}-"
                        f"{str(row.get('clar','')).lower()}-clarity-"
                        f"{str(row.get('lab','')).lower()}-certified-"
                        f"{str(row.get('ReportNo',''))}"
                    )
                },
                "gemstone": {
                    "title": lambda row: f"{row.get('shape','')} {row.get('Color','')} {row.get('gemType','')} Gemstone - {row.get('carats','')} Carats, {row.get('Clarity','')} Clarity, {row.get('Cut','')} Cut, {row.get('Lab','')} Certified",
                    "description": lambda row: (
                        f"{row.get('shape','')} {row.get('gemType','')} {row.get('Color','')} Gemstone - "
                        f"{row.get('carats','')} carats, clarity: {row.get('Clarity','')}, cut: {row.get('Cut','')}, "
                        f"lab: {row.get('Lab','')}, treatment: {row.get('Treatment','')}, "
                        f"origin: {row.get('Mine of Origin','')}, size: {row.get('length','')}x{row.get('width','')} mm."
                    ),
                    "link": lambda row: f"https://leeladiamond.com/pages/gemstone-catalog?id={row.get('ReportNo','')}"
                }
            }

            template = product_templates.get(product_type)
            if template is None:
                print(f"Unknown product type: {product_type}")
                continue

            df['title'] = df.apply(template['title'], axis=1)
            df['description'] = df.apply(template['description'], axis=1)
            df['link'] = df.apply(template['link'], axis=1)

            # Select final columns
            final_cols = [
                'id', 'title', 'description', 'link', 'image_link', 'availability', 'price',
                'google_product_category', 'brand', 'mpn', 'condition', 'color', 'age_group', 'gender'
            ]
            for c in final_cols:
                if c not in df.columns:
                    df[c] = ''
            df = df[final_cols]
            all_data.append(df)

        if not all_data:
            raise RuntimeError("No input files were processed; combined feed not created.")

        # Combine all product data and save to CSV
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df.to_csv(
            output_file,
            index=False,
            quoting=csv.QUOTE_MINIMAL,
            quotechar='"',
            escapechar='\\'
        )
        print(f"Combined data saved to {output_file}")
    except Exception as e:
        print(f"Error in processing files: {e}")

# ----------------------------
# GOOGLE CLOUD UPLOAD FUNCTION
# ----------------------------

def upload_files_to_bucket(bucket_name, bucket_folder, local_directory):
    """Upload all files in a local directory to a GCS bucket folder."""
    try:
        if not bucket_name or not bucket_folder:
            print("Skipping upload: BUCKET_NAME or BUCKET_FOLDER not set.")
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
                print(f"Uploaded {file_name} to {destination_blob_name}")
    except Exception as e:
        print(f"Error during upload: {e}")

# ----------------------------
# MAIN AUTOMATION WORKFLOW
# ----------------------------

def run_workflow():
    # Step 1: Download raw CSV files from the FTP server
    download_all_files()

    # Step 2: Define file paths for processing (using ftp_download_dir)
    files_to_load = {
        "natural": {"file_path": os.path.join(ftp_download_dir, "Natural.csv")},
        "lab_grown": {"file_path": os.path.join(ftp_download_dir, "Labgrown.csv")},
        "gemstone": {"file_path": os.path.join(ftp_download_dir, "gemstones.csv")},
    }
    output_file = os.path.join(local_output_directory, "combined_google_merchant_feed.csv")

    # Step 3: Process the downloaded files and create a combined CSV
    process_files_to_cad(files_to_load, output_file)

    # Step 4: Upload the combined CSV (and any other files in the output directory) to GCS
    upload_files_to_bucket(bucket_name, bucket_folder, local_output_directory)
    return "Workflow executed successfully."

# ----------------------------
# CLOUD FUNCTION ENTRY POINT
# ----------------------------

def cloud_function_entry(request):
    """HTTP Cloud Function entry point."""
    try:
        result = run_workflow()
        return jsonify({"status": "success", "message": result})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# ----------------------------
# RUN LOCALLY (for testing purposes)
# ----------------------------

if __name__ == "__main__":
    # For local testing, run the workflow directly
    print(run_workflow())
