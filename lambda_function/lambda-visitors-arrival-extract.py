import os
import tempfile
import boto3
import pdfplumber
import pandas as pd
import urllib.parse

s3 = boto3.client('s3')

SKIP_KEYWORDS = ["GRAND TOTAL", "OVERSEAS FILIPINOS", "FOREIGN TOURISTS"]

def is_summary_row(country):
    country = country.upper().replace("\xa0", " ").replace("*", "").strip()
    return any(skip in country for skip in SKIP_KEYWORDS)

def detect_ranking_type(text):
    text = text.upper()
    if "BY NATIONALITY" in text:
        return "Ranking by Nationality"
    if "BY COUNTRY OF RESIDENCE" in text:
        return "Ranking by Country of Residence"
    return "Unknown"

def clean_number(val):
    if val is None:
        return ""
    return val.replace(",", "").strip()

def lambda_handler(event, context):
    # Get S3 event info
    src_bucket = event['Records'][0]['s3']['bucket']['name']
    src_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    dst_bucket = os.environ.get('DEST_BUCKET', src_bucket)
    dst_prefix = os.environ.get('DEST_PREFIX', '')
    print(f"Bucket: {src_bucket}, Key: {src_key}")
    # Download PDF to /tmp
    with tempfile.TemporaryDirectory() as tmpdir:
        pdf_path = os.path.join(tmpdir, 'input.pdf')
        s3.download_file(src_bucket, src_key, pdf_path)

        # Process PDF
        rows = []
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                # Try to detect ranking type from page text
                page_text = page.extract_text() or ""
                ranking_type = detect_ranking_type(page_text)
                # Extract all tables from the page
                tables = page.extract_tables()
                for table in tables:
                    # Skip header rows (look for "Rank" or "Country" in first row)
                    for row in table:
                        if not row or len(row) < 14:
                            continue
                        if "RANK" in (row[0] or "").upper() or "COUNTRY" in (row[1] or "").upper():
                            continue
                        # Clean up row values
                        rank = (row[0] or "").strip()
                        country = (row[1] or "").strip()
                        if not country or is_summary_row(country):
                            continue
                        months = [clean_number(x) for x in row[2:12]]
                        total = clean_number(row[12])
                        pct = clean_number(row[13]).replace("%", "")
                        rows.append([rank, country] + months + [total, pct, ranking_type])

        columns = [
            "Rank", "Country", "January", "February", "March", "April", "May", "June", "July",
            "August", "September", "October", "Jan-Oct Total", "% Share", "RankingType"
        ]
        df = pd.DataFrame(rows, columns=columns)

        # Save CSV to /tmp and upload to S3
        csv_filename = os.path.splitext(os.path.basename(src_key))[0] + ".csv"
        csv_path = os.path.join(tmpdir, csv_filename)
        df.to_csv(csv_path, index=False)

        dst_key = f"{dst_prefix}{csv_filename}"
        s3.upload_file(csv_path, dst_bucket, dst_key)

        # Move the original PDF to the processed folder in S3
        processed_key = f"visitors-arrivals-landing-processed/{os.path.basename(src_key)}"
        s3.copy_object(
            Bucket=src_bucket,
            CopySource={'Bucket': src_bucket, 'Key': src_key},
            Key=processed_key
        )
        s3.delete_object(Bucket=src_bucket, Key=src_key)
        print(f"✅ Original PDF moved to: s3://{src_bucket}/{processed_key}")

        print(f"✅ Cleaned visitor arrivals saved to: s3://{dst_bucket}/{dst_key}")

    return {
        'statusCode': 200,
        'body': f"CSV saved to s3://{dst_bucket}/{dst_key}"
    } 