import boto3
import json
import os
from io import BytesIO

# === CONFIG ===
BUCKET_NAME = 'imdbreviews-scalable'
INPUT_KEY = 'input_files/part-02.json'      # Change this
CHUNK_SIZE = 100_000
OUTPUT_PREFIX = 'input_chunks/'                   # Change if needed

# === Initialize S3 ===
s3 = boto3.client('s3')

# === Load large JSON from S3 ===
print(f"⬇️ Downloading {INPUT_KEY} from S3...")
response = s3.get_object(Bucket=BUCKET_NAME, Key=INPUT_KEY)
data = json.loads(response['Body'].read().decode('utf-8'))
print(f"✅ Loaded {len(data)} records")

# === Split and upload chunks ===
for i in range(0, len(data), CHUNK_SIZE):
    chunk = data[i:i + CHUNK_SIZE]
    chunk_filename = f"{OUTPUT_PREFIX}split_{i//CHUNK_SIZE + 1}.json"

    # Convert to JSON and upload directly
    buffer = BytesIO()
    buffer.write(json.dumps(chunk).encode('utf-8'))
    buffer.seek(0)

    s3.upload_fileobj(buffer, BUCKET_NAME, chunk_filename)
    print(f"☁️ Uploaded {chunk_filename} with {len(chunk)} records")

print("\n✅ All chunks uploaded to S3.")

