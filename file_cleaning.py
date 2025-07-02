import boto3
import json
import logging
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError

# ---------- Configuration ----------
S3_BUCKET = 'imdbreviews-scalable'
S3_INPUT_PREFIX = 'input_chunks/' # Changed to prefix
S3_OUTPUT_PREFIX = 'cleaned_files/' # Changed to prefix

# Keys to remove from the JSON
keys_to_remove = ['review_id', 'reviewer', 'rating', 'review_date', 'spoiler_tag', 'helpful']

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize S3 client outside the function if using ThreadPoolExecutor for efficiency
s3 = boto3.client('s3')

def list_json_objects(bucket: str, prefix: str) -> list[str]:
    """Lists all JSON object keys within a given SS3 bucket and prefix."""
    logging.info(f"Listing JSON objects in s3://{bucket}/{prefix}...")
    keys = []
    paginator = s3.get_paginator('list_objects_v2')
    try:
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        for page in pages:
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('.json') and not key.endswith('/'): # Ensure it's a file, not a folder marker
                    keys.append(key)
    except ClientError as e:
        logging.error(f"Error listing objects in S3: {e}")
        raise
    logging.info(f"Found {len(keys)} JSON files in s3://{bucket}/{prefix}.")
    return keys

def process_single_file(input_key: str):
    """
    Downloads a single JSON file, removes specified keys, and uploads the cleaned data.
    """
    try:
        logging.info(f"Processing file: {input_key}")

        # Construct the output key, preserving the folder structure/filename
        # Example: input_files/subdir/file.json -> cleaned_files/subdir/file.json
        relative_path = os.path.relpath(input_key, S3_INPUT_PREFIX)
        output_key = os.path.join(S3_OUTPUT_PREFIX, relative_path).replace("\\", "/") # Handle Windows paths if run locally

        # 1. Download from S3
        response = s3.get_object(Bucket=S3_BUCKET, Key=input_key)
        data = json.loads(response['Body'].read().decode('utf-8'))

        # 2. Remove keys
        if isinstance(data, dict):  # Single JSON object
            for key in keys_to_remove:
                data.pop(key, None) # .pop(key, None) safely removes if key exists, otherwise does nothing
        elif isinstance(data, list):  # List of JSON objects
            data = [
                {k: v for k, v in item.items() if k not in keys_to_remove}
                for item in data
            ]
        else:
            logging.warning(f"Unsupported JSON format in {input_key}. Skipping key removal for this file.")
            # If format is truly unsupported, you might want to skip the file entirely or handle it differently.
            # For now, we'll just not modify it.

        # 3. Upload cleaned JSON to S3
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=output_key,
            Body=json.dumps(data, indent=2),
            ContentType='application/json'
        )
        logging.info(f"Successfully processed and uploaded {input_key} to {output_key}.")
        return input_key # Indicate success
    except ClientError as e:
        logging.error(f"S3 client error for {input_key}: {e}")
        return None # Indicate failure
    except json.JSONDecodeError as e:
        logging.error(f"JSON decoding error for {input_key}: {e}. File might be corrupted or not valid JSON.")
        return None # Indicate failure
    except Exception as e:
        logging.error(f"An unexpected error occurred while processing {input_key}: {e}")
        return None # Indicate failure

if __name__ == '__main__':
    start_time = time.time()
    processed_count = 0
    failed_count = 0

    all_input_keys = list_json_objects(S3_BUCKET, S3_INPUT_PREFIX)

    if not all_input_keys:
        logging.info("No JSON files found to process. Exiting.")
    else:
        # Use ThreadPoolExecutor for concurrent processing
        # Adjust max_workers based on your network bandwidth and CPU capacity
        # A common starting point is cpu_count() * 2 or more if I/O bound.
        max_workers = min(os.cpu_count() * 4, 32) # Example: up to 32 concurrent threads
        logging.info(f"Starting processing with {max_workers} concurrent workers...")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tasks for each file and collect futures
            future_to_key = {executor.submit(process_single_file, key): key for key in all_input_keys}
            
            for future in as_completed(future_to_key):
                original_key = future_to_key[future]
                try:
                    result_key = future.result()
                    if result_key:
                        processed_count += 1
                    else:
                        failed_count += 1
                except Exception as exc:
                    logging.error(f'{original_key} generated an exception: {exc}')
                    failed_count += 1

    end_time = time.time()
    total_time = round(end_time - start_time, 2)

    logging.info(f"--- Processing Summary ---")
    logging.info(f"Total files found: {len(all_input_keys)}")
    logging.info(f"Files successfully processed: {processed_count}")
    logging.info(f"Files failed to process: {failed_count}")
    logging.info(f"Total execution time: {total_time} seconds")
    logging.info(" All applicable JSON files processed.")
