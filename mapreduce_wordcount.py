import os
import boto3
import json
import time
from datetime import datetime
from multiprocessing import Pool, cpu_count
from collections import Counter
import re
import logging
from botocore.config import Config
from botocore.exceptions import ClientError

# Configure logging for better output management
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ---------- Configuration ----------
S3_BUCKET = 'imdbreviews-scalable'
S3_INPUT_PREFIX = 'cleaned_files/'  # Renamed for clarity, original was S3_PREFIX
S3_SUMMARY_PREFIX = 'summaries/'   # Target folder for storing summary JSON
SUMMARY_BASE_NAME = 'mapreduce_wordcount_summary'  # Prefix used to delete old files

# Configure Boto3 client for better performance and stability
BOTO_CONFIG = Config(
    read_timeout=900,
    connect_timeout=900,
    retries={'max_attempts': 10, 'mode': 'standard'}
)

# Global S3 client for the main process (for listing/deleting/uploading summaries)
_global_s3_client = boto3.client('s3', config=BOTO_CONFIG)

# Global S3 client for worker processes (initialized once per process)
_worker_s3_client = None

# Enhanced STOP_WORDS set
STOP_WORDS = {
    'the', 'and', 'to', 'of', 'a', 'in', 'it', 'is', 'i', 'that',
    'this', 'was', 'for', 'on', 'with', 'as', 'but', 'at', 'by',
    'an', 'be', 'from', 'are', 'have', 'has', 'not', 'they', 'you',
    'there', 'he', 'his', 'she', 'her', 'them', 'or', 'so', 'if',
    'my', 'we', 'our', 'their', 'what', 'who', 'will', 'just',
    # Added common punctuation/non-alpha tokens that might appear after regex if not careful
    '', '-', '_', '.', ',', '?', '!', ':', ';', '(', ')', '[', ']',
    '{', '}', '"', "'", '...', '/', '\\', '@', '#', '$', '%', '^',
    '&', '*', '+', '=', '<', '>', '~', '`', 's', 't', 'm', 'd', 'll', 've', 're', # Common contractions leftovers
    'nbsp', 'amp' # HTML entities if they might appear
}

def list_json_keys(bucket: str, prefix: str) -> list[str]:
    """Lists all JSON object keys within a given S3 bucket and prefix."""
    logging.info(f"Listing JSON objects in s3://{bucket}/{prefix}...")
    keys = []
    # Use the global S3 client for the main process
    paginator = _global_s3_client.get_paginator('list_objects_v2')
    try:
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        for page in pages:
            for obj in page.get('Contents', []):
                key = obj['Key']
                # Ensure it's a .json file and not an S3 folder marker
                if key.endswith('.json') and not key.endswith('/'):
                    keys.append(key)
    except ClientError as e:
        logging.error(f"Error listing objects in S3: {e}")
        raise # Re-raise to halt if we can't get file list
    logging.info(f"Found {len(keys)} JSON files in s3://{bucket}/{prefix}.")
    return keys

def delete_existing_summaries(bucket: str, prefix: str, base_name: str):
    """Deletes old summary files with a specific base name from S3."""
    logging.info("Checking and deleting old summary files if any...")
    
    # Use list_json_keys which uses the _global_s3_client
    keys_in_prefix = list_json_keys(bucket, prefix)
    
    keys_to_delete = [k for k in keys_in_prefix if k.startswith(prefix + base_name)]

    if keys_to_delete:
        logging.info(f"Found {len(keys_to_delete)} old summary file(s) to delete:")
        # Boto3 supports deleting multiple objects in one go
        delete_objects = {'Objects': [{'Key': k} for k in keys_to_delete]}
        try:
            _global_s3_client.delete_objects(Bucket=bucket, Delete=delete_objects)
            for k in keys_to_delete:
                logging.info(f" - Deleted {k}")
            logging.info("Old summary files deleted.")
        except ClientError as e:
            logging.error(f"Error deleting old summary files: {e}")
    else:
        logging.info("No matching old summary files found.")

def init_worker():
    """Initializer for multiprocessing pool workers. Creates an S3 client for each process."""
    global _worker_s3_client
    # Each process gets its own Boto3 client for thread-safety and resource management
    _worker_s3_client = boto3.client('s3', config=BOTO_CONFIG)
    logging.info(f"Worker process {os.getpid()} initialized S3 client.")

def map_wordcount_from_file(key: str) -> tuple[Counter, int]:
    """
    Downloads a single JSON file from S3, parses reviews, tokenizes words,
    removes stop words, and returns a Counter of word frequencies.
    """
    # Ensure worker client is initialized (redundant if initializer is used correctly, but safer)
    if _worker_s3_client is None:
        init_worker() 
    
    current_s3_client = _worker_s3_client # Use the client initialized for this process

    try:
        response = current_s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)

        counter = Counter()
        reviews_in_file = 0

        # Handle both list of dicts and a single dict within the JSON file
        reviews_data = data if isinstance(data, list) else [data]

        for r in reviews_data:
            if isinstance(r, dict):
                reviews_in_file += 1
                # Combine detail and summary, ensuring they are treated as strings
                review_detail = str(r.get('review_detail', ''))
                review_summary = str(r.get('review_summary', ''))
                text = f"{review_detail} {review_summary}".strip()

                # Improved word tokenization using regex to handle punctuation more robustly
                # \b\w+\b matches sequences of word characters (alphanumeric + underscore)
                words = [
                    word.lower()
                    for word in re.findall(r'\b\w+\b', text)
                    if word.lower() not in STOP_WORDS
                ]
                counter.update(words)
        return counter, reviews_in_file
    except ClientError as e:
        logging.error(f"S3 client error for {key}: {e}")
        return Counter(), 0
    except json.JSONDecodeError as e:
        logging.error(f"JSON decoding error for {key}: {e}. File might be corrupted or not valid JSON.")
        return Counter(), 0
    except Exception as e:
        logging.error(f"An unexpected error occurred while processing {key}: {e}")
        return Counter(), 0

def reduce_counts(mapped_results: list[tuple[Counter, int]]) -> tuple[Counter, int]:
    """Combines partial word counts and sums total reviews from mapped results."""
    final_word_counts = Counter()
    total_reviews_processed = 0
    for partial_word_count, reviews_count_in_file in mapped_results:
        final_word_counts.update(partial_word_count)
        total_reviews_processed += reviews_count_in_file
    return final_word_counts, total_reviews_processed

if __name__ == '__main__':
    start_time = time.time()

    logging.info("Starting word count process...")

    # Delete old summaries using the _global_s3_client
    delete_existing_summaries(S3_BUCKET, S3_SUMMARY_PREFIX, SUMMARY_BASE_NAME)

    logging.info("Listing input files from S3...")
    # List keys using the _global_s3_client
    keys = list_json_keys(S3_BUCKET, S3_INPUT_PREFIX)
    if not keys:
        logging.warning("No JSON input files found. Exiting.")
        exit()
    
    logging.info("Running parallel Map step...")
    num_processes = cpu_count()
    # Chunksize: Divide total items by (num_processes * factor) for better load balancing.
    # A common factor is 4-10. Experiment with this value for optimal performance.
    chunk_size = max(1, len(keys) // (num_processes * 4)) 
    logging.info(f"Using {num_processes} processes with chunk_size={chunk_size} for map step.")

    # Use a multiprocessing Pool with an initializer to set up the S3 client for each worker
    with Pool(processes=num_processes, initializer=init_worker) as pool:
        mapped_results = pool.map(map_wordcount_from_file, keys, chunksize=chunk_size)

    logging.info("Reducing results...")
    final_word_counts, total_reviews_processed = reduce_counts(mapped_results)

    end_time = time.time()
    total_mapreduce_time = round(end_time - start_time, 2)

    throughput = round(total_reviews_processed / total_mapreduce_time, 2) if total_mapreduce_time > 0 else 0
    latency = round(total_mapreduce_time / total_reviews_processed, 4) if total_reviews_processed > 0 else 0

    logging.info("\n--- MapReduce Summary ---")
    logging.info(f"Total files processed: {len(keys)}")
    logging.info(f"Total reviews processed: {total_reviews_processed}")
    logging.info(f"Total MapReduce Time: {total_mapreduce_time} seconds")
    logging.info(f"Overall Throughput: {throughput} reviews/second")
    logging.info(f"Overall Latency: {latency} seconds/review")

    logging.info("\nTop 10 Words (excluding stop words):")
    top_words = final_word_counts.most_common(10)
    for word, count in top_words:
        logging.info(f"{word}: {count}")

    # --- Save summary to S3 ---
    summary_data = {
        "method": "Word Count (Parallel MapReduce)",
        "total_files_processed": len(keys),
        "total_reviews_processed": total_reviews_processed,
        "total_mapreduce_time_sec": total_mapreduce_time,
        "throughput_reviews_per_sec": throughput,
        "latency_sec_per_review": latency,
        "top_10_words": top_words,
        "timestamp_utc": datetime.utcnow().isoformat()
    }

    summary_key = f"{S3_SUMMARY_PREFIX}{SUMMARY_BASE_NAME}_{int(time.time())}.json"
    try:
        # Use the _global_s3_client for uploading the final summary
        _global_s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=summary_key,
            Body=json.dumps(summary_data, indent=2),
            ContentType='application/json'
        )
        logging.info(f"\nSummary saved to s3://{S3_BUCKET}/{summary_key}")
    except ClientError as e:
        logging.error(f"Error saving summary to S3: {e}")
