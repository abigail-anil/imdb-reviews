import boto3
import json
import re
import time
from datetime import datetime
import os # For os.getpid() in logging
from multiprocessing import Pool, cpu_count
from collections import Counter
import logging
from botocore.config import Config
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ---------- Configuration ----------
S3_BUCKET = 'imdbreviews-scalable'
S3_INPUT_PREFIX = 'cleaned_files/' 

# Stop-word-style banned hashtags
STOP_TAGS = {
    'the', 'and', 'you', 'but', 'was', 'are', 'for', 'have', 'not',
    'his', 'her', 'she', 'there', 'with', 'this', 'that', 'what',
    'in', 'on', 'out', 'at', 'by', 'all', 'spoiler', 'movie', 'film',
    'one', 'more', 'some', 'just', 'from', 'like', 'contains', 'commentessentially',
    'review', 'summary', 'detail', 'imdb', 'good', 'bad', 'great', 'really',
    'watch', 'seen', 'time', 'story', 'character', 'acting', 'ending', 'plot'
}

# Configure Boto3 client for better performance and stability
BOTO_CONFIG = Config(
    read_timeout=900,
    connect_timeout=900,
    retries={'max_attempts': 10, 'mode': 'standard'}
)

# Global S3 client for the main process (for listing files)
_global_s3_client = boto3.client('s3', config=BOTO_CONFIG)

# Global S3 client for worker processes (initialized once per process)
_worker_s3_client = None

# Step 1: List all JSON files in S3
def list_json_keys(bucket: str, prefix: str) -> list[str]:
    """Lists all JSON object keys within a given S3 bucket and prefix."""
    logging.info(f"Listing JSON objects in s3://{bucket}/{prefix}...")
    keys = []
    paginator = _global_s3_client.get_paginator('list_objects_v2')
    try:
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        for page in pages:
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('.json') and not key.endswith('/'): # Ensure it's a file
                    keys.append(key)
    except ClientError as e:
        logging.error(f"Error listing objects in S3: {e}")
        raise # Re-raise to halt if we can't get file list
    logging.info(f"Found {len(keys)} JSON files in s3://{bucket}/{prefix}.")
    return keys

# Initializer for multiprocessing pool workers
def init_worker():
    """Initializes a dedicated Boto3 S3 client for each worker process."""
    global _worker_s3_client
    _worker_s3_client = boto3.client('s3', config=BOTO_CONFIG)
    logging.info(f"Worker process {os.getpid()} initialized S3 client.")

# Step 2: Fetch reviews from one file
# Return (list of valid reviews, count of valid reviews)
def fetch_reviews_from_s3(key_bucket_pair: tuple[str, str]) -> tuple[list[dict], int]:
    """
    Fetches a single JSON file from S3, parses it, and extracts valid review dictionaries.
    Returns a tuple of (list of valid review dicts, count of valid reviews).
    """
    key, bucket = key_bucket_pair
    
    # Use the S3 client initialized for this worker process
    if _worker_s3_client is None:
        init_worker() # Fallback, should be handled by Pool initializer
    current_s3_client = _worker_s3_client

    try:
        response = current_s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)
        
        valid_reviews = []
        if isinstance(data, list):
            for r in data:
                if isinstance(r, dict):
                    valid_reviews.append(r)
        elif isinstance(data, dict): # Handle case where JSON contains a single review object
            valid_reviews.append(data)
        
        return valid_reviews, len(valid_reviews)
    except ClientError as e:
        logging.error(f"S3 client error reading {key}: {e}")
        return [], 0
    except json.JSONDecodeError as e:
        logging.error(f"JSON decoding error for {key}: {e}. File might be corrupted or not valid JSON.")
        return [], 0
    except Exception as e:
        logging.error(f"An unexpected error occurred while reading {key}: {e}")
        return [], 0

# Step 3: Extract hashtags from a single review
def extract_hashtags(review: dict) -> list[str]:
    """
    Extracts relevant hashtags from a single review dictionary.
    Hashtags must be at least 3 alphabetical characters long and not in STOP_TAGS.
    """
    # Use .get() with default empty string for robustness
    text = str(review.get("review_detail", "")) + " " + str(review.get("review_summary", ""))
    
    # Regex: (?<!\w)#([a-zA-Z]{3,})\b
    # (?<!\w) - Negative lookbehind, ensures no word character before # (e.g., prevents "abc#tag")
    # # - Matches the literal hash symbol
    # ([a-zA-Z]{3,}) - Captures only letters, at least 3 characters long (the actual tag)
    # \b - Word boundary to ensure the tag ends properly (e.g., #tag. would match only #tag)
    raw_tags = re.findall(r'(?<!\w)#([a-zA-Z]{3,})\b', text)
    
    # Convert to lowercase and prepend '#' for consistency, then filter by STOP_TAGS
    return [
        f"#{tag.lower()}"
        for tag in raw_tags
        if tag.lower() not in STOP_TAGS
    ]

# Main execution
if __name__ == "__main__":
    start_total_time = time.time()
    
    logging.info("Starting hashtag extraction process...")
    logging.info("Listing S3 files...")
    keys = list_json_keys(S3_BUCKET, S3_INPUT_PREFIX)
    
    if not keys:
        logging.warning("No JSON files found in the input prefix. Exiting.")
        exit()

    logging.info(f"Found {len(keys)} files. Loading reviews in parallel...")
    num_fetch_processes = cpu_count()
    # Calculate chunk size for fetching files
    # A chunk_size of 1 is often fine for I/O bound tasks if files vary greatly in size
    # For many small files, a larger chunk_size might reduce overhead.
    fetch_chunk_size = max(1, len(keys) // (num_fetch_processes * 2))
    logging.info(f"Using {num_fetch_processes} processes with chunk_size={fetch_chunk_size} for fetching reviews.")

    # Step 4: Fetch reviews from S3 in parallel
    # Use initializer to ensure each process has its own S3 client
    with Pool(processes=num_fetch_processes, initializer=init_worker) as pool:
        # pool.map returns a list of (reviews_list, count) tuples
        all_reviews_and_counts_nested = pool.map(fetch_reviews_from_s3, [(key, S3_BUCKET) for key in keys], chunksize=fetch_chunk_size)

    # Flatten all reviews and sum up total reviews processed
    reviews = []
    total_reviews_processed = 0
    for review_list_from_file, count_from_file in all_reviews_and_counts_nested:
        reviews.extend(review_list_from_file)
        total_reviews_processed += count_from_file

    if not reviews:
        logging.warning("No valid reviews found after fetching all files. Exiting.")
        exit()

    logging.info(f"Loaded {len(reviews)} review objects for hashtag extraction.")
    logging.info(f"Total actual reviews counted from files: {total_reviews_processed}")
    
    logging.info(f"Processing {len(reviews)} reviews for hashtags in parallel...")
    num_extract_processes = cpu_count()
    # Calculate chunk size for hashtag extraction (CPU bound)
    extract_chunk_size = max(1, len(reviews) // (num_extract_processes * 4)) # More aggressive chunking
    logging.info(f"Using {num_extract_processes} processes with chunk_size={extract_chunk_size} for hashtag extraction.")

    # Step 5: Extract hashtags in parallel
    # This Pool does not need an S3 client, so no initializer is required
    with Pool(processes=num_extract_processes) as pool:
        hashtags_nested = pool.map(extract_hashtags, reviews, chunksize=extract_chunk_size)

    # Flatten and count
    all_tags = [tag for sublist in hashtags_nested for tag in sublist]
    hashtag_counter = Counter(all_tags)

    # Top hashtags (at least 2 times)
    # Using most_common(None) gets all items, then filter. This is fine for reasonable number of unique tags.
    # If millions of unique tags, consider a different approach for filtering.
    top_hashtags = [(tag, count) for tag, count in hashtag_counter.most_common() if count >= 2]
    # Sort again by count in descending order, then alphabetically for ties (most_common already does this, but for clarity)
    top_hashtags.sort(key=lambda x: (-x[1], x[0]))
    # Limit to top 20 after filtering for count >= 2
    top_hashtags = top_hashtags[:20]

    # Output
    logging.info("\n--- Top Hashtags ---")
    if top_hashtags:
        for tag, count in top_hashtags:
            logging.info(f"{tag}: {count}")
    else:
        logging.info("No hashtags found matching criteria.")

    end_total_time = time.time()
    total_time = round(end_total_time - start_total_time, 2)

    # Calculate Throughput and Latency based on total_reviews_processed
    throughput = round(total_reviews_processed / total_time, 2) if total_time > 0 else 0
    latency = round(total_time / total_reviews_processed, 4) if total_reviews_processed > 0 else 0

    logging.info(f"\n--- Performance Summary ---")
    logging.info(f"Total Time: {total_time} seconds")
    logging.info(f"Throughput: {throughput} reviews/second")
    logging.info(f"Latency: {latency} seconds/review")
    logging.info(" Hashtag extraction complete.")
