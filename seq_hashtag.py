import boto3
import json
import re
import time
from collections import Counter
from datetime import datetime

# Config
S3_BUCKET = 'imdbreviews-scalable'
S3_PREFIX = 'cleaned_files/'
SUMMARY_PREFIX = 'summaries/'
SUMMARY_TYPE_PREFIX = 'hashtag_sequential_summary_'

# Stop-word-style banned hashtags
stop_tags = {
    'the', 'and', 'you', 'but', 'was', 'are', 'for', 'have', 'not',
    'his', 'her', 'she', 'there', 'with', 'this', 'that', 'what',
    'in', 'on', 'out', 'at', 'by', 'all', 'spoiler', 'movie', 'film',
    'one', 'more', 'some', 'just', 'from', 'like', 'contains', 'commentessentially'
}

# Spoiler tags (Not directly used in the current logic)
SPOILER_KEYWORDS = {'#spoiler', '#spoilers', '#spoilerwarning', '#containsspoiler'}

# Initialize S3 client globally for sequential processing
s3 = boto3.client('s3')

def list_json_keys(bucket, prefix):
    """Lists all JSON files in the specified S3 prefix."""
    paginator = s3.get_paginator('list_objects_v2')
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.json'):
                keys.append(obj['Key'])
    return keys

def delete_old_summaries():
    """Deletes existing summary files from S3 based on type prefix."""
    paginator = s3.get_paginator('list_objects_v2')
    keys_to_delete = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=SUMMARY_PREFIX):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.startswith(SUMMARY_PREFIX + SUMMARY_TYPE_PREFIX):
                keys_to_delete.append({'Key': key})
    if keys_to_delete:
        s3.delete_objects(Bucket=S3_BUCKET, Delete={'Objects': keys_to_delete})
        print(f"Deleted {len(keys_to_delete)} old summaries matching '{SUMMARY_TYPE_PREFIX}'.")
    else:
        print(f"No old summaries matching '{SUMMARY_TYPE_PREFIX}' found to delete.")

def fetch_reviews_from_s3(key):
    """Fetches review data from a single S3 JSON file."""
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)
        valid_reviews = [r for r in data if isinstance(r, dict)]
        return valid_reviews, len(valid_reviews)
    except Exception as e:
        print(f"Error reading {key}: {e}")
        return [], 0

def extract_hashtags(review):
    """Extracts and cleans hashtags from a single review text."""
    text = review.get("review_detail", "") + " " + review.get("review_summary", "")
    # Regex for hashtags: starts with #, followed by 3+ letters, word boundary
    raw_tags = re.findall(r'(?<!\w)#([a-zA-Z]{3,})\b', text)
    return [
        f"#{tag.lower()}"
        for tag in raw_tags
        if tag.lower() not in stop_tags # Filter out common words used as hashtags
    ]

def save_summary_to_s3(summary_data):
    """Saves the final summary JSON to S3."""
    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%S-%f')
    key = f'{SUMMARY_PREFIX}{SUMMARY_TYPE_PREFIX}{timestamp}.json'
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(summary_data, indent=2),
        ContentType='application/json'
    )
    print(f"Summary saved to s3://{S3_BUCKET}/{key}")

if __name__ == "__main__":
    start_total_time = time.time()

    print("--- Starting Sequential Hashtag Analysis ---")

    print("Deleting existing hashtag_sequential_summary files from S3...")
    delete_old_summaries()

    print("Listing S3 files...")
    keys = list_json_keys(S3_BUCKET, S3_PREFIX)
    print(f"Found {len(keys)} files.")

    # --- Phase 1: Sequential Fetching of Reviews ---
    print("Loading reviews sequentially...")
    
    all_reviews_nested_result = []
    total_reviews_processed = 0

    for key in keys:
        reviews_from_file, count_from_file = fetch_reviews_from_s3(key)
        if reviews_from_file:
            all_reviews_nested_result.append(reviews_from_file)
            total_reviews_processed += count_from_file
    
    # Flatten the list of lists of reviews into a single list of all reviews
    reviews = [review for sublist in all_reviews_nested_result for review in sublist]

    print(f"Finished loading {len(reviews)} reviews. Total actual reviews counted: {total_reviews_processed}")
    print(f"Processing {len(reviews)} reviews for hashtags sequentially...")

    # --- Phase 2: Sequential Hashtag Extraction from Reviews ---
    all_tags = []
    for review_item in reviews:
        all_tags.extend(extract_hashtags(review_item))

    # --- Phase 3: Aggregate Results and Generate Summary ---
    hashtag_counter = Counter(all_tags)

    # Get top 20 hashtags that appear at least twice
    top_hashtags = [(tag, count) for tag, count in hashtag_counter.most_common(20) if count >= 2]

    end_total_time = time.time()
    total_pipeline_time = round(end_total_time - start_total_time, 2)
    overall_throughput_reviews_per_sec = round(total_reviews_processed / total_pipeline_time, 2) if total_pipeline_time > 0 else 0
    overall_latency_sec_per_review = round(total_pipeline_time / total_reviews_processed, 4) if total_reviews_processed > 0 else 0

    print("\n--- Hashtag Analysis Summary ---")
    if top_hashtags:
        print("Top 20 Hashtags (occurring at least twice):")
        for tag, count in top_hashtags:
            print(f"{tag}: {count}")
    else:
        print("No top hashtags found meeting the criteria.")

    print(f"\nTotal Pipeline Execution Time: {total_pipeline_time} seconds")
    print(f"Overall Throughput: {overall_throughput_reviews_per_sec} reviews/second")
    print(f"Overall Latency: {overall_latency_sec_per_review} seconds/review")

    summary = {
        'method': 'Hashtag (Sequential)', # Method name reflects sequential processing
        'total_reviews_processed': total_reviews_processed,
        'total_pipeline_time_sec': total_pipeline_time,
        'throughput_reviews_per_sec': overall_throughput_reviews_per_sec,
        'latency_sec_per_review': overall_latency_sec_per_review,
        'top_hashtags_occurrences': top_hashtags,
        'total_unique_hashtags': len(hashtag_counter),
        'total_extracted_hashtags': len(all_tags),
        'timestamp_utc': datetime.utcnow().isoformat() + "Z"
    }

    save_summary_to_s3(summary)
    print("\n--- Script Finished ---")
