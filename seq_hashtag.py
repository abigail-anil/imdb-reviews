import boto3
import json
import re
import time
from collections import Counter

# Config
S3_BUCKET = 'imdbreviews-scalable'
S3_PREFIX = 'input_files/'

# Stop-word-style banned hashtags
stop_tags = {
    'the', 'and', 'you', 'but', 'was', 'are', 'for', 'have', 'not',
    'his', 'her', 'she', 'there', 'with', 'this', 'that', 'what',
    'in', 'on', 'out', 'at', 'by', 'all', 'spoiler', 'movie', 'film',
    'one', 'more', 'some', 'just', 'from', 'like', 'contains', 'commentessentially'
}

# Define spoiler-related keywords to identify spoiler tags
SPOILER_KEYWORDS = {'#spoiler', '#spoilers', '#spoilerwarning', '#containsspoiler'}

# Step 1: List all JSON files in S3
def list_json_keys(bucket, prefix):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.json'):
                keys.append(obj['Key'])
    return keys

# Step 2: Fetch reviews from one file (sequential execution)
# boto3 client is created once per function call as it's sequential now.
def fetch_reviews_from_s3(key_bucket_pair):
    key, bucket = key_bucket_pair
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)
        valid_reviews = [r for r in data if isinstance(r, dict)]
        return valid_reviews, len(valid_reviews) # Return reviews and their count
    except Exception as e:
        print(f"Error reading {key}: {e}")
        return [], 0 # Return empty list and 0 count on error

# Step 3: Extract hashtags from a single review (sequential execution)
def extract_hashtags(review):
    text = review.get("review_detail", "") + " " + review.get("review_summary", "")
    raw_tags = re.findall(r'(?<!\w)#([a-zA-Z]{3,})\b', text)
    return [
        f"#{tag.lower()}"
        for tag in raw_tags
        if tag.lower() not in stop_tags
    ]

# Main
if __name__ == "__main__":
    start_total_time = time.time()

    print(" Listing S3 files...")
    keys = list_json_keys(S3_BUCKET, S3_PREFIX)
    print(f" Found {len(keys)} files. Loading reviews sequentially...")

    # Step 4: Fetch reviews from S3 sequentially
    all_reviews_and_counts_nested = []
    for key in keys:
        reviews_from_file, count_from_file = fetch_reviews_from_s3((key, S3_BUCKET))
        all_reviews_and_counts_nested.append((reviews_from_file, count_from_file))

    # Flatten all reviews and sum up total reviews processed
    reviews = []
    total_reviews_processed = 0
    for review_list_from_file, count_from_file in all_reviews_and_counts_nested:
        reviews.extend(review_list_from_file)
        total_reviews_processed += count_from_file

    print(f" Processing {len(reviews)} reviews for hashtags...")
    print(f" Total actual reviews processed from files: {total_reviews_processed}")

    # Step 5: Extract hashtags sequentially
    hashtags_nested = []
    for review_item in reviews:
        extracted_tags = extract_hashtags(review_item)
        hashtags_nested.append(extracted_tags)

    # Flatten and count
    all_tags = [tag for sublist in hashtags_nested for tag in sublist]
    hashtag_counter = Counter(all_tags)

    # Top hashtags (at least 2 times)
    top_hashtags = [(tag, count) for tag, count in hashtag_counter.most_common(20) if count >= 2]

    # Calculate spoiler percentage
    total_relevant_tags = sum(count for tag, count in hashtag_counter.items() if tag not in SPOILER_KEYWORDS)
    spoiler_tags_count = sum(hashtag_counter[tag] for tag in SPOILER_KEYWORDS if tag in hashtag_counter)
    
    # Calculate total tags extracted (excluding stop_tags)
    total_extracted_tags = len(all_tags)
    
    spoiler_percent = 0
    if total_extracted_tags > 0:
        spoiler_percent = round((spoiler_tags_count / total_extracted_tags) * 100, 2)

    # Output
    print("\n--- Hashtag Analysis Summary ---")
    print(" Top Hashtags:")
    if top_hashtags:
        for tag, count in top_hashtags:
            print(f"{tag}: {count}")
    else:
        print(" No top hashtags found (or none occurred at least twice).")

    end_total_time = time.time()
    total_pipeline_time = round(end_total_time - start_total_time, 2)

    # Calculate overall throughput and latency
    overall_throughput_reviews_per_sec = round(total_reviews_processed / total_pipeline_time, 2) if total_pipeline_time > 0 else 0
    overall_latency_sec_per_review = round(total_pipeline_time / total_reviews_processed, 4) if total_reviews_processed > 0 else 0

    print(f"\n Total Pipeline Execution Time: {total_pipeline_time} seconds")
    print(f" Overall Throughput: {overall_throughput_reviews_per_sec} reviews/second")
    print(f" Overall Latency: {overall_latency_sec_per_review} seconds/review")
    print(f" Spoiler Tags Percentage (of all extracted tags): {spoiler_percent}%")
