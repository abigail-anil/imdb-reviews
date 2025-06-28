import boto3
import json
import re
import time
from multiprocessing import Pool, cpu_count
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

# Step 2: Fetch reviews from one file
def fetch_reviews_from_s3(key_bucket_pair):
    key, bucket = key_bucket_pair
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)
        return [r for r in data if isinstance(r, dict)]
    except Exception as e:
        print(f"Error reading {key}: {e}")
        return []

# Step 3: Extract hashtags from a single review
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
    start = time.time()
    print(" Listing S3 files...")
    keys = list_json_keys(S3_BUCKET, S3_PREFIX)
    print(f" Found {len(keys)} files. Loading reviews in parallel...")

    # Step 4: Fetch reviews from S3 in parallel
    with Pool(cpu_count()) as pool:
        all_reviews_nested = pool.map(fetch_reviews_from_s3, [(key, S3_BUCKET) for key in keys])

    # Flatten all reviews
    reviews = [review for sublist in all_reviews_nested for review in sublist]
    print(f" Processing {len(reviews)} reviews for hashtags...")

    # Step 5: Extract hashtags in parallel
    with Pool(cpu_count()) as pool:
        hashtags_nested = pool.map(extract_hashtags, reviews)

    # Flatten and count
    all_tags = [tag for sublist in hashtags_nested for tag in sublist]
    hashtag_counter = Counter(all_tags)

    # Top hashtags (at least 2 times)
    top_hashtags = [(tag, count) for tag, count in hashtag_counter.most_common(20) if count >= 2]

    # Output
    print("\n Top Hashtags:")
    for tag, count in top_hashtags:
        print(f"{tag}: {count}")

    print(f"\n Total Time: {round(time.time() - start, 2)} seconds")

