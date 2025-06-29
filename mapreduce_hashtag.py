import boto3
import json
import re
import time
from multiprocessing import Pool, cpu_count
from collections import Counter

# Config
S3_BUCKET = 'imdbreviews-scalable'
S3_PREFIX = 'input_files/'
SUMMARY_PREFIX = 'summaries/'
SUMMARY_BASE_NAME = 'mapreduce_hashtag_summary'

# Stop-word-style banned hashtags
stop_tags = {
    'the', 'and', 'you', 'but', 'was', 'are', 'for', 'have', 'not',
    'his', 'her', 'she', 'there', 'with', 'this', 'that', 'what',
    'in', 'on', 'out', 'at', 'by', 'all', 'spoiler', 'movie', 'film',
    'one', 'more', 'some', 'just', 'from', 'like', 'contains', 'commentessentially'
}

def list_json_keys(bucket, prefix):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.json'):
                keys.append(obj['Key'])
    return keys

def delete_existing_summaries(bucket, prefix, base_name):
    print(f"Checking for old summaries with prefix '{prefix + base_name}'...")
    s3 = boto3.client('s3')
    all_keys = list_json_keys(bucket, prefix)
    matching_keys = [key for key in all_keys if key.startswith(prefix + base_name)]
    
    if matching_keys:
        for key in matching_keys:
            print(f" - Deleting: {key}")
            s3.delete_object(Bucket=bucket, Key=key)
        print("Old summaries deleted.")
    else:
        print("No old summaries found to delete.")

def fetch_reviews_from_s3(key_bucket_pair):
    key, bucket = key_bucket_pair
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)
        valid_reviews = [r for r in data if isinstance(r, dict)]
        return valid_reviews, len(valid_reviews)
    except Exception as e:
        print(f"Error reading {key}: {e}")
        return [], 0

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

    print("Listing S3 review files...")
    keys = list_json_keys(S3_BUCKET, S3_PREFIX)
    print(f"Found {len(keys)} files. Loading reviews in parallel...")

    with Pool(cpu_count()) as pool:
        all_reviews_and_counts_nested = pool.map(fetch_reviews_from_s3, [(key, S3_BUCKET) for key in keys])

    reviews = []
    total_reviews_processed = 0
    for review_list, count in all_reviews_and_counts_nested:
        reviews.extend(review_list)
        total_reviews_processed += count

    print(f"Processing {len(reviews)} reviews for hashtags...")
    print(f"Total reviews processed: {total_reviews_processed}")

    with Pool(cpu_count()) as pool:
        hashtags_nested = pool.map(extract_hashtags, reviews)

    all_tags = [tag for sublist in hashtags_nested for tag in sublist]
    hashtag_counter = Counter(all_tags)

    top_hashtags = [(tag, count) for tag, count in hashtag_counter.most_common(20) if count >= 2]

    print("\nTop Hashtags:")
    for tag, count in top_hashtags:
        print(f"{tag}: {count}")

    end = time.time()
    total_time = round(end - start, 2)

    throughput = round(total_reviews_processed / total_time, 2) if total_time > 0 else 0
    latency = round(total_time / total_reviews_processed, 4) if total_reviews_processed > 0 else 0

    print(f"\nTotal Time: {total_time} seconds")
    print(f"Throughput: {throughput} reviews/second")
    print(f"Latency: {latency} seconds/review")

    # Prepare summary data
    summary_data = {
        "method": "Parallel Hashtag Analysis",
        "total_files_processed": len(keys),
        "total_reviews_processed": total_reviews_processed,
        "top_hashtags": top_hashtags,
        "total_time_sec": total_time,
        "throughput_reviews_per_sec": throughput,
        "latency_sec_per_review": latency,
        "timestamp_utc": time.strftime("%Y-%m-%dT%H-%M-%S", time.gmtime())
    }

    # Delete old summaries with same prefix
    delete_existing_summaries(S3_BUCKET, SUMMARY_PREFIX, SUMMARY_BASE_NAME)

    # Save new summary
    summary_key = f"{SUMMARY_PREFIX}{SUMMARY_BASE_NAME}_{int(time.time())}.json"
    try:
        s3 = boto3.client('s3')
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=summary_key,
            Body=json.dumps(summary_data, indent=2),
            ContentType='application/json'
        )
        print(f"\nHashtag summary saved to s3://{S3_BUCKET}/{summary_key}")
    except Exception as e:
        print(f"Failed to upload summary to S3: {e}")

