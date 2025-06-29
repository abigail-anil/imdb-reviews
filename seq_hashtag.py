import boto3
import json
import re
import time
from collections import Counter
from datetime import datetime

# Config
S3_BUCKET = 'imdbreviews-scalable'
S3_PREFIX = 'input_files/'
SUMMARY_PREFIX = 'summaries/'
SUMMARY_TYPE_PREFIX = 'hashtag_sequential_summary_'

# Stop-word-style banned hashtags
stop_tags = {
    'the', 'and', 'you', 'but', 'was', 'are', 'for', 'have', 'not',
    'his', 'her', 'she', 'there', 'with', 'this', 'that', 'what',
    'in', 'on', 'out', 'at', 'by', 'all', 'spoiler', 'movie', 'film',
    'one', 'more', 'some', 'just', 'from', 'like', 'contains', 'commentessentially'
}

# Spoiler tags
SPOILER_KEYWORDS = {'#spoiler', '#spoilers', '#spoilerwarning', '#containsspoiler'}

s3 = boto3.client('s3')

def list_json_keys(bucket, prefix):
    paginator = s3.get_paginator('list_objects_v2')
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.json'):
                keys.append(obj['Key'])
    return keys

def delete_old_summaries():
    paginator = s3.get_paginator('list_objects_v2')
    keys_to_delete = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=SUMMARY_PREFIX):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.startswith(SUMMARY_PREFIX + SUMMARY_TYPE_PREFIX):
                keys_to_delete.append({'Key': key})
    if keys_to_delete:
        s3.delete_objects(Bucket=S3_BUCKET, Delete={'Objects': keys_to_delete})

def fetch_reviews_from_s3(key_bucket_pair):
    key, bucket = key_bucket_pair
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

def save_summary_to_s3(summary):
    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%S-%f')
    key = f'{SUMMARY_PREFIX}{SUMMARY_TYPE_PREFIX}{timestamp}.json'
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(summary, indent=2),
        ContentType='application/json'
    )
    print(f"Summary saved to s3://{S3_BUCKET}/{key}")

if __name__ == "__main__":
    start_total_time = time.time()

    print("Deleting existing hashtag_sequential_summary files from S3...")
    delete_old_summaries()

    print("Listing S3 files...")
    keys = list_json_keys(S3_BUCKET, S3_PREFIX)
    print(f"Found {len(keys)} files. Loading reviews sequentially...")

    all_reviews_and_counts_nested = []
    for key in keys:
        reviews_from_file, count_from_file = fetch_reviews_from_s3((key, S3_BUCKET))
        all_reviews_and_counts_nested.append((reviews_from_file, count_from_file))

    reviews = []
    total_reviews_processed = 0
    for review_list_from_file, count_from_file in all_reviews_and_counts_nested:
        reviews.extend(review_list_from_file)
        total_reviews_processed += count_from_file

    print(f"Processing {len(reviews)} reviews for hashtags...")

    hashtags_nested = []
    for review_item in reviews:
        extracted_tags = extract_hashtags(review_item)
        hashtags_nested.append(extracted_tags)

    all_tags = [tag for sublist in hashtags_nested for tag in sublist]
    hashtag_counter = Counter(all_tags)

    top_hashtags = [(tag, count) for tag, count in hashtag_counter.most_common(20) if count >= 2]

    total_extracted_tags = len(all_tags)
    spoiler_tags_count = sum(hashtag_counter[tag] for tag in SPOILER_KEYWORDS if tag in hashtag_counter)
    spoiler_percent = round((spoiler_tags_count / total_extracted_tags) * 100, 2) if total_extracted_tags > 0 else 0

    end_total_time = time.time()
    total_pipeline_time = round(end_total_time - start_total_time, 2)
    overall_throughput_reviews_per_sec = round(total_reviews_processed / total_pipeline_time, 2) if total_pipeline_time > 0 else 0
    overall_latency_sec_per_review = round(total_pipeline_time / total_reviews_processed, 4) if total_reviews_processed > 0 else 0

    print("\n--- Hashtag Analysis Summary ---")
    if top_hashtags:
        for tag, count in top_hashtags:
            print(f"{tag}: {count}")
    else:
        print("No top hashtags found.")

    print(f"\nTotal Time: {total_pipeline_time} seconds")
    print(f"Throughput: {overall_throughput_reviews_per_sec} reviews/second")
    print(f"Latency: {overall_latency_sec_per_review} seconds/review")
    print(f"Spoiler Tags Percentage: {spoiler_percent}%")

    summary = {
        'method': 'Hashtag (Sequential)',
        'total_reviews': total_reviews_processed,
        'total_time_sec': total_pipeline_time,
        'throughput': overall_throughput_reviews_per_sec,
        'latency': overall_latency_sec_per_review,
        'spoiler_percent': spoiler_percent,
        'top_10_hashtags': top_hashtags
    }

    save_summary_to_s3(summary)

