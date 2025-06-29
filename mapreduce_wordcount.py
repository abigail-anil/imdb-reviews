import boto3
import json
import time
from datetime import datetime
from multiprocessing import Pool, cpu_count
from collections import Counter

S3_BUCKET = 'imdbreviews-scalable'
S3_PREFIX = 'input_files/'
SUMMARY_PREFIX = 'summaries/'  # Target folder for storing summary JSON
SUMMARY_BASE_NAME = 'mapreduce_wordcount_summary'  # Prefix used to delete old files

STOP_WORDS = {
    'the', 'and', 'to', 'of', 'a', 'in', 'it', 'is', 'i', 'that',
    'this', 'was', 'for', 'on', 'with', 'as', 'but', 'at', 'by',
    'an', 'be', 'from', 'are', 'have', 'has', 'not', 'they', 'you',
    'there', 'he', 'his', 'she', 'her', 'them', 'or', 'so', 'if',
    'my', 'we', 'our', 'their', 'what', 'who', 'will', 'just'
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
    print("Checking and deleting old summary files if any...")
    s3 = boto3.client('s3')
    keys = list_json_keys(bucket, prefix)
    keys_to_delete = [k for k in keys if k.startswith(prefix + base_name)]
    
    if keys_to_delete:
        print(f"Found {len(keys_to_delete)} old summary file(s) to delete:")
        for k in keys_to_delete:
            print(f" - {k}")
            s3.delete_object(Bucket=bucket, Key=k)
        print("Old summary files deleted.")
    else:
        print("No matching old summary files found.")

def map_wordcount_from_file(key):
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)

        counter = Counter()
        reviews_in_file = 0
        for r in data:
            if isinstance(r, dict):
                reviews_in_file += 1
                text = r.get('review_detail', '') + ' ' + r.get('review_summary', '')
                words = [
                    word.lower()
                    for word in text.split()
                    if word.isalpha() and word.lower() not in STOP_WORDS
                ]
                counter.update(words)
        return counter, reviews_in_file
    except Exception as e:
        print(f"Error processing {key}: {e}")
        return Counter(), 0

def reduce_counts(mapped_results):
    final_word_counts = Counter()
    total_reviews_processed = 0
    for partial_word_count, reviews_count_in_file in mapped_results:
        final_word_counts.update(partial_word_count)
        total_reviews_processed += reviews_count_in_file
    return final_word_counts, total_reviews_processed

if __name__ == '__main__':
    start_time = time.time()

    print("Listing input files from S3...")
    keys = list_json_keys(S3_BUCKET, S3_PREFIX)
    print(f"Found {len(keys)} JSON input files.")

    print("Running parallel Map step...")
    with Pool(cpu_count()) as pool:
        mapped_results = pool.map(map_wordcount_from_file, keys)

    print("Reducing results...")
    final_word_counts, total_reviews_processed = reduce_counts(mapped_results)

    end_time = time.time()
    total_mapreduce_time = round(end_time - start_time, 2)

    throughput = round(total_reviews_processed / total_mapreduce_time, 2) if total_mapreduce_time > 0 else 0
    latency = round(total_mapreduce_time / total_reviews_processed, 4) if total_reviews_processed > 0 else 0

    print("\n--- MapReduce Summary ---")
    print(f"Total files processed: {len(keys)}")
    print(f"Total reviews processed: {total_reviews_processed}")
    print(f"Total MapReduce Time: {total_mapreduce_time} seconds")
    print(f"Overall Throughput: {throughput} reviews/second")
    print(f"Overall Latency: {latency} seconds/review")

    print("\nTop 10 Words (excluding stop words):")
    top_words = final_word_counts.most_common(10)
    for word, count in top_words:
        print(f"{word}: {count}")

    # --- Save summary to S3 ---
    summary_data = {
        "method": "Word Count (Parallel)",
        "total_files_processed": len(keys),
        "total_reviews_processed": total_reviews_processed,
        "total_mapreduce_time_sec": total_mapreduce_time,
        "throughput_reviews_per_sec": throughput,
        "latency_sec_per_review": latency,
        "top_10_words": top_words,
        "timestamp_utc": datetime.utcnow().isoformat()
    }

    delete_existing_summaries(S3_BUCKET, SUMMARY_PREFIX, SUMMARY_BASE_NAME)

    summary_key = f"{SUMMARY_PREFIX}{SUMMARY_BASE_NAME}_{int(time.time())}.json"
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=summary_key,
        Body=json.dumps(summary_data, indent=2),
        ContentType='application/json'
    )
    print(f"\nSummary saved to s3://{S3_BUCKET}/{summary_key}")

