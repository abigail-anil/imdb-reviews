import boto3
import json
import time
from collections import Counter
from datetime import datetime

S3_BUCKET = 'imdbreviews-scalable'
S3_PREFIX = 'cleaned_files/'
SUMMARY_OUTPUT_PREFIX = 'summaries/'
SUMMARY_PREFIX_MATCH = 'wordcount_sequential_summary_'

STOP_WORDS = {
    'the', 'and', 'to', 'of', 'a', 'in', 'it', 'is', 'i', 'that',
    'this', 'was', 'for', 'on', 'with', 'as', 'but', 'at', 'by',
    'an', 'be', 'from', 'are', 'have', 'has', 'not', 'they', 'you',
    'there', 'he', 'his', 'she', 'her', 'them', 'or', 'so', 'if',
    'my', 'we', 'our', 'their', 'what', 'who', 'will', 'just'
}

def delete_old_summaries():
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    keys_to_delete = []

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=SUMMARY_OUTPUT_PREFIX):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.startswith(SUMMARY_OUTPUT_PREFIX + SUMMARY_PREFIX_MATCH):
                keys_to_delete.append({'Key': key})

    if keys_to_delete:
        print(f" Deleting {len(keys_to_delete)} old wordcount sequential summaries...")
        s3.delete_objects(Bucket=S3_BUCKET, Delete={'Objects': keys_to_delete})
    else:
        print(" No old wordcount sequential summaries found.")

def list_json_keys(bucket, prefix):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.json'):
                keys.append(obj['Key'])
    return keys

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

def save_summary_to_s3(summary_data):
    s3 = boto3.client('s3')
    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%S-%f')
    filename = f"{SUMMARY_PREFIX_MATCH}{timestamp}.json"
    s3_key = SUMMARY_OUTPUT_PREFIX + filename
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(summary_data, indent=2),
        ContentType='application/json'
    )
    print(f"\n Saved summary to s3://{S3_BUCKET}/{s3_key}")

# Main
if __name__ == '__main__':
    delete_old_summaries()

    start_time = time.time()

    print(" Listing input files...")
    keys = list_json_keys(S3_BUCKET, S3_PREFIX)
    print(f" Found {len(keys)} JSON files.")

    print(" Running sequential word count...")
    mapped_results = []
    for key in keys:
        result = map_wordcount_from_file(key)
        mapped_results.append(result)

    print(" Reducing word counts...")
    final_word_counts, total_reviews_processed = reduce_counts(mapped_results)

    end_time = time.time()
    total_mapreduce_time = round(end_time - start_time, 2)

    throughput = round(total_reviews_processed / total_mapreduce_time, 2) if total_mapreduce_time > 0 else 0
    latency = round(total_mapreduce_time / total_reviews_processed, 4) if total_reviews_processed > 0 else 0

    print("\n Summary:")
    print(f"Total files: {len(keys)}")
    print(f"Total reviews: {total_reviews_processed}")
    print(f"Time taken: {total_mapreduce_time} seconds")
    print(f"Throughput: {throughput} reviews/sec")
    print(f"Latency: {latency} sec/review")

    top_words = final_word_counts.most_common(10)
    print("\n Top 10 Words:")
    for word, count in top_words:
        print(f"{word}: {count}")

    summary = {
        'method': 'Word Count (Sequential)',
        'total_reviews': total_reviews_processed,
        'total_files': len(keys),
        'total_time_sec': total_mapreduce_time,
        'throughput': throughput,
        'latency': latency,
        'top_10_words': top_words
    }

    save_summary_to_s3(summary)

