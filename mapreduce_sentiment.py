import os
import json
import time
import nltk
import boto3
from multiprocessing import Pool, cpu_count
from multiprocessing.pool import ThreadPool
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# nltk.download('vader_lexicon', quiet=True)

S3_BUCKET = 'imdbreviews-scalable'
S3_PREFIX = 'input_files/'
SUMMARY_PREFIX = 'summaries/'
SUMMARY_BASE_NAME = 'mapreduce_sentiment_summary'

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
        for k in keys_to_delete:
            print(f" - Deleting: {k}")
            s3.delete_object(Bucket=bucket, Key=k)
        print("Old summary files deleted.")
    else:
        print("No matching old summary files found.")

def load_and_process_file(key):
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)
        texts = []
        reviews_in_file_count = 0
        for r in data:
            if isinstance(r, dict):
                reviews_in_file_count += 1
                text = (r.get('review_detail', '') + ' ' + r.get('review_summary', '')).strip()
                if text:
                    texts.append(text)
        return texts, reviews_in_file_count
    except Exception as e:
        print(f" Error in {key}: {e}")
        return [], 0

def init_analyzer_process():
    global _analyzer
    _analyzer = SentimentIntensityAnalyzer()

def analyze_sentiment_process(text):
    return _analyzer.polarity_scores(text)['compound']

if __name__ == '__main__':
    start_total_time = time.time()

    print("Listing JSON files from input directory...")
    keys = list_json_keys(S3_BUCKET, S3_PREFIX)
    print(f"Found {len(keys)} files.")

    print("Loading and flattening reviews with ThreadPool for I/O...")
    with ThreadPool(4) as pool:
        all_reviews_and_counts_nested = pool.map(load_and_process_file, keys)

    texts = []
    total_reviews_processed = 0
    for file_texts, file_review_count in all_reviews_and_counts_nested:
        texts.extend(file_texts)
        total_reviews_processed += file_review_count

    print(f"Total reviews loaded: {len(texts)}")
    print(f"Total actual reviews processed: {total_reviews_processed}")

    print(f"Performing sentiment analysis using {cpu_count()} CPU cores...")
    start_sentiment = time.time()
    with Pool(processes=cpu_count(), initializer=init_analyzer_process) as pool:
        sentiments = pool.map(analyze_sentiment_process, texts, chunksize=max(1, len(texts) // (cpu_count() * 10)))

    avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else 0
    end_total_time = time.time()
    total_pipeline_time = round(end_total_time - start_total_time, 2)
    overall_throughput = round(total_reviews_processed / total_pipeline_time, 2) if total_pipeline_time > 0 else 0
    overall_latency = round(total_pipeline_time / total_reviews_processed, 4) if total_reviews_processed > 0 else 0

    print(f"Average Sentiment: {round(avg_sentiment, 4)}")
    print(f"Sentiment Analysis Time: {round(time.time() - start_sentiment, 2)} seconds")
    print(f"Total Pipeline Time: {total_pipeline_time} seconds")
    print(f"Throughput: {overall_throughput} reviews/sec")
    print(f"Latency: {overall_latency} sec/review")

    summary_data = {
        "method": "Parallel Sentiment Analysis",
        "total_files_processed": len(keys),
        "total_reviews_processed": total_reviews_processed,
        "total_pipeline_time_sec": total_pipeline_time,
        "average_sentiment": round(avg_sentiment, 4),
        "sentiment_analysis_time_sec": round(time.time() - start_sentiment, 2),
        "throughput_reviews_per_sec": overall_throughput,
        "latency_sec_per_review": overall_latency,
        "timestamp_utc": time.strftime("%Y-%m-%dT%H-%M-%S", time.gmtime())
    }

    print("Cleaning up existing summary files...")
    delete_existing_summaries(S3_BUCKET, SUMMARY_PREFIX, SUMMARY_BASE_NAME)

    summary_key = f"{SUMMARY_PREFIX}{SUMMARY_BASE_NAME}_{int(time.time())}.json"
    try:
        s3 = boto3.client('s3')
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=summary_key,
            Body=json.dumps(summary_data, indent=2),
            ContentType='application/json'
        )
        print(f"Sentiment summary saved to s3://{S3_BUCKET}/{summary_key}")
    except Exception as e:
        print(f"Failed to upload summary: {e}")

