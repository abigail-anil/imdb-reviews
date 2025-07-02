import boto3
import json
from collections import Counter, defaultdict
import re

S3_BUCKET = 'imdbreviews-scalable'
S3_PREFIX = 'kafka-consumer-outputs/'

s3 = boto3.client('s3')

def list_json_objects(bucket, prefix):
    objects = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            objects.extend(obj['Key'] for obj in page['Contents'] if obj['Key'].endswith('.json'))
    return objects

def load_json_from_s3(key):
    response = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return json.loads(response['Body'].read().decode('utf-8'))

def extract_timestamp_from_key(key):
    match = re.search(r'summary_(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-\d+)\.json', key)
    return match.group(1) if match else ""

def summarize_outputs():
    keys = sorted(list_json_objects(S3_BUCKET, S3_PREFIX), key=extract_timestamp_from_key)
    if not keys:
        print("No summary files found.")
        return

    # Latest file is assumed to be the last in sorted list
    final_key = keys[-1]
    final_summary = load_json_from_s3(final_key)

    max_total_records_processed_overall = 0
    total_sentiment_weighted = 0.0
    total_reviews_for_sentiment = 0
    total_duration = 0.0
    total_records = 0
    total_run_time_sec = final_summary.get('total_run_time_sec', 0.0)

    word_counter = Counter()
    movie_sentiments = defaultdict(list)

    for key in keys:
        data = load_json_from_s3(key)

        current_total = data.get('total_records_processed_overall', 0)
        if current_total > max_total_records_processed_overall:
            max_total_records_processed_overall = current_total

        count = data.get('records_in_batch', 0)
        duration = data.get('duration_sec_current_interval', 0.0)

        total_duration += duration
        total_records += count

        avg_sent = data.get('avg_sentiment_window')
        if avg_sent is not None and count > 0:
            total_sentiment_weighted += avg_sent * count
            total_reviews_for_sentiment += count

        for word, count in data.get('top_words_window', []):
            word_counter[word] += count

        for movie, score in data.get('sentiment_by_movie_current_window', {}).items():
            movie_sentiments[movie].append(score)

    avg_sentiment = round(total_sentiment_weighted / total_reviews_for_sentiment, 4) if total_reviews_for_sentiment else None
    avg_throughput = round(total_records / total_duration, 2) if total_duration else None
    avg_latency = round(total_duration / total_records, 4) if total_records else None

    top_words = word_counter.most_common(10)
    movie_avg_sentiment = {
        movie: round(sum(scores) / len(scores), 3)
        for movie, scores in movie_sentiments.items()
    }
    top_movies = sorted(movie_avg_sentiment.items(), key=lambda x: x[1], reverse=True)[:10]

    summary = {
        'total_reviews': max_total_records_processed_overall,
        'total_run_time_sec': total_run_time_sec,
        'avg_sentiment': avg_sentiment,
        'avg_throughput': avg_throughput,
        'avg_latency_sec': avg_latency,
        'positive_count_total': final_summary.get('positive_count_total', 0),
        'neutral_count_total': final_summary.get('neutral_count_total', 0),
        'negative_count_total': final_summary.get('negative_count_total', 0),
        'top_10_words': top_words,
        'top_10_movies_sentiment': top_movies
    }

    print("\nFinal Aggregated Summary:")
    for k, v in summary.items():
        if isinstance(v, list):
            print(f"{k}:")
            for item in v:
                print(f"   {item}")
        else:
            print(f"{k}: {v}")

if __name__ == "__main__":
    summarize_outputs()

