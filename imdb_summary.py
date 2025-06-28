import boto3
import json
from collections import Counter, defaultdict
import re # Import regex for timestamp extraction

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

def summarize_outputs():
    keys = list_json_objects(S3_BUCKET, S3_PREFIX)
    print(f"Found {len(keys)} summary files")

    max_total_records_processed_overall = 0 # To store the ultimate total reviews

    total_sentiment = 0.0
    total_spoiler_count = 0
    total_duration = 0.0

    # These will now track weighted averages or sums
    weighted_sentiment_sum = 0.0
    weighted_spoiler_count_sum = 0.0
    total_records_for_averages = 0 # To track total records contributing to avg sentiment/spoiler

    # For avg latency and throughput, we need to sum up individual batch durations and records
    total_records_for_avg_latency_throughput = 0
    total_duration_for_avg_latency_throughput = 0.0


    word_counter = Counter()
    movie_sentiments = defaultdict(list)

    # To track the last seen overall records for correct total
    last_overall_records = 0

    # Sort keys by timestamp to ensure we process in order and get the latest overall count
    # Assuming filenames are like summary_2025-06-28T17-02-36-350486.json
    def extract_timestamp_from_key(key):
        match = re.search(r'summary_(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-\d+)\.json', key)
        if match:
            return match.group(1)
        return "" # Or handle error

    keys.sort(key=extract_timestamp_from_key)


    for key in keys:
        data = load_json_from_s3(key)

        # Update the maximum total records processed overall
        current_overall_records = data.get('total_records_processed_overall', 0)
        if current_overall_records > max_total_records_processed_overall:
            max_total_records_processed_overall = current_overall_records

        # Use records_in_batch for calculations that are *per batch*
        batch_reviews = data.get('records_in_batch', 0)
        current_duration = data.get('duration_sec_current_interval', 0.0)

        # Accumulate for average latency and throughput
        total_records_for_avg_latency_throughput += batch_reviews
        total_duration_for_avg_latency_throughput += current_duration


        # For avg_sentiment and avg_spoiler_percent, we need to weight them by the number of reviews in that batch
        # The 'avg_sentiment_window' and 'spoiler_percent_window' are averages for that specific window,
        # so we need to multiply by the batch_reviews to get the total "sentiment points" or "spoiler points" for that batch
        avg_sent = data.get('avg_sentiment_window')
        if avg_sent is not None and batch_reviews > 0:
            weighted_sentiment_sum += avg_sent * batch_reviews
            total_records_for_averages += batch_reviews # Add to the count for the overall weighted average

        spoiler_pct = data.get('spoiler_percent_window')
        if spoiler_pct is not None and batch_reviews > 0:
            weighted_spoiler_count_sum += (spoiler_pct / 100) * batch_reviews # Convert percentage to a proportion of reviews
            # No need to add to total_records_for_averages again, it's the same base count


        for word, count in data.get('top_words_window', []):
            word_counter[word] += count

        for movie, score in data.get('sentiment_by_movie_current_window', {}).items():
            movie_sentiments[movie].append(score)

    movie_avg_sentiment = {
        movie: round(sum(scores) / len(scores), 3)
        for movie, scores in movie_sentiments.items()
    }

    top_words = word_counter.most_common(10)
    top_movies = sorted(movie_avg_sentiment.items(), key=lambda x: x[1], reverse=True)[:10]

    summary = {
        'total_reviews': max_total_records_processed_overall, # This is the corrected value
        'total_duration_sec': round(total_duration_for_avg_latency_throughput, 2), # Sum of all individual batch durations
        'avg_throughput': round(total_records_for_avg_latency_throughput / total_duration_for_avg_latency_throughput, 2) if total_duration_for_avg_latency_throughput else None,
        'avg_latency_sec': round(total_duration_for_avg_latency_throughput / total_records_for_avg_latency_throughput, 4) if total_records_for_avg_latency_throughput else None,
        'avg_sentiment': round(weighted_sentiment_sum / total_records_for_averages, 4) if total_records_for_averages else None,
        'avg_spoiler_percent': round(100 * weighted_spoiler_count_sum / total_records_for_averages, 2) if total_records_for_averages else None,
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
