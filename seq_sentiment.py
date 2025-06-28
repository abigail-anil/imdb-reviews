import os
import json
import time
import nltk
import boto3
from nltk.sentiment.vader import SentimentIntensityAnalyzer

#nltk.download('vader_lexicon', quiet=True) 

# S3 Configuration
S3_BUCKET = 'imdbreviews-scalable'
S3_PREFIX = 'input_files/'

def list_json_keys(bucket, prefix):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.json'):
                keys.append(obj['Key'])
    return keys

# Load and preprocess reviews from each file (now sequential execution)
# boto3 client is created once per function call as it's sequential now.
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


# Sentiment analysis function (now takes analyzer as argument)
def analyze_sentiment_sequential(text, analyzer):
    return analyzer.polarity_scores(text)['compound']

if __name__ == '__main__':
    start_total_time = time.time() # Overall start time

    print(" Listing JSON files...")
    keys = list_json_keys(S3_BUCKET, S3_PREFIX)
    print(f" Found {len(keys)} files.")

    print(" Loading and flattening reviews sequentially...")
    all_reviews_and_counts_nested = []
    for key in keys:
        result_texts, result_count = load_and_process_file(key)
        all_reviews_and_counts_nested.append((result_texts, result_count))

    # Flatten list of lists and sum total reviews
    texts = []
    total_reviews_processed = 0
    for file_texts, file_review_count in all_reviews_and_counts_nested:
        texts.extend(file_texts)
        total_reviews_processed += file_review_count

    print(f" Total reviews loaded: {len(texts)}")
    print(f" Total actual reviews processed from files: {total_reviews_processed}")

    print(" Performing sentiment analysis sequentially...")
    start_sentiment = time.time()

    sia_analyzer = SentimentIntensityAnalyzer()
    sentiments = []
    for text_item in texts:
        sentiment_score = analyze_sentiment_sequential(text_item, sia_analyzer)
        sentiments.append(sentiment_score)

    avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else 0

    end_total_time = time.time() # Overall end time
    total_pipeline_time = round(end_total_time - start_total_time, 2) # Total pipeline time

    # Calculate overall throughput and latency
    overall_throughput_reviews_per_sec = round(total_reviews_processed / total_pipeline_time, 2) if total_pipeline_time > 0 else 0
    overall_latency_sec_per_review = round(total_pipeline_time / total_reviews_processed, 4) if total_reviews_processed > 0 else 0

    print(f" Average Sentiment Polarity: {round(avg_sentiment, 4)}")
    print(f" Sentiment Analysis Time: {round(time.time() - start_sentiment, 2)} seconds")
    print(f" Total Time: {total_pipeline_time} seconds")

    print(f" Overall Throughput: {overall_throughput_reviews_per_sec} reviews/second")
    print(f" Overall Latency: {overall_latency_sec_per_review} seconds/review")
