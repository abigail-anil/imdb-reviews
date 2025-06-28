import os
import json
import time
import nltk
import boto3
from multiprocessing import Pool, cpu_count # Import Pool from multiprocessing
from multiprocessing.pool import ThreadPool # Keep ThreadPool for S3 I/O
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

# Load and preprocess reviews from each file (I/O bound)
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
        return texts, reviews_in_file_count #Return both the texts and the count
    except Exception as e:
        print(f" Error in {key}: {e}")
        return [], 0 #  Return empty list and 0 count on error

# Initialize VADER for each process
def init_analyzer_process():
    global _analyzer
    _analyzer = SentimentIntensityAnalyzer()

# Sentiment analysis function (CPU bound)
def analyze_sentiment_process(text):
    # Use the analyzer initialized in this process
    return _analyzer.polarity_scores(text)['compound']

if __name__ == '__main__':
    start_total_time = time.time() # Overall start time

    print(" Listing JSON files...")
    keys = list_json_keys(S3_BUCKET, S3_PREFIX)
    print(f" Found {len(keys)} files.")

    print(" Loading and flattening reviews with ThreadPool for I/O...")
    with ThreadPool(4) as pool: # 4 threads for I/O 
        all_reviews_and_counts_nested = pool.map(load_and_process_file, keys)

    # Flatten list of lists and sum total reviews
    texts = []
    total_reviews_processed = 0 # This will be the total count for throughput/latency
    for file_texts, file_review_count in all_reviews_and_counts_nested: #Loop to process results
        texts.extend(file_texts)
        total_reviews_processed += file_review_count # Sum up total reviews

    print(f" Total reviews loaded: {len(texts)}")
    # Print the actual total reviews from files
    print(f" Total actual reviews processed from files: {total_reviews_processed}")

    print(f" Performing sentiment analysis with Multiprocessing Pool ({cpu_count()} processes)...")
    start_sentiment = time.time() # This variable name remains as per your original script

    # Use multiprocessing.Pool for CPU bound sentiment analysis
    # Optimal processes = cpu_count()
    with Pool(processes=cpu_count(), initializer=init_analyzer_process) as pool:
        sentiments = pool.map(analyze_sentiment_process, texts, chunksize=max(1, len(texts) // (cpu_count() * 10)))

    avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else 0

    end_total_time = time.time() # Overall end time
    total_pipeline_time = round(end_total_time - start_total_time, 2) # Total pipeline time

    # Calculate overall throughput and latency
    overall_throughput_reviews_per_sec = round(total_reviews_processed / total_pipeline_time, 2) if total_pipeline_time > 0 else 0
    overall_latency_sec_per_review = round(total_pipeline_time / total_reviews_processed, 4) if total_reviews_processed > 0 else 0

    print(f" Average Sentiment Polarity: {round(avg_sentiment, 4)}")
    print(f" Sentiment Analysis Time: {round(time.time() - start_sentiment, 2)} seconds") 
    print(f" Total Time: {total_pipeline_time} seconds") # new total_pipeline_time

    # ADDED: Print throughput and latency
    print(f" Overall Throughput: {overall_throughput_reviews_per_sec} reviews/second")
    print(f" Overall Latency: {overall_latency_sec_per_review} seconds/review")
