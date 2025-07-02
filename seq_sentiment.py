import os
import json
import time
import boto3
from datetime import datetime
import re # For word tokenization
# from collections import Counter # Only needed if you plan to do word frequency counts

# AFINN Lexicon: This will be loaded from a file
AFINN_LEXICON_FILE = 'AFINN-111.txt'
AFINN_LEXICON = {}

# Sentiment Thresholds (Adjust these values as needed for your data)
# These thresholds define what constitutes positive, negative, or neutral sentiment
POSITIVE_SCORE_THRESHOLD = 0.5
NEGATIVE_SCORE_THRESHOLD = -0.5

# S3 Configuration
S3_BUCKET = 'imdbreviews-scalable'
S3_PREFIX = 'cleaned_files/'
SUMMARY_PREFIX = 'summaries/'
SUMMARY_TYPE_PREFIX = 'sentiment_sequential_summary_'

s3 = boto3.client('s3')

def load_afinn_lexicon(filepath: str = AFINN_LEXICON_FILE):
    """
    Loads the AFINN lexicon from a text file into the global AFINN_LEXICON dictionary.
    Each line in the file should be 'word\tscore'.
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) == 2:
                    word, score_str = parts
                    AFINN_LEXICON[word] = int(score_str)
        print(f"Loaded AFINN lexicon with {len(AFINN_LEXICON)} words from {filepath}.")
    except FileNotFoundError:
        print(f"Error: AFINN lexicon file not found at {filepath}. "
              "Sentiment analysis will return 0.0 for all reviews. "
              "Please ensure 'AFINN-111.txt' is in the script's directory.")
    except Exception as e:
        print(f"Error loading AFINN lexicon: {e}")

def list_json_keys(bucket, prefix):
    """Lists all JSON files in the specified S3 prefix."""
    paginator = s3.get_paginator('list_objects_v2')
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.json'):
                keys.append(obj['Key'])
    return keys

def delete_old_summaries():
    """Deletes existing sentiment sequential summary files from S3."""
    paginator = s3.get_paginator('list_objects_v2')
    keys_to_delete = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=SUMMARY_PREFIX):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.startswith(SUMMARY_PREFIX + SUMMARY_TYPE_PREFIX):
                keys_to_delete.append({'Key': key})
    if keys_to_delete:
        s3.delete_objects(Bucket=S3_BUCKET, Delete={'Objects': keys_to_delete})
        print(f"Deleted {len(keys_to_delete)} old summaries matching '{SUMMARY_TYPE_PREFIX}'.")
    else:
        print(f"No old summaries matching '{SUMMARY_TYPE_PREFIX}' found to delete.")

def load_and_process_file(key):
    """Loads and extracts review texts from a single S3 JSON file."""
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)
        texts = []
        reviews_in_file_count = 0

        if isinstance(data, list):
            for r in data:
                if isinstance(r, dict):
                    reviews_in_file_count += 1
                    text = (r.get('review_detail', '') + ' ' + r.get('review_summary', '')).strip()
                    if text:
                        texts.append(text)
        else:
            print(f"Warning: {key} does not contain a list of reviews. Skipping file.")
            return [], 0

        return texts, reviews_in_file_count
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from {key}: {e}")
        return [], 0
    except Exception as e:
        print(f"Error processing S3 file {key}: {e}")
        return [], 0

def analyze_sentiment_sequential(text: str) -> float:
    """
    Performs sentiment analysis using the loaded AFINN lexicon.
    Returns a normalized score (total score / number of sentiment words).
    """
    text = text.lower()
    text = re.sub(r'[^a-z\s]', '', text).strip()

    words = re.findall(r'\b[a-z]+\b', text)

    scores = []
    for word in words:
        if word in AFINN_LEXICON:
            scores.append(AFINN_LEXICON[word])
    
    return sum(scores) / len(scores) if scores else 0.0

def categorize_sentiment(score: float) -> str:
    """Categorizes a sentiment score into 'Positive', 'Neutral', or 'Negative'."""
    if score >= POSITIVE_SCORE_THRESHOLD:
        return "Positive"
    elif score <= NEGATIVE_SCORE_THRESHOLD:
        return "Negative"
    else:
        return "Neutral"

def save_summary_to_s3(summary_data):
    """Saves the final summary JSON to S3."""
    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%S-%f')
    key = f'{SUMMARY_PREFIX}{SUMMARY_TYPE_PREFIX}{timestamp}.json'
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(summary_data, indent=2),
        ContentType='application/json'
    )
    print(f"Summary saved to s3://{S3_BUCKET}/{key}")

if __name__ == '__main__':
    start_total_time = time.time()

    print("--- Starting Sequential Sentiment Analysis ---")

    print("Deleting existing sentiment_sequential_summary files from S3...")
    delete_old_summaries()

    load_afinn_lexicon()
    if not AFINN_LEXICON:
        print("Warning: AFINN lexicon is empty. Sentiment scores will default to 0.0.")

    print("Listing JSON files in S3...")
    keys = list_json_keys(S3_BUCKET, S3_PREFIX)
    print(f"Found {len(keys)} files.")

    print("Loading and flattening reviews sequentially...")
    all_reviews_and_counts_nested = []
    for key in keys:
        result_texts, result_count = load_and_process_file(key)
        all_reviews_and_counts_nested.append((result_texts, result_count))

    texts = []
    total_reviews_processed = 0
    for file_texts, file_review_count in all_reviews_and_counts_nested:
        texts.extend(file_texts)
        total_reviews_processed += file_review_count

    print(f"Total review texts extracted for analysis: {len(texts)}")
    print(f"Total raw reviews counted in files: {total_reviews_processed}")

    print("Performing sentiment analysis sequentially...")
    start_sentiment = time.time()

    sentiments = []
    positive_count = 0
    negative_count = 0
    neutral_count = 0

    for text_item in texts:
        sentiment_score = analyze_sentiment_sequential(text_item)
        sentiments.append(sentiment_score)
        
        # Categorize and count sentiments
        category = categorize_sentiment(sentiment_score)
        if category == "Positive":
            positive_count += 1
        elif category == "Negative":
            negative_count += 1
        else: # Neutral
            neutral_count += 1

    avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else 0.0

    end_total_time = time.time()
    total_pipeline_time = round(end_total_time - start_total_time, 2)

    overall_throughput_reviews_per_sec = round(total_reviews_processed / total_pipeline_time, 2) if total_pipeline_time > 0 else 0
    overall_latency_sec_per_review = round(total_pipeline_time / total_reviews_processed, 4) if total_reviews_processed > 0 else 0

    print("\n--- Analysis Results ---")
    print(f"Average Sentiment Polarity: {round(avg_sentiment, 4)}")
    print(f"Positive Sentiments: {positive_count}")
    print(f"Negative Sentiments: {negative_count}")
    print(f"Neutral Sentiments: {neutral_count}")
    print(f"Sentiment Analysis Phase Time: {round(time.time() - start_sentiment, 2)} seconds")
    print(f"Total Pipeline Execution Time: {total_pipeline_time} seconds")
    print(f"Overall Throughput: {overall_throughput_reviews_per_sec} reviews/second")
    print(f"Overall Latency: {overall_latency_sec_per_review} seconds/review")

    summary = {
        'method': 'Sentiment (Sequential - AFINN)',
        'total_reviews_processed': total_reviews_processed,
        'total_pipeline_time_sec': total_pipeline_time,
        'average_sentiment_polarity': round(avg_sentiment, 4),
        'sentiment_counts': {
            'Positive': positive_count,
            'Negative': negative_count,
            'Neutral': neutral_count,
            'Total_Categorized': positive_count + negative_count + neutral_count # Should equal len(texts)
        },
        'throughput_reviews_per_sec': overall_throughput_reviews_per_sec,
        'latency_sec_per_review': overall_latency_sec_per_review,
        'timestamp_utc': datetime.utcnow().isoformat() + "Z"
    }

    save_summary_to_s3(summary)
    print("\n--- Script Finished ---")
