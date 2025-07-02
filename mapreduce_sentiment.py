import os
import json
import time
import boto3
from multiprocessing import Pool, cpu_count
from multiprocessing.pool import ThreadPool
from botocore.config import Config
import logging
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# S3 Configuration
S3_BUCKET = 'imdbreviews-scalable'
S3_PREFIX = 'cleaned_files/'

# Configure Boto3 client for better performance
BOTO_CONFIG = Config(
    read_timeout=900,
    connect_timeout=900,
    retries={'max_attempts': 10, 'mode': 'standard'}
)

# --- AFINN Lexicon Configuration ---
AFINN_LEXICON_FILE = 'AFINN-111.txt' # Make sure this file is in the same directory as your script
AFINN_LEXICON = {} # This dictionary will hold our loaded lexicon

# --- Sentiment Thresholds (Adjust these values as needed for your data) ---
# AFINN scores are typically -5 to +5.
# A score of 0.0 generally means no sentiment words were found or positive/negative balanced.
POSITIVE_SCORE_THRESHOLD = 0.5   # Reviews with an average word score > 0.5 will be positive
NEGATIVE_SCORE_THRESHOLD = -0.5  # Reviews with an average word score < -0.5 will be negative
# Reviews between -0.5 and 0.5 (inclusive) will be considered neutral.

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
        logging.info(f"Process {os.getpid()}: Loaded AFINN lexicon with {len(AFINN_LEXICON)} words.")
    except FileNotFoundError:
        logging.error(f"Process {os.getpid()}: AFINN lexicon file not found at {filepath}. "
                      "Sentiment analysis will use an empty lexicon and return 0.0 for all reviews. "
                      "Please ensure 'AFINN-111.txt' is in the script's directory.")
    except Exception as e:
        logging.error(f"Process {os.getpid()}: Error loading AFINN lexicon from {filepath}: {e}")

# --- S3 Functions ---
def list_json_keys(bucket: str, prefix: str) -> list[str]:
    """Lists all JSON keys in a given S3 bucket and prefix."""
    s3 = boto3.client('s3', config=BOTO_CONFIG)
    paginator = s3.get_paginator('list_objects_v2')
    keys = []
    try:
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        for page in pages:
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('.json') and not key.endswith('/'):
                    keys.append(key)
    except ClientError as e:
        logging.error(f"Error listing S3 keys: {e}")
        raise
    return keys

def load_and_process_file(key: str) -> tuple[list[str], int]:
    """
    Loads and preprocesses reviews from a single S3 JSON file.
    Returns a tuple of (list of review texts, count of reviews in file).
    """
    s3 = boto3.client('s3', config=BOTO_CONFIG)
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)
        
        texts: list[str] = []
        reviews_in_file_count = 0

        if isinstance(data, list):
            for r in data:
                if isinstance(r, dict):
                    reviews_in_file_count += 1
                    review_detail = r.get('review_detail', '')
                    review_summary = r.get('review_summary', '')
                    text = f"{review_detail} {review_summary}".strip()
                    if text:
                        texts.append(text)
        elif isinstance(data, dict):
             reviews_in_file_count = 1
             review_detail = data.get('review_detail', '')
             review_summary = data.get('review_summary', '')
             text = f"{review_detail} {review_summary}".strip()
             if text:
                 texts.append(text)

        return texts, reviews_in_file_count
    except Exception as e:
        logging.error(f"Error processing file {key}: {e}")
        return [], 0

# --- Sentiment Analysis Function using Loaded AFINN Lexicon ---
def analyze_sentiment_process(text: str) -> float:
    """
    Performs sentiment analysis using the loaded AFINN lexicon.
    Returns a normalized score (total score / number of sentiment words).
    Scores typically range from -5.0 to +5.0 if at least one sentiment word is found.
    """
    words = re.findall(r'\b[a-z]+\b', text.lower())
    
    total_afinn_score = 0
    sentiment_word_count = 0

    for word in words:
        if word in AFINN_LEXICON:
            total_afinn_score += AFINN_LEXICON[word]
            sentiment_word_count += 1
            
    if sentiment_word_count == 0:
        return 0.0
        
    return total_afinn_score / sentiment_word_count

# --- Initializer for Multiprocessing Pool ---
def init_analyzer_process():
    """
    Initializer function for the multiprocessing Pool.
    Ensures the AFINN lexicon is loaded into each child process.
    """
    load_afinn_lexicon()

if __name__ == '__main__':
    start_total_time = time.time()

    logging.info("Listing JSON files...")
    keys = list_json_keys(S3_BUCKET, S3_PREFIX)
    if not keys:
        logging.warning("No JSON files found. Exiting.")
        exit()
    logging.info(f"Found {len(keys)} files.")

    logging.info("Loading and flattening reviews with ThreadPool for I/O...")
    num_io_threads = min(max(cpu_count() * 2, 8), 64)
    logging.info(f"Using {num_io_threads} threads for S3 I/O.")
    
    start_io_time = time.time()
    with ThreadPool(num_io_threads) as pool:
        all_reviews_and_counts_nested = pool.map(load_and_process_file, keys)
    io_time = round(time.time() - start_io_time, 2)
    logging.info(f"S3 I/O and initial processing time: {io_time} seconds.")

    texts: list[str] = []
    total_reviews_processed = 0
    for file_texts, file_review_count in all_reviews_and_counts_nested:
        texts.extend(file_texts)
        total_reviews_processed += file_review_count

    if not texts:
        logging.warning("No review texts loaded after processing files. Exiting.")
        exit()

    logging.info(f"Total review texts loaded for analysis: {len(texts)}")
    logging.info(f"Total actual reviews counted from files: {total_reviews_processed}")

    num_processes = cpu_count()
    logging.info(f"Performing sentiment analysis with Multiprocessing Pool ({num_processes} processes)...")
    start_sentiment = time.time()

    target_chunks = max(num_processes * 4, 100)
    chunk_size = max(1, len(texts) // target_chunks)
    logging.info(f"Using chunk size: {chunk_size} for sentiment analysis.")

    with Pool(processes=num_processes, initializer=init_analyzer_process) as pool:
        sentiments = pool.map(analyze_sentiment_process, texts, chunksize=chunk_size)

    sentiment_analysis_time = round(time.time() - start_sentiment, 2)
    logging.info(f"Sentiment Analysis Time: {sentiment_analysis_time} seconds")

    avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else 0

    # --- Sentiment Categorization and Counting ---
    positive_count = 0
    neutral_count = 0
    negative_count = 0

    for score in sentiments:
        if score >= POSITIVE_SCORE_THRESHOLD:
            positive_count += 1
        elif score <= NEGATIVE_SCORE_THRESHOLD:
            negative_count += 1
        else:
            neutral_count += 1
    # --- End Sentiment Categorization ---

    end_total_time = time.time()
    total_pipeline_time = round(end_total_time - start_total_time, 2)

    overall_throughput_reviews_per_sec = round(total_reviews_processed / total_pipeline_time, 2) if total_pipeline_time > 0 else 0
    overall_latency_sec_per_review = round(total_pipeline_time / total_reviews_processed, 4) if total_reviews_processed > 0 else 0

    logging.info(f"--- Sentiment Analysis Results ---")
    logging.info(f"Average Sentiment Polarity: {round(avg_sentiment, 4)}")
    logging.info(f"Total Positive Sentiments: {positive_count}")
    logging.info(f"Total Neutral Sentiments: {neutral_count}")
    logging.info(f"Total Negative Sentiments: {negative_count}")
    logging.info(f"Total reviews classified: {positive_count + neutral_count + negative_count}") # Should match len(texts)
    
    logging.info(f"--- Overall Pipeline Metrics ---")
    logging.info(f"Total Time: {total_pipeline_time} seconds")
    logging.info(f"Overall Throughput: {overall_throughput_reviews_per_sec} reviews/second")
    logging.info(f"Overall Latency: {overall_latency_sec_per_review} seconds/review")
    logging.info(" Sentiment analysis complete.")
