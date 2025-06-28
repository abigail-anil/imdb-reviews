import os
import json
import time
import boto3
from collections import Counter, deque, defaultdict
from kafka import KafkaConsumer
from datetime import datetime
from multiprocessing import Pool, cpu_count
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.tokenize import sent_tokenize

# One-time NLTK downloads (uncomment and run once if you haven't already)
# nltk.download('vader_lexicon', quiet=True)
# nltk.download('punkt', quiet=True)

# Config
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "imdb-reviews")
GROUP_ID = "imdb-consumer-group" # Change this if you want to start fresh without resetting offsets manually
S3_BUCKET = "imdbreviews-scalable"
S3_PREFIX = "kafka-consumer-outputs/"
WINDOW_SECONDS = 300 # 5 minutes for rolling window calculations

# Multiprocessing configuration
MAX_PROCESSES = cpu_count() # Use all available CPU cores for sentiment analysis
BATCH_SIZE = 1000 # Max records to poll from Kafka at once
TRIM_MAX_SENTENCES = 5 # Number of sentences to trim reviews to for faster processing

# Global data structures for aggregation in the main process
recent_words = deque()
recent_sentiments = deque()
recent_spoilers = deque()
recent_movies = deque()
sentiment_by_movie = defaultdict(list)
spoiler_tags_counter = Counter()

# Stop words set - used in main process for initial definitions, and in each worker process
stop_words_main = {
    'the', 'and', 'to', 'of', 'a', 'in', 'it', 'is', 'i', 'that',
    'this', 'was', 'for', 'on', 'with', 'as', 'but', 'at', 'by',
    'an', 'be', 'from', 'are', 'have', 'has', 'not', 'they', 'you',
    'there', 'he', 'his', 'she', 'her', 'them'
}

# Functions for Worker Processes (executed in parallel)
def init_worker():
    """
    Initializes NLTK's SentimentIntensityAnalyzer and stop_words in each new worker process.
    This is crucial for multiprocessing as processes do not share global objects directly.
    """
    global _sia, _stop_words_worker, _sent_tokenize

    # Import modules within the worker's scope
    from nltk.sentiment.vader import SentimentIntensityAnalyzer
    from nltk.tokenize import sent_tokenize

    _sia = SentimentIntensityAnalyzer()
    # Define stop_words specifically for the worker process
    _stop_words_worker = {
        'the', 'and', 'to', 'of', 'a', 'in', 'it', 'is', 'i', 'that',
        'this', 'was', 'for', 'on', 'with', 'as', 'but', 'at', 'by',
        'an', 'be', 'from', 'are', 'have', 'has', 'not', 'they', 'you',
        'there', 'he', 'his', 'she', 'her', 'them'
    }
    _sent_tokenize = sent_tokenize # Assign to global for use in process_single_record

def process_single_record(record_str):
    """
    Processes a single Kafka record string: trims text, performs sentiment analysis,
    extracts words, movie info, and spoiler tags.
    This function runs in a separate process.
    """
    try:
        review = json.loads(record_str)
        text = review.get('review_detail', '') + ' ' + review.get('review_summary', '')

        # Trim the text to a maximum number of sentences for performance
        sentences = _sent_tokenize(text) # Use the imported sent_tokenize
        trimmed_text = " ".join(sentences[:TRIM_MAX_SENTENCES])

        sentiment = _sia.polarity_scores(trimmed_text)['compound'] # Use the initialized _sia
        words = [w for w in trimmed_text.lower().split() if w.isalpha() and w not in _stop_words_worker] # Use _stop_words_worker
        movie = review.get('movie', 'unknown')
        is_spoiler = review.get('spoiler_tag', 0) == 1
        spoiler_type = review.get('spoiler_type', 'general') if is_spoiler else None

        return {
            "words": words,
            "sentiment": sentiment,
            "spoiler": is_spoiler,
            "movie": movie,
            "sentiment_movie_data": (movie, sentiment), # This is used to append to sentiment_by_movie in main thread
            "spoiler_type": spoiler_type
        }
    except Exception:
        # Silently skip records that cause an error during processing
        return None # Return None for failed records

# Functions for Main Process (handles Kafka polling, aggregation, and S3 writes)
def delete_existing_outputs_s3():
    """
    Deletes all existing summary files in the specified S3 prefix.
    """
    s3_client = boto3.client('s3') # Create client in this function's scope
    print(f"Deleting existing S3 files under s3://{S3_BUCKET}/{S3_PREFIX}")
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        if 'Contents' in page:
            objects = [{'Key': obj['Key']} for obj in page['Contents']]
            s3_client.delete_objects(Bucket=S3_BUCKET, Delete={'Objects': objects})
            print(f"    Deleted {len(objects)} files.")
        else:
            print("    No files to delete.")

def save_summary_to_s3(summary):
    """
    Saves the aggregated summary to an S3 bucket with a unique timestamped key.
    """
    s3_client = boto3.client('s3') # Create client in this function's scope
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S-%f")
    key = f"{S3_PREFIX}summary_{timestamp}.json"
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(summary, indent=2),
        ContentType='application/json'
    )
    print(f"Saved summary to s3://{S3_BUCKET}/{key}")

def clean_deques(now):
    """
    Removes old data from the deques based on the sliding window.
    """
    cutoff = now - WINDOW_SECONDS
    global recent_words, recent_sentiments, recent_spoilers, recent_movies
    recent_words = deque([w for w in recent_words if w[1] > cutoff])
    recent_sentiments = deque([s for s in recent_sentiments if s[1] > cutoff])
    recent_spoilers = deque([s for s in recent_spoilers if s[1] > cutoff])
    recent_movies = deque([m for m in recent_movies if m[1] > cutoff])
	
def main():
    delete_existing_outputs_s3()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        group_id=GROUP_ID,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: x.decode('utf-8'),
        max_poll_records=BATCH_SIZE,
        fetch_max_bytes=8 * 1024 * 1024,
        fetch_min_bytes=1024 * 64,
        session_timeout_ms=10000,
        request_timeout_ms=15000,
    )

    print("Waiting for partition assignment...")
    while not consumer.assignment():
        consumer.poll(timeout_ms=100)

    print(f"Listening to Kafka topic: {TOPIC}")
    start_time_summary_interval = time.time()
    total_records_processed_overall = 0
    last_poll_time = time.time() # Track last time we received records

    # Add a timeout for inactivity to trigger a final summary
    INACTIVITY_TIMEOUT_SEC = 60 # e.g., 60 seconds of no new records

    try: # Use a try-finally block for graceful shutdown and final summary
        with Pool(processes=MAX_PROCESSES, initializer=init_worker) as process_pool:
            while True:
                records_polled = consumer.poll(timeout_ms=1000) # Poll for 1 second
                raw_batch = []
                for tp, msgs in records_polled.items():
                    raw_batch.extend([msg.value for msg in msgs])

                if not raw_batch:
                    # No new records in this poll
                    time.sleep(0.1) # Short sleep to avoid busy-waiting

                    # Check for prolonged inactivity
                    if time.time() - last_poll_time > INACTIVITY_TIMEOUT_SEC:
                        print(f"No records received for {INACTIVITY_TIMEOUT_SEC} seconds. Initiating graceful shutdown and final summary save.")
                        break # Exit the loop to trigger finally block

                    if time.time() - start_time_summary_interval > WINDOW_SECONDS / 2:
                        clean_deques(time.time())
                    continue

                # Records were received, update last_poll_time
                last_poll_time = time.time()

                processed_results = process_pool.map(
                    process_single_record,
                    raw_batch,
                    chunksize=max(1, len(raw_batch) // MAX_PROCESSES // 2)
                )

                valid_results = [r for r in processed_results if r]
                current_timestamp = time.time()

                for result in valid_results:
                    recent_words.extend([(w, current_timestamp) for w in result["words"]])
                    recent_sentiments.append((result["sentiment"], current_timestamp))
                    recent_spoilers.append((result["spoiler"], current_timestamp))
                    recent_movies.append((result["movie"], current_timestamp))
                    sentiment_by_movie[result["sentiment_movie_data"][0]].append(result["sentiment_movie_data"][1])
                    if result["spoiler_type"]:
                        spoiler_tags_counter[result["spoiler_type"]] += 1

                total_records_processed_overall += len(valid_results)
                print(f"Processed {len(valid_results)} records, total so far: {total_records_processed_overall}")

                # Check if it's time to generate and save a summary
                # This '5' second interval is what you had, keep it
                if time.time() - start_time_summary_interval > 5:
                    clean_deques(time.time())

                    duration_current_interval = time.time() - start_time_summary_interval
                    throughput = len(valid_results) / duration_current_interval if duration_current_interval else 0
                    latency = duration_current_interval / len(valid_results) if valid_results else 0
                    avg_sentiment_window = sum(s for s, _ in recent_sentiments) / len(recent_sentiments) if recent_sentiments else None
                    spoiler_percent_window = 100 * sum(1 for s, _ in recent_spoilers if s) / len(recent_spoilers) if recent_spoilers else None

                    movie_counts = Counter(m for m, _ in recent_movies)
                    top_movies_for_sentiment = [m for m, _ in movie_counts.most_common(10)]

                    sentiment_avg_current_window = {
                        m: round(sum(sentiment_by_movie[m]) / len(sentiment_by_movie[m]), 3)
                        for m in top_movies_for_sentiment if m in sentiment_by_movie
                    }

                    top_words_window = Counter(w for w, _ in recent_words).most_common(5)
                    top_movies_window = movie_counts.most_common(3)

                    summary = {
                        "timestamp_utc": datetime.utcnow().isoformat(),
                        "records_in_batch": len(valid_results),
                        "total_records_processed_overall": total_records_processed_overall, # This will have the correct value
                        "duration_sec_current_interval": round(duration_current_interval, 2),
                        "avg_latency_sec": round(latency, 4),
                        "throughput_rec_per_sec": round(throughput, 2),
                        "avg_sentiment_window": avg_sentiment_window,
                        "spoiler_percent_window": spoiler_percent_window,
                        "top_words_window": top_words_window,
                        "top_movies_window": top_movies_window,
                        "spoiler_tags_total": spoiler_tags_counter.most_common(3),
                        "sentiment_by_movie_current_window": sentiment_avg_current_window,
                        "recent_words_count_in_window": len(recent_words),
                        "recent_sentiments_count_in_window": len(recent_sentiments)
                    }

                    print("\nStreaming Summary:")
                    print(json.dumps(summary, indent=2))
                    save_summary_to_s3(summary)
                    start_time_summary_interval = time.time()

    finally:
        # This block will execute when the loop breaks or an error occurs
        print("\nConsumer shutting down. Saving final summary...")
        clean_deques(time.time()) # Clean deques one last time

        # Calculate final summary based on the state at shutdown
        # Note: These metrics might be for the "last window" or accumulated depending on your design
        # For simplicity, let's just make sure total_records_processed_overall is correct
        # and other metrics reflect the last state of the deques.

        # Recalculate metrics for the final summary (similar to the interval summary)
        # You might want to adjust how `duration_current_interval` is calculated here
        # for the "final" summary, perhaps just use the last interval's duration,
        # or a very small dummy duration if no records were processed recently.
        # For simplicity, we'll just ensure total_records_processed_overall is accurate.

        duration_final_interval = time.time() - start_time_summary_interval # Duration since last save
        if duration_final_interval <= 0: # Avoid division by zero if less than 1 second has passed
            duration_final_interval = 1 # Use a small default

        # These calculations will now reflect the *entire* recent window at shutdown
        final_avg_sentiment_window = sum(s for s, _ in recent_sentiments) / len(recent_sentiments) if recent_sentiments else None
        final_spoiler_percent_window = 100 * sum(1 for s, _ in recent_spoilers if s) / len(recent_spoilers) if recent_spoilers else None

        final_movie_counts = Counter(m for m, _ in recent_movies)
        final_top_movies_for_sentiment = [m for m, _ in final_movie_counts.most_common(10)]

        final_sentiment_avg_current_window = {
            m: round(sum(sentiment_by_movie[m]) / len(sentiment_by_movie[m]), 3)
            for m in final_top_movies_for_sentiment if m in sentiment_by_movie
        }

        final_top_words_window = Counter(w for w, _ in recent_words).most_common(5)
        final_top_movies_window = final_movie_counts.most_common(3)


        final_summary = {
            "timestamp_utc": datetime.utcnow().isoformat(),
            "records_in_batch": total_records_processed_overall - (summary.get("total_records_processed_overall", 0) if 'summary' in locals() else 0), # Records since last save
            "total_records_processed_overall": total_records_processed_overall, # This will be the *true* final count
            "duration_sec_current_interval": round(duration_final_interval, 2),
            "avg_latency_sec": round(duration_final_interval / (total_records_processed_overall - (summary.get("total_records_processed_overall", 0) if 'summary' in locals() else 0)) if (total_records_processed_overall - (summary.get("total_records_processed_overall", 0) if 'summary' in locals() else 0)) > 0 else 0, 4),
            "throughput_rec_per_sec": round((total_records_processed_overall - (summary.get("total_records_processed_overall", 0) if 'summary' in locals() else 0)) / duration_final_interval if duration_final_interval > 0 else 0, 2),
            "avg_sentiment_window": final_avg_sentiment_window,
            "spoiler_percent_window": final_spoiler_percent_window,
            "top_words_window": final_top_words_window,
            "top_movies_window": final_top_movies_window,
            "spoiler_tags_total": spoiler_tags_counter.most_common(3),
            "sentiment_by_movie_current_window": final_sentiment_avg_current_window,
            "recent_words_count_in_window": len(recent_words),
            "recent_sentiments_count_in_window": len(recent_sentiments)
        }

        print("\nFinal Shutdown Summary:")
        print(json.dumps(final_summary, indent=2))
        save_summary_to_s3(final_summary)

    consumer.close()

if __name__ == "__main__":
    main()
