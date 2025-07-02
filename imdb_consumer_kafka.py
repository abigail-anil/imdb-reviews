import os
import json
import time
import re
import boto3
from collections import Counter, deque, defaultdict
from kafka import KafkaConsumer
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool, cpu_count

# --- AFINN Configuration ---
AFINN_LEXICON_FILE = 'AFINN-111.txt'
AFINN_LEXICON = {}
POSITIVE_SCORE_THRESHOLD = 0.5
NEGATIVE_SCORE_THRESHOLD = -0.5

# --- Config ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "172.31.30.123:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "imdb-reviews")
GROUP_ID = "imdb-consumer-group"
S3_BUCKET = "imdbreviews-scalable"
S3_PREFIX = "kafka-consumer-outputs/"
WINDOW_SECONDS = 300

MAX_PROCESSES = cpu_count()
BATCH_SIZE = 1000
TRIM_MAX_SENTENCES = 5

recent_words = deque()
recent_sentiments = deque()
recent_movies = deque()
sentiment_by_movie = defaultdict(list)

stop_words_main = {
    'the', 'and', 'to', 'of', 'a', 'in', 'it', 'is', 'i', 'that',
    'this', 'was', 'for', 'on', 'with', 'as', 'but', 'at', 'by',
    'an', 'be', 'from', 'are', 'have', 'has', 'not', 'they', 'you',
    'there', 'he', 'his', 'she', 'her', 'them'
}

def load_afinn_lexicon(filepath=AFINN_LEXICON_FILE):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) == 2:
                    word, score_str = parts
                    AFINN_LEXICON[word] = int(score_str)
        print(f"AFINN lexicon loaded with {len(AFINN_LEXICON)} words.")
    except Exception as e:
        print(f"Error loading AFINN lexicon: {e}")

def afinn_sentiment(text: str) -> float:
    words = re.findall(r'\b[a-z]+\b', text.lower())
    total_score = 0
    count = 0
    for word in words:
        if word in AFINN_LEXICON:
            total_score += AFINN_LEXICON[word]
            count += 1
    return total_score / count if count else 0.0

def init_worker():
    load_afinn_lexicon()

def process_single_record(record_str):
    try:
        review = json.loads(record_str)
        text = review.get('review_detail', '') + ' ' + review.get('review_summary', '')
        text = " ".join(text.split('.')[:TRIM_MAX_SENTENCES])
        sentiment = afinn_sentiment(text)
        words = [w for w in text.lower().split() if w.isalpha() and w not in stop_words_main]
        movie = review.get('movie', 'unknown')

        sentiment_category = (
            "positive" if sentiment >= POSITIVE_SCORE_THRESHOLD else
            "negative" if sentiment <= NEGATIVE_SCORE_THRESHOLD else
            "neutral"
        )

        return {
            "words": words,
            "sentiment": sentiment,
            "sentiment_category": sentiment_category,
            "movie": movie,
            "sentiment_movie_data": (movie, sentiment)
        }
    except Exception:
        return None

def delete_existing_outputs_s3():
    s3 = boto3.client('s3')
    print(f"Deleting existing S3 files in s3://{S3_BUCKET}/{S3_PREFIX}...")
    paginator = s3.get_paginator('list_objects_v2')
    deleted = 0
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        if 'Contents' in page:
            keys = [{'Key': obj['Key']} for obj in page['Contents']]
            s3.delete_objects(Bucket=S3_BUCKET, Delete={'Objects': keys})
            deleted += len(keys)
    print(f"Deleted {deleted} old files from S3.")

def save_summary_to_s3(summary):
    s3 = boto3.client('s3')
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S-%f")
    key = f"{S3_PREFIX}summary_{timestamp}.json"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(summary, indent=2), ContentType='application/json')
    print(f"Saved summary to s3://{S3_BUCKET}/{key}")

def clean_deques(now):
    cutoff = now - WINDOW_SECONDS
    global recent_words, recent_sentiments, recent_movies
    recent_words = deque([w for w in recent_words if w[1] > cutoff])
    recent_sentiments = deque([s for s in recent_sentiments if s[1] > cutoff])
    recent_movies = deque([m for m in recent_movies if m[1] > cutoff])

def main():
    print("Starting Kafka Consumer...")
    delete_existing_outputs_s3()
    executor = ThreadPoolExecutor(max_workers=1)

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

    print("Waiting for Kafka partition assignment...")
    while not consumer.assignment():
        consumer.poll(timeout_ms=100)

    print(f"Listening to Kafka topic: {TOPIC}")
    start_time_summary_interval = time.time()
    total_records_processed_overall = 0
    last_poll_time = time.time()
    INACTIVITY_TIMEOUT_SEC = 60

    positive_count_total = 0
    neutral_count_total = 0
    negative_count_total = 0

    try:
        with Pool(processes=MAX_PROCESSES, initializer=init_worker) as process_pool:
            while True:
                records_polled = consumer.poll(timeout_ms=1000)
                raw_batch = []
                for tp, msgs in records_polled.items():
                    raw_batch.extend([msg.value for msg in msgs])

                if not raw_batch:
                    time.sleep(0.1)
                    if time.time() - last_poll_time > INACTIVITY_TIMEOUT_SEC:
                        print("No new records received. Stopping consumer.")
                        break
                    if time.time() - start_time_summary_interval > WINDOW_SECONDS / 2:
                        clean_deques(time.time())
                    continue

                last_poll_time = time.time()
                processed_results = process_pool.map(process_single_record, raw_batch, chunksize=max(1, len(raw_batch) // MAX_PROCESSES // 2))
                valid_results = [r for r in processed_results if r]
                current_timestamp = time.time()

                for result in valid_results:
                    recent_words.extend([(w, current_timestamp) for w in result["words"]])
                    recent_sentiments.append((result["sentiment"], current_timestamp))
                    recent_movies.append((result["movie"], current_timestamp))
                    sentiment_by_movie[result["sentiment_movie_data"][0]].append(result["sentiment_movie_data"][1])

                    if result["sentiment_category"] == "positive":
                        positive_count_total += 1
                    elif result["sentiment_category"] == "neutral":
                        neutral_count_total += 1
                    elif result["sentiment_category"] == "negative":
                        negative_count_total += 1

                total_records_processed_overall += len(valid_results)
                print(f"Processed {len(valid_results)} records, total: {total_records_processed_overall}")

                if time.time() - start_time_summary_interval > 5:
                    clean_deques(time.time())
                    duration = time.time() - start_time_summary_interval
                    throughput = len(valid_results) / duration if duration else 0
                    latency = duration / len(valid_results) if valid_results else 0
                    avg_sentiment = sum(s for s, _ in recent_sentiments) / len(recent_sentiments) if recent_sentiments else None

                    movie_counts = Counter(m for m, _ in recent_movies)
                    top_movies = [m for m, _ in movie_counts.most_common(10)]
                    sentiment_avg_by_movie = {
                        m: round(sum(sentiment_by_movie[m]) / len(sentiment_by_movie[m]), 3)
                        for m in top_movies if m in sentiment_by_movie
                    }

                    summary = {
                        "timestamp_utc": datetime.utcnow().isoformat(),
                        "records_in_batch": len(valid_results),
                        "total_records_processed_overall": total_records_processed_overall,
                        "duration_sec_current_interval": round(duration, 2),
                        "avg_latency_sec": round(latency, 4),
                        "throughput_rec_per_sec": round(throughput, 2),
                        "avg_sentiment_window": avg_sentiment,
                        "top_words_window": Counter(w for w, _ in recent_words).most_common(5),
                        "top_movies_window": movie_counts.most_common(3),
                        "sentiment_by_movie_current_window": sentiment_avg_by_movie,
                        "recent_words_count_in_window": len(recent_words),
                        "recent_sentiments_count_in_window": len(recent_sentiments),
                        "positive_count_total": positive_count_total,
                        "neutral_count_total": neutral_count_total,
                        "negative_count_total": negative_count_total
                    }

                    print("Saving periodic summary...")
                    executor.submit(save_summary_to_s3, summary)
                    start_time_summary_interval = time.time()

    finally:
        print("Final shutdown. Saving last summary...")
        clean_deques(time.time())
        duration = time.time() - start_time_summary_interval
        total_run_time = time.time() - script_start_time

        if duration <= 0: duration = 1

        avg_sentiment = sum(s for s, _ in recent_sentiments) / len(recent_sentiments) if recent_sentiments else None
        movie_counts = Counter(m for m, _ in recent_movies)
        top_movies = [m for m, _ in movie_counts.most_common(10)]
        sentiment_avg_by_movie = {
            m: round(sum(sentiment_by_movie[m]) / len(sentiment_by_movie[m]), 3)
            for m in top_movies if m in sentiment_by_movie
        }

        final_summary = {
            "timestamp_utc": datetime.utcnow().isoformat(),
            "total_run_time_sec": round(total_run_time, 2),
            "records_in_batch": len(recent_sentiments),
            "total_records_processed_overall": total_records_processed_overall,
            "duration_sec_current_interval": round(duration, 2),
            "avg_latency_sec": round(duration / len(recent_sentiments), 4) if recent_sentiments else 0,
            "throughput_rec_per_sec": round(len(recent_sentiments) / duration, 2) if recent_sentiments else 0,
            "avg_sentiment_window": avg_sentiment,
            "top_words_window": Counter(w for w, _ in recent_words).most_common(5),
            "top_movies_window": movie_counts.most_common(3),
            "sentiment_by_movie_current_window": sentiment_avg_by_movie,
            "recent_words_count_in_window": len(recent_words),
            "recent_sentiments_count_in_window": len(recent_sentiments),
            "positive_count_total": positive_count_total,
            "neutral_count_total": neutral_count_total,
            "negative_count_total": negative_count_total
        }

        executor.submit(save_summary_to_s3, final_summary)
        executor.shutdown(wait=True)
        consumer.close()
        print("Consumer shut down completed.")

if __name__ == "__main__":
    script_start_time = time.time()
    main()
