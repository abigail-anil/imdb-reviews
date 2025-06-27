import os
import json
import time
import boto3
from collections import Counter, deque, defaultdict
from kafka import KafkaConsumer
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk

# One-time download (comment out after first run)
#nltk.download('vader_lexicon')

# Config
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "imdb-reviews")
GROUP_ID = "imdb-consumer-group"
S3_BUCKET = "imdbreviews-scalable"
S3_PREFIX = "kafka-consumer-outputs/"
WINDOW_SECONDS = 300
MAX_WORKERS = 4
BATCH_SIZE = 1000
BATCH_THREAD_SIZE = 250

s3 = boto3.client('s3')
sia = SentimentIntensityAnalyzer()

recent_words = deque()
recent_sentiments = deque()
recent_spoilers = deque()
recent_movies = deque()
sentiment_by_movie = defaultdict(list)
spoiler_tags_counter = Counter()

stop_words = {
    'the', 'and', 'to', 'of', 'a', 'in', 'it', 'is', 'i', 'that',
    'this', 'was', 'for', 'on', 'with', 'as', 'but', 'at', 'by',
    'an', 'be', 'from', 'are', 'have', 'has', 'not', 'they', 'you',
    'there', 'he', 'his', 'she', 'her', 'them'
}

def delete_existing_outputs():
    print(f"🫹 Deleting existing S3 files under s3://{S3_BUCKET}/{S3_PREFIX}")
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        if 'Contents' in page:
            objects = [{'Key': obj['Key']} for obj in page['Contents']]
            s3.delete_objects(Bucket=S3_BUCKET, Delete={'Objects': objects})
            print(f"   Deleted {len(objects)} files.")
        else:
            print("   No files to delete.")

def clean_deques(now):
    cutoff = now - WINDOW_SECONDS
    global recent_words, recent_sentiments, recent_spoilers, recent_movies
    recent_words = deque([w for w in recent_words if w[1] > cutoff])
    recent_sentiments = deque([s for s in recent_sentiments if s[1] > cutoff])
    recent_spoilers = deque([s for s in recent_spoilers if s[1] > cutoff])
    recent_movies = deque([m for m in recent_movies if m[1] > cutoff])

def process_batch(records):
    now = time.time()
    results = []
    for record in records:
        try:
            review = json.loads(record)
            text = review.get('review_detail', '') + ' ' + review.get('review_summary', '')
            sentiment = sia.polarity_scores(text)['compound']
            words = [w for w in text.lower().split() if w.isalpha() and w not in stop_words]
            movie = review.get('movie', 'unknown')
            is_spoiler = review.get('spoiler_tag', 0) == 1
            spoiler_type = review.get('spoiler_type', 'general') if is_spoiler else None

            results.append({
                "words": [(w, now) for w in words],
                "sentiment": (sentiment, now),
                "spoiler": (is_spoiler, now),
                "movie": (movie, now),
                "sentiment_movie": (movie, sentiment),
                "spoiler_type": spoiler_type
            })
        except Exception:
            continue
    return results

def save_summary_to_s3(summary):
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S-%f")
    key = f"{S3_PREFIX}summary_{timestamp}.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(summary, indent=2),
        ContentType='application/json'
    )
    print(f"✅ Saved summary to s3://{S3_BUCKET}/{key}")

def main():
    delete_existing_outputs()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        group_id=GROUP_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: x.decode('utf-8'),
        max_poll_records=BATCH_SIZE,
        fetch_max_bytes=8 * 1024 * 1024,
        fetch_min_bytes=1024 * 64,
    )

    print("⏳ Waiting for partition assignment...")
    while not consumer.assignment():
        consumer.poll(timeout_ms=100)

    print(f"🔁 Listening to Kafka topic: {TOPIC}")
    start_time = time.time()
    total_records = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        while True:
            records = consumer.poll(timeout_ms=1000)
            raw_batch = []
            for tp, msgs in records.items():
                raw_batch.extend([msg.value for msg in msgs])

            if not raw_batch:
                continue

            futures = [executor.submit(process_batch, raw_batch[i:i+BATCH_THREAD_SIZE])
                       for i in range(0, len(raw_batch), BATCH_THREAD_SIZE)]

            for future in as_completed(futures):
                for result in future.result():
                    now = time.time()
                    recent_words.extend(result["words"])
                    recent_sentiments.append(result["sentiment"])
                    recent_spoilers.append(result["spoiler"])
                    recent_movies.append(result["movie"])
                    sentiment_by_movie[result["sentiment_movie"][0]].append(result["sentiment_movie"][1])
                    if result["spoiler_type"]:
                        spoiler_tags_counter[result["spoiler_type"]] += 1

            total_records += len(raw_batch)
            print(f"📅 Processed {len(raw_batch)} records, total so far: {total_records}")
            clean_deques(time.time())

            duration = time.time() - start_time
            throughput = total_records / duration if duration else 0
            latency = duration / total_records if total_records else 0
            avg_sentiment = sum(s for s, _ in recent_sentiments) / len(recent_sentiments) if recent_sentiments else None
            spoiler_percent = 100 * sum(1 for s, _ in recent_spoilers if s) / len(recent_spoilers) if recent_spoilers else None
            movie_counts = Counter(m for m, _ in recent_movies)
            top_words = Counter(w for w, _ in recent_words).most_common(5)
            top_movies = movie_counts.most_common(3)
            top_10 = [m for m, _ in movie_counts.most_common(10)]
            sentiment_avg = {m: round(sum(v)/len(v), 3) for m, v in sentiment_by_movie.items() if m in top_10}

            summary = {
                "records_in_batch": len(raw_batch),
                "duration_sec": round(duration, 2),
                "avg_latency_sec": round(latency, 4),
                "throughput": round(throughput, 2),
                "avg_sentiment": avg_sentiment,
                "spoiler_percent": spoiler_percent,
                "top_words": top_words,
                "top_movies": top_movies,
                "spoiler_tags": spoiler_tags_counter.most_common(3),
                "sentiment_by_movie": sentiment_avg
            }

            print("\n📊 Streaming Summary:")
            print(json.dumps(summary, indent=2))
            save_summary_to_s3(summary)

if __name__ == "__main__":
    main()

