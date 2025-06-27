import boto3
import json
from collections import Counter, defaultdict

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
    print(f"📂 Found {len(keys)} summary files")

    total_reviews = 0
    total_sentiment = 0.0
    total_spoiler_count = 0
    duration_sec = 0.0

    word_counter = Counter()
    movie_sentiments = defaultdict(list)

    for key in keys:
        data = load_json_from_s3(key)
        batch_reviews = data.get('records_in_batch', 0)
        total_reviews += batch_reviews

        duration_sec = max(duration_sec, data.get('duration_sec', 0.0))

        # Weighted sentiment
        if data.get('avg_sentiment') is not None:
            total_sentiment += data['avg_sentiment'] * batch_reviews

        # Weighted spoiler percent
        if data.get('spoiler_percent') is not None:
            total_spoiler_count += (data['spoiler_percent'] / 100) * batch_reviews

        # Accumulate word frequencies
        for word, count in data.get('top_words', []):
            word_counter[word] += count

        # Collect sentiment scores by movie
        for movie, sentiment in data.get('sentiment_by_movie', {}).items():
            movie_sentiments[movie].append(sentiment)

    # Compute average sentiment per movie
    movie_avg_sentiment = {
        movie: round(sum(scores) / len(scores), 3)
        for movie, scores in movie_sentiments.items()
    }

    # Top 10 by frequency
    top_words = word_counter.most_common(10)
    top_movies = sorted(movie_avg_sentiment.items(), key=lambda x: x[1], reverse=True)[:10]

    # Final aggregation
    summary = {
        'total_reviews': total_reviews,
        'total_duration_sec': round(duration_sec, 2),
        'avg_throughput': round(total_reviews / duration_sec, 2) if duration_sec else None,
        'avg_latency_sec': round(duration_sec / total_reviews, 4) if total_reviews else None,
        'avg_sentiment': round(total_sentiment / total_reviews, 4) if total_reviews else None,
        'avg_spoiler_percent': round(100 * total_spoiler_count / total_reviews, 2) if total_reviews else None,
        'top_10_words': top_words,
        'top_10_movies_sentiment': top_movies
    }

    print("\n📊 Final Aggregated Summary:")
    for k, v in summary.items():
        if isinstance(v, list):
            print(f"{k}:")
            for item in v:
                print(f"   {item}")
        else:
            print(f"{k}: {v}")

if __name__ == "__main__":
    summarize_outputs()

