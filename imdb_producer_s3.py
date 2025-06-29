import boto3
import json
import time
from kafka import KafkaProducer
from concurrent.futures import ThreadPoolExecutor, as_completed

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'imdb-reviews'
THREAD_COUNT = 5

# S3 configuration
S3_BUCKET = 'imdbreviews-scalable'
S3_PREFIX = 'input_files/'  

# Initialize Kafka producer with recommended configs
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=10,
    batch_size=64 * 1024,
    compression_type='gzip',
    max_request_size=1048576,
    request_timeout_ms=30000,
    retries=5
)

# Initialize S3 client
s3 = boto3.client('s3')  

def list_json_keys(bucket, prefix):
    paginator = s3.get_paginator('list_objects_v2')
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.json'):
                keys.append(obj['Key'])
    return keys

def load_all_reviews(bucket, keys):
    all_reviews = []
    for key in keys:
        try:
            print(f"Reading {key}")
            response = s3.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read().decode('utf-8')
            data = json.loads(content)
            if isinstance(data, list):
                all_reviews.extend(data)
        except Exception as e:
            print(f"Error reading {key}: {e}")
    return all_reviews

def send_to_kafka(record):
    try:
        producer.send(TOPIC_NAME, value=record)
        # time.sleep(0.001)  
        return True
    except Exception as e:
        print(f"Kafka send failed: {e}")
        return False

if __name__ == "__main__":
    print("Listing JSON files in S3...")
    json_keys = list_json_keys(S3_BUCKET, S3_PREFIX)
    print(f"Found {len(json_keys)} JSON files.")

    print("Downloading and parsing reviews...")
    all_reviews = load_all_reviews(S3_BUCKET, json_keys)
    print(f"Loaded {len(all_reviews)} reviews.")

    print("Sending reviews to Kafka in parallel...")
    start_time = time.time()
    success_count = 0

    with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
        futures = [executor.submit(send_to_kafka, review) for review in all_reviews]
        for i, future in enumerate(as_completed(futures), 1):
            if future.result():
                success_count += 1
            if i % 5000 == 0:
                elapsed = round(time.time() - start_time, 2)
                print(f"Sent {i}/{len(all_reviews)} reviews... Elapsed: {elapsed}s")

    try:
        producer.flush(timeout=30)
        producer.close()
    except Exception as e:
        print(f"Kafka producer close failed: {e}")

    duration = round(time.time() - start_time, 2)
    print(f"\nStreamed {success_count}/{len(all_reviews)} reviews in {duration} seconds.")

