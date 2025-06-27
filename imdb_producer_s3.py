import boto3
import json
import time
from kafka import KafkaProducer
from concurrent.futures import ThreadPoolExecutor, as_completed

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'  # 🔁 Replace if needed
TOPIC_NAME = 'imdb-reviews'
THREAD_COUNT = 5  # Reduced for better stability

# S3 configuration
S3_BUCKET = 'imdbreviews-scalable'
S3_KEY = 'input_files/sample.json'

# Initialize Kafka producer with safer config
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=10,
    batch_size=32 * 1024,
    max_request_size=1048576,
    request_timeout_ms=30000,
    retries=5
)

# Initialize S3 client
s3 = boto3.client('s3')

# Download and load the JSON data from S3
print("⬇️ Downloading from S3...")
response = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
data = json.loads(response['Body'].read().decode('utf-8'))
print(f"📦 Loaded {len(data)} records from S3")

def send_to_kafka(record):
    try:
        producer.send(TOPIC_NAME, value=record)
        time.sleep(0.001)  # Slight delay to avoid overwhelming broker
        return True
    except Exception as e:
        print(f"❌ Failed to send record: {e}")
        return False

# ⏱ Start parallel streaming
print("🚀 Starting parallel Kafka publishing...")
start_time = time.time()
success_count = 0

with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
    futures = [executor.submit(send_to_kafka, review) for review in data]
    for future in as_completed(futures):
        if future.result():
            success_count += 1

# ✅ Flush and close the producer safely
try:
    producer.flush(timeout=30)
    producer.close()
except Exception as e:
    print(f"⚠️ Flush/close failed: {e}")

end_time = time.time()
duration = round(end_time - start_time, 2)

print(f"\n✅ Streamed {success_count}/{len(data)} reviews in {duration} seconds.")

