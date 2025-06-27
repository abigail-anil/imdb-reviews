import boto3
import json
from multiprocessing import Pool
from collections import Counter
import time

# Step 1: Read JSON reviews from S3
def load_reviews_from_s3(bucket, key):
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    data = json.loads(content)
    return [r.get('review_detail', '') + ' ' + r.get('review_summary', '') for r in data]

# Step 2: Stop words set
STOP_WORDS = {
    'the', 'and', 'to', 'of', 'a', 'in', 'it', 'is', 'i', 'that',
    'this', 'was', 'for', 'on', 'with', 'as', 'but', 'at', 'by',
    'an', 'be', 'from', 'are', 'have', 'has', 'not', 'they', 'you',
    'there', 'he', 'his', 'she', 'her', 'them', 'or', 'so', 'if',
    'my', 'we', 'our', 'their', 'what', 'who', 'will', 'just'
}

# Step 3: Map function with stop word filtering
def map_words(text):
    words = [word.lower() for word in text.split() if word.isalpha() and word.lower() not in STOP_WORDS]
    return Counter(words)

# Step 4: Reduce function
def reduce_counts(mapped_results):
    total = Counter()
    for partial in mapped_results:
        total.update(partial)
    return total

# Step 5: Run MapReduce
if __name__ == "__main__":
    BUCKET = 'imdbreviews-scalable'
    KEY = 'input_files/*.json' 
    start = time.time()

    texts = load_reviews_from_s3(BUCKET, KEY)

    with Pool() as pool:
        mapped = pool.map(map_words, texts)
    
    final_counts = reduce_counts(mapped)

    # Print top 10 words
    print("\nTop 10 words (excluding stop words):")
    for word, count in final_counts.most_common(10):
        print(f"{word}: {count}")

    print(f" Total Time: {round(time.time() - start, 2)} seconds")
