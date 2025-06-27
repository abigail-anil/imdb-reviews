import boto3
import json
import re
from multiprocessing import Pool
from collections import Counter
import time

# Config
S3_BUCKET = 'imdbreviews-scalable'
S3_KEY = 'input_files/*.json'

# Define stop hashtags
stop_tags = {
    'the', 'and', 'you', 'but', 'was', 'are', 'for', 'have', 'not',
    'his', 'her', 'she', 'there', 'with', 'this', 'that', 'what',
    'in', 'on', 'out', 'at', 'by', 'all', 'spoiler', 'movie', 'film',
    'one', 'more', 'some', 'just', 'from', 'like', 'contains', 'commentessentially'
}

# Load file from S3
def load_reviews():
    print(" Loading reviews from S3...")
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
    content = response['Body'].read().decode('utf-8')
    data = json.loads(content)
    return data

# Extract hashtags from review
def extract_hashtags(review):
    text = review.get("review_detail", "") + " " + review.get("review_summary", "")
    #raw_tags = re.findall(r'(?<!\w)#([a-zA-Z]{4,})\b', text)
    raw_tags = re.findall(r'(?<!\w)#([a-zA-Z]{3,})\b', text)
    return [
        f"#{tag.lower()}" for tag in raw_tags
        if tag.lower() not in stop_tags
    ]

if __name__ == "__main__":
    start = time.time()
    reviews = load_reviews()
    print(f" Processing {len(reviews)} reviews for hashtags...")

    # MapReduce
    with Pool() as pool:
        results = pool.map(extract_hashtags, reviews)

    # Flatten and count
    all_tags = [tag for sublist in results for tag in sublist]
    hashtag_counter = Counter(all_tags)

    # Top 10 hashtags with at least 2 occurrences
    top_hashtags = [(tag, count) for tag, count in hashtag_counter.most_common(20) if count >= 2]

    print("\n Top Hashtags:")
    for tag, count in top_hashtags:
        print(f"{tag}: {count}")
    
    print(f"\n Total Time: {round(time.time() - start, 2)} seconds")
