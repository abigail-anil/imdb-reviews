import json
import boto3
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
from multiprocessing import Pool, cpu_count
import time

# Download once
nltk.download('vader_lexicon')
sia = SentimentIntensityAnalyzer()

# Config
S3_BUCKET = 'imdbreviews-scalable'
S3_KEY = 'input_files/*.json'

# Load from S3
s3 = boto3.client('s3')
obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
data = json.loads(obj['Body'].read().decode('utf-8'))

# Extract text data for sentiment analysis
texts = [
    (review.get('review_detail', '') + ' ' + review.get('review_summary', '')).strip()
    for review in data if isinstance(review, dict)
]

# Remove empty or invalid entries
texts = [t for t in texts if isinstance(t, str) and t]

# Map function for sentiment
def map_sentiment(text):
    score = sia.polarity_scores(text)['compound']
    return score

if __name__ == '__main__':
    start = time.time()
    with Pool(cpu_count()) as pool:
        sentiments = pool.map(map_sentiment, texts)

    avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else 0
    print(f"\n Average Sentiment Polarity (VADER): {round(avg_sentiment, 4)}")
    print(f"\n Total Time: {round(time.time() - start, 2)} seconds")

