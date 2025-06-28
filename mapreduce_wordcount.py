import boto3
import json
import time
from multiprocessing import Pool, cpu_count
from collections import Counter

S3_BUCKET = 'imdbreviews-scalable'
S3_PREFIX = 'input_files/'

STOP_WORDS = {
    'the', 'and', 'to', 'of', 'a', 'in', 'it', 'is', 'i', 'that',
    'this', 'was', 'for', 'on', 'with', 'as', 'but', 'at', 'by',
    'an', 'be', 'from', 'are', 'have', 'has', 'not', 'they', 'you',
    'there', 'he', 'his', 'she', 'her', 'them', 'or', 'so', 'if',
    'my', 'we', 'our', 'their', 'what', 'who', 'will', 'just'
}

# List all .json files in S3 prefix
def list_json_keys(bucket, prefix):
    s3 = boto3.client('s3')
    paginator = s3.get_pagnator('list_objects_v2')
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.json'):
                keys.append(obj['Key'])
    return keys

# Map function: read file, clean and count words, and count reviews
def map_wordcount_from_file(key):
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)

        counter = Counter()
        reviews_in_file = 0 # Initialize counter for reviews in this file
        for r in data:
            if isinstance(r, dict):
                reviews_in_file += 1 # Increment for each valid review
                text = r.get('review_detail', '') + ' ' + r.get('review_summary', '')
                words = [
                    word.lower()
                    for word in text.split()
                    if word.isalpha() and word.lower() not in STOP_WORDS
                ]
                counter.update(words)
        return counter, reviews_in_file # Return both the word count and review count
    except Exception as e:
        print(f"Error processing {key}: {e}")
        return Counter(), 0 # Return empty counter and 0 reviews on error

# Reduce function: merge all word counters and sum review counts
def reduce_counts(mapped_results):
    final_word_counts = Counter()
    total_reviews_processed = 0 # Initialize total reviews

    for partial_word_count, reviews_count_in_file in mapped_results:
        final_word_counts.update(partial_word_count)
        total_reviews_processed += reviews_count_in_file # Sum up reviews from each file
    return final_word_counts, total_reviews_processed

# Main
if __name__ == '__main__':
    start_time = time.time() # Renamed to avoid conflict if 'start' is a keyword elsewhere

    print("Listing files...")
    keys = list_json_keys(S3_BUCKET, S3_PREFIX)
    print(f"Found {len(keys)} JSON files.")

    print("Running parallel Map step...")
    with Pool(cpu_count()) as pool:
        # mapped_results will now be a list of tuples: [(Counter, int), (Counter, int), ...]
        mapped_results = pool.map(map_wordcount_from_file, keys)

    print("Reducing results...")
    # final_counts will be the Counter, total_reviews will be the integer
    final_word_counts, total_reviews_processed = reduce_counts(mapped_results)

    end_time = time.time()
    total_mapreduce_time = round(end_time - start_time, 2)

    # Calculate Throughput and Latency
    throughput = round(total_reviews_processed / total_mapreduce_time, 2) if total_mapreduce_time > 0 else 0
    latency = round(total_mapreduce_time / total_reviews_processed, 4) if total_reviews_processed > 0 else 0

    print("\n--- MapReduce Summary ---")
    print(f"Total files processed: {len(keys)}")
    print(f"Total reviews processed: {total_reviews_processed}")
    print(f"Total MapReduce Time: {total_mapreduce_time} seconds")
    print(f"Overall Throughput: {throughput} reviews/second")
    print(f"Overall Latency: {latency} seconds/review")

    print("\nTop 10 Words (excluding stop words):")
    for word, count in final_word_counts.most_common(10):
        print(f"{word}: {count}")
