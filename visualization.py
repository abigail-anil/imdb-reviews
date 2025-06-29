import os
import json
import glob
import boto3
import matplotlib.pyplot as plt

# Configurations
SUMMARY_DIR = './summaries'
GRAPH_OUTPUT_PREFIX = 'graph_outputs'
S3_BUCKET = 'imdbreviews-scalable'
S3_SUMMARY_PREFIX = 'summaries/'  

# Ensure local directory exists
os.makedirs(SUMMARY_DIR, exist_ok=True)

# Download JSON summary files from S3
def download_summaries_from_s3():
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    print(f"Downloading summary files from s3://{S3_BUCKET}/{S3_SUMMARY_PREFIX}")
    
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_SUMMARY_PREFIX):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.json'):
                filename = os.path.basename(key)
                local_path = os.path.join(SUMMARY_DIR, filename)
                s3.download_file(S3_BUCKET, key, local_path)
                print(f"Downloaded: {filename}")

# Load and extract relevant metrics
def parse_summary_files():
    methods, throughputs, latencies, total_times = [], [], [], []

    for filepath in sorted(glob.glob(os.path.join(SUMMARY_DIR, '*.json'))):
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)

            method = data.get('method') or os.path.basename(filepath).replace('.json', '')
            methods.append(method)

            # Get throughput
            throughput = next((v for k, v in data.items() if 'throughput' in k.lower()), 0)
            throughputs.append(throughput)

            # Get latency
            latency = next((v for k, v in data.items() if 'latency' in k.lower()), 0)
            latencies.append(latency)

            # Get total time
            total_time = next((v for k, v in data.items() if 'time' in k.lower() or 'duration' in k.lower()), 0)
            total_times.append(total_time)

            print(f"Parsed: {os.path.basename(filepath)}")
        except Exception as e:
            print(f"Error processing {filepath}: {e}")
    
    return methods, throughputs, latencies, total_times

# Generate and upload graph
def plot_and_upload(methods, values, ylabel, filename):
    plt.figure(figsize=(8, 5))
    plt.barh(methods, values)
    plt.title(f'{ylabel} Comparison')
    plt.xlabel(ylabel)
    plt.tight_layout()
    plt.savefig(filename)
    print(f"Saved plot: {filename}")

    # Upload to S3
    s3 = boto3.client('s3')
    s3.upload_file(filename, S3_BUCKET, f"{GRAPH_OUTPUT_PREFIX}/{filename}")
    print(f"Uploaded to S3: s3://{S3_BUCKET}/{GRAPH_OUTPUT_PREFIX}/{filename}")

# MAIN
if __name__ == '__main__':
    print(f"Loading summary files from: {SUMMARY_DIR}")
    download_summaries_from_s3()
    
    methods, throughputs, latencies, total_times = parse_summary_files()

    if not methods:
        print("No valid summary data found. Exiting.")
        exit()

    plot_and_upload(methods, throughputs, 'Reviews per Second', 'throughput_comparison.png')
    plot_and_upload(methods, latencies, 'Seconds per Review', 'latency_comparison.png')
    plot_and_upload(methods, total_times, 'Total Time (Seconds)', 'total_time_comparison.png')

    print("Graph processing complete.")

