import os
import json
import glob
import boto3
import matplotlib.pyplot as plt
import numpy as np

from matplotlib.gridspec import GridSpec
from botocore.exceptions import ClientError

# --- Config ---
SUMMARY_DIR = './graph_summaries'
GRAPH_OUTPUT_PREFIX = 'graph_outputs'
S3_BUCKET = 'imdbreviews-scalable'
S3_SUMMARY_PREFIX = 'summaries/'
OUTPUT_PDF = 'batch_vs_sequential_metrics.pdf'

os.makedirs(SUMMARY_DIR, exist_ok=True)

# --- S3 Download ---
def download_summaries_from_s3():
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    print(" Downloading summary files from S3...")

    for f in glob.glob(os.path.join(SUMMARY_DIR, '*.json')):
        os.remove(f)

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_SUMMARY_PREFIX):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.json'):
                local_path = os.path.join(SUMMARY_DIR, os.path.basename(key))
                try:
                    s3.download_file(S3_BUCKET, key, local_path)
                    print(f"Downloaded: {os.path.basename(key)}")
                except ClientError as e:
                    print(f"Failed to download {key}: {e}")

# --- Metric Extraction ---
def find_value(summary, patterns):
    for key, value in summary.items():
        if any(p.lower() in key.lower() for p in patterns):
            try:
                return float(value)
            except:
                return 0
    return 0

# --- Parse JSON Summaries ---
def parse_summaries():
    data = {}
    for file in glob.glob(os.path.join(SUMMARY_DIR, '*.json')):
        with open(file, 'r') as f:
            summary = json.load(f)

        method = summary.get('method', '').lower()
        if 'streaming' in method:
            continue
        elif 'sequential' in method:
            category = 'Sequential'
        elif 'mapreduce' in method or 'parallel' in method:
            category = 'Batch'
        else:
            continue

        if 'word count' in method:
            task = 'WordCount'
        elif 'sentiment' in method:
            task = 'Sentiment'
        elif 'hashtag' in method:
            task = 'Hashtag'
        else:
            continue

        key = (category, task)
        if key not in data:
            data[key] = {'throughput': 0, 'latency': 0, 'time': 0}

        data[key]['throughput'] += find_value(summary, ['throughput'])
        data[key]['latency'] += find_value(summary, ['latency'])
        data[key]['time'] += find_value(summary, ['time'])

    return data

# --- Plot Comparison Charts ---
def plot_comparisons(data):
    metrics = ['throughput', 'latency', 'time']
    labels = ['Throughput (items/sec)', 'Latency (sec/item)', 'Total Time (sec)']
    tasks = ['WordCount', 'Sentiment', 'Hashtag']
    methods = ['Sequential', 'Batch']

    fig, axes = plt.subplots(3, 1, figsize=(10, 12))
    for i, metric in enumerate(metrics):
        ax = axes[i]
        bar_width = 0.35
        x = np.arange(len(tasks))

        seq_vals = [data.get(('Sequential', t), {}).get(metric, 0) for t in tasks]
        batch_vals = [data.get(('Batch', t), {}).get(metric, 0) for t in tasks]

        ax.bar(x - bar_width/2, seq_vals, bar_width, label='Sequential', color='#FF9999')
        ax.bar(x + bar_width/2, batch_vals, bar_width, label='Batch (MapReduce)', color='#66B3FF')

        ax.set_xticks(x)
        ax.set_xticklabels(tasks)
        ax.set_ylabel(labels[i])
        ax.set_title(f'{labels[i]} Comparison')
        ax.legend()
        ax.grid(axis='y', linestyle='--', alpha=0.7)

    plt.tight_layout()
    plt.savefig(OUTPUT_PDF, format='pdf', bbox_inches='tight')
    print(f"\n Saved comparison charts to: {OUTPUT_PDF}")
    return OUTPUT_PDF

# --- Upload PDF to S3 ---
def upload_to_s3(file_path):
    s3 = boto3.client('s3')
    s3_key = f"{GRAPH_OUTPUT_PREFIX}/{os.path.basename(file_path)}"
    try:
        s3.upload_file(file_path, S3_BUCKET, s3_key)
        print(f"☁️ Uploaded to S3: s3://{S3_BUCKET}/{s3_key}")
    except ClientError as e:
        print(f" Upload failed: {e}")

# --- Run Everything ---
if __name__ == "__main__":
    download_summaries_from_s3()
    parsed_data = parse_summaries()
    if parsed_data:
        pdf_path = plot_comparisons(parsed_data)
        upload_to_s3(pdf_path)
    else:
        print(" No batch or sequential data found to plot.")

