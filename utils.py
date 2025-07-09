import boto3
import json
import pandas as pd

S3_BUCKET = "imdbreviews-scalable"
S3_PREFIX = "kafka-consumer-outputs/"

def list_summary_keys():
    s3 = boto3.client('s3')
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.json'):
                keys.append(obj['Key'])
    return sorted(keys)

def read_summary_from_s3(key):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return json.loads(obj['Body'].read())

def load_summary_dataframe():
    keys = list_summary_keys()
    summaries = []
    for k in keys:
        try:
            data = read_summary_from_s3(k)
            if "timestamp_utc" in data:  # only include if key exists
                summaries.append(data)
        except Exception as e:
            print(f"Skipping {k}: {e}")
            continue
    if not summaries:
        return pd.DataFrame()
    
    df = pd.DataFrame(summaries)
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"])
    df.sort_values("timestamp_utc", inplace=True)
    return df

