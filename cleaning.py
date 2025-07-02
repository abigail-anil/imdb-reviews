import boto3
import json
import re # Import regular expression module

# ---------- Configuration ----------
S3_BUCKET = 'imdbreviews-scalable'
INPUT_KEY = 'input_files/sample.json'
OUTPUT_KEY = 'preprocessed_files/cleaned_filtered_sample.json' # Changed output key to reflect filtering

# Keys to remove from the JSON
keys_to_remove = ['helpful', 'review_id', 'reviewer', 'rating', 'review_date', 'spoiler_tag']

# ---------- Text Cleaning Function ----------
def clean_review_detail(text: str) -> str:
    """
    Performs cleaning operations on the review_detail text.
    - Removes HTML tags.
    - Removes URLs.
    - Replaces common HTML entities.
    - Collapses multiple whitespaces and strips leading/trailing whitespace.
    - Converts to lowercase.
    """
    if not isinstance(text, str):
        return "" # Ensure we are working with a string

    # 1. Remove HTML tags (e.g., <br/>, <p>)
    text = re.sub(r'<[^>]+>', '', text)

    # 2. Remove URLs
    text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)

    # 3. Replace common HTML entities (e.g., &amp; -> &, &quot; -> ", &#x27; -> ', &gt; -> >)
    text = text.replace('&amp;', '&').replace('&quot;', '"').replace('&#x27;', "'").replace('&gt;', '>')
    text = text.replace('&lt;', '<') # Add more if needed

    # 4. Remove excessive newlines and carriage returns, replace with single space
    text = re.sub(r'[\r\n]+', ' ', text)

    # 5. Collapse multiple spaces into a single space and strip leading/trailing whitespace
    text = re.sub(r'\s+', ' ', text).strip()

    # 6. Optional: Convert to lowercase (useful for consistency in text analysis)
    text = text.lower()

    return text

# ---------- Load from S3 ----------
s3 = boto3.client('s3')

print(f"Downloading {INPUT_KEY} from S3...")
response = s3.get_object(Bucket=S3_BUCKET, Key=INPUT_KEY)
raw_data = json.loads(response['Body'].read().decode('utf-8'))

filtered_data = []

if isinstance(raw_data, list):
    print("Processing list of JSON objects...")
    for item in raw_data:
        # 1. Remove unwanted keys first
        cleaned_item = {k: v for k, v in item.items() if k not in keys_to_remove}

        # 2. Clean 'review_detail' and count lines
        review_detail = cleaned_item.get('review_detail', '')
        
        # Apply cleaning
        cleaned_review_detail = clean_review_detail(review_detail)
        cleaned_item['review_detail'] = cleaned_review_detail # Update the item with cleaned detail

        # Count lines based on newline characters in the *original* review_detail
        # or, if you want lines based on the cleaned text's logical structure,
        # you might consider counting sentences or just checking for the original newlines.
        # For simplicity and direct line count as per request, we use `\n` in the *original* text.
        # However, for true "lines" after cleaning, you'd want to use a more sophisticated method
        # or define what "line" means for cleaned text (e.g., sentence count).
        # Given "reviews<=3 lines", it implies structural lines from the raw input.
        # Let's refine the line count to be after basic newline normalization but before single-space collapsing.

        # To accurately count lines <= 3 AFTER basic cleanup, we need a step in between:
        # 1. Normalize newlines to a single character (e.g., '\n')
        # 2. Count these normalized newlines
        temp_review_detail = re.sub(r'[\r\n]+', '\n', review_detail).strip()
        line_count = temp_review_detail.count('\n') + (1 if temp_review_detail else 0) # +1 for the last line if not empty

        if line_count <= 3:
            filtered_data.append(cleaned_item)

elif isinstance(raw_data, dict):
    print("Processing single JSON object (will not be filtered by line count).")
    # For a single dict, line filtering is not applicable as it's a single record.
    # We still clean the 'review_detail' if it exists.
    for key in keys_to_remove:
        raw_data.pop(key, None)
    
    review_detail = raw_data.get('review_detail', '')
    raw_data['review_detail'] = clean_review_detail(review_detail)
    filtered_data = raw_data # Assign the single processed dict

else:
    print("Unsupported JSON format. Data must be a list of objects or a single object.")

# ---------- Save cleaned and filtered JSON to S3 ----------
if filtered_data:
    print(f"Uploading cleaned and filtered JSON to {OUTPUT_KEY}...")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=OUTPUT_KEY,
        Body=json.dumps(filtered_data, indent=2),
        ContentType='application/json'
    )
    print(f"✅ Cleaned and filtered JSON file uploaded successfully. Total records: {len(filtered_data)}")
else:
    print("No data to upload after filtering.")
