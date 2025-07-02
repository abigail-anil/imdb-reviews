import os
import json
import glob
import boto3
import matplotlib.pyplot as plt
import numpy as np
from collections import Counter
from wordcloud import WordCloud
from matplotlib.gridspec import GridSpec

# Ensure Matplotlib uses a non-interactive backend for server environments
import matplotlib
matplotlib.use('Agg')


# --- Config ---
SUMMARY_DIR = './graph_summaries'
GRAPH_OUTPUT_PREFIX = 'graph_outputs'
S3_BUCKET = 'imdbreviews-scalable'
S3_SUMMARY_PREFIX = 'summaries/'
COMBINED_GRAPH_FILENAME = 'all_performance_metrics.pdf' # Changed to PDF for optimum display

os.makedirs(SUMMARY_DIR, exist_ok=True)

# --- S3 download ---
def download_summaries_from_s3():
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    print(f"Downloading summary files from s3://{S3_BUCKET}/{S3_SUMMARY_PREFIX}")
    downloaded_files_count = 0
    # Clear local summary directory before downloading to ensure fresh data
    for f in glob.glob(os.path.join(SUMMARY_DIR, '*')):
        os.remove(f)
    print(f"Cleared local summary directory: {SUMMARY_DIR}")

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_SUMMARY_PREFIX):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.json'):
                filename = os.path.basename(key)
                local_path = os.path.join(SUMMARY_DIR, filename)
                try:
                    s3.download_file(S3_BUCKET, key, local_path)
                    print(f"Downloaded: {filename}")
                    downloaded_files_count += 1
                except Exception as e:
                    print(f"Error downloading {filename}: {e}")
    if downloaded_files_count == 0:
        print(f"No summary files found or downloaded from s3://{S3_BUCKET}/{S3_SUMMARY_PREFIX}. Ensure files exist and permissions are correct.")


# --- Upload to S3 ---
def upload_to_s3(file_path, s3_prefix):
    s3 = boto3.client('s3')
    s3_key = f"{s3_prefix}/{os.path.basename(file_path)}"
    try:
        s3.upload_file(file_path, S3_BUCKET, s3_key)
        print(f"Uploaded to S3: s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"Error uploading {file_path} to S3: {e}")

# --- Helper function for pattern matching ---
def find_value_by_pattern(summary_dict, patterns, default_value=0):
    """
    Searches a dictionary for a key containing any of the given patterns (case-insensitive)
    and returns its value. Returns default_value if no match is found.
    """
    for key, value in summary_dict.items():
        if any(p.lower() in key.lower() for p in patterns):
            try:
                # Attempt to convert to float. If it's a string that's not a number, it will fail.
                return float(value)
            except (ValueError, TypeError):
                return default_value
    return default_value

# --- Parse summaries ---
def parse_summary_files():
    grouped_data = {}
    sentiment_data = Counter()
    word_freq = Counter()
    movie_sentiments = {}

    summary_files_found = False
    for filepath in sorted(glob.glob(os.path.join(SUMMARY_DIR, '*.json'))):
        summary_files_found = True
        try:
            with open(filepath, 'r') as f:
                summary = json.load(f)

            method = summary.get('method', os.path.basename(filepath).replace('.json', '')).lower()

            # Determine category (priority: Streaming, then Parallel/MapReduce, then Sequential)
            if 'streaming' in method:
                category = 'Streaming'
            elif 'parallel' in method or 'mapreduce' in method or 'mapreduce' in filepath.lower():
                category = 'Parallel'
            elif 'sequential' in method:
                category = 'Sequential'
            else:
                category = 'Other' # Fallback for unknown methods/filenames

            # Determine task (priority: specific keywords in method/filename)
            task = 'Sentiment' # Default if no specific task keyword found
            if 'wordcount' in method or 'wordcount' in filepath.lower():
                task = 'WordCount'
            elif 'hashtag' in method or 'hashtag' in filepath.lower():
                task = 'Hashtag'
            elif 'sentiment' in method or 'sentiment' in filepath.lower():
                task = 'Sentiment' # Explicitly pick up sentiment if present


            key = (category, task)

            # Initialize the entry for this key if it doesn't exist
            if key not in grouped_data:
                grouped_data[key] = {'throughput': 0, 'latency': 0, 'time': 0}

            # --- Pattern-based Extraction for Throughput, Latency, Time ---
            throughput_val = find_value_by_pattern(summary, ['throughput', 'reviews_per_sec', 'avg_throughput', 'processing_rate', 'speed'])
            latency_val = find_value_by_pattern(summary, ['latency', 'delay', 'response_time', 'avg_latency_sec', 'processing_latency'])
            time_val = find_value_by_pattern(summary, ['time', 'duration', 'run_time', 'total_run_time_sec', 'total_time'])

            grouped_data[key]['throughput'] += throughput_val
            grouped_data[key]['latency'] += latency_val
            grouped_data[key]['time'] += time_val


            # Aggregate sentiment data
            if 'sentiment_counts' in summary and isinstance(summary['sentiment_counts'], dict):
                for label, count in summary['sentiment_counts'].items():
                    if label.lower() != 'total_categorized':
                        sentiment_data[label] += count
            
            sentiment_data['Positive'] += summary.get('positive_count_total', 0)
            sentiment_data['Neutral'] += summary.get('neutral_count_total', 0)
            sentiment_data['Negative'] += summary.get('negative_count_total', 0)


            # Aggregate word frequencies
            for word, count in summary.get('top_10_words', []):
                word_freq[word] += count

            # Aggregate movie sentiments
            for movie, score in summary.get('top_10_movies_sentiment', []):
                movie_sentiments[movie] = score

        except json.JSONDecodeError as e:
            print(f"Error decoding JSON from {filepath}: {e}")
        except Exception as e:
            print(f"Failed to process {filepath}: {e}")
            
    if not summary_files_found:
        print(f"No JSON summary files found in {SUMMARY_DIR}. Please ensure files are present.")

    print(f"\n--- Parsed Data Summary ---")
    print(f"Grouped Data (summary_data): {grouped_data}")
    print(f"Sentiment Data (sentiment_data): {sentiment_data}")
    print(f"Word Frequencies (word_freq): {word_freq}")
    print(f"Movie Sentiments (movie_sentiments): {movie_sentiments}")
    print(f"---------------------------\n")

    return grouped_data, sentiment_data, word_freq, movie_sentiments

# --- Grouped Bar Chart (modified to accept ax) ---
def plot_grouped_bar(ax, data, metric, ylabel, title):
    categories = ['Sequential', 'Parallel', 'Streaming']
    tasks = ['WordCount', 'Sentiment', 'Hashtag']
    width = 0.2
    x = np.arange(len(categories))

    present_tasks = []
    for task in tasks:
        if any(data.get((cat, task), {}).get(metric, 0) > 0 for cat in categories):
            present_tasks.append(task)

    if not present_tasks:
        ax.set_title(f"{title}\n(No data available)", fontsize=14, color='red')
        ax.set_xticks(x)
        ax.set_xticklabels(categories)
        ax.set_visible(False) # Hide axes if no data
        return

    num_tasks_to_plot = len(present_tasks)
    offset = width * (num_tasks_to_plot - 1) / 2

    for i, task in enumerate(present_tasks):
        values = [data.get((cat, task), {}).get(metric, 0) for cat in categories]
        ax.bar(x + i * width - offset, values, width, label=task)

    ax.set_xlabel('Processing Method', fontsize=10)
    ax.set_ylabel(ylabel, fontsize=10)
    ax.set_title(title, fontsize=12)
    ax.set_xticks(x)
    ax.set_xticklabels(categories, rotation=0, ha='center', fontsize=9)
    ax.legend(title="Task", fontsize=9, title_fontsize='10')
    ax.grid(axis='y', linestyle='--', alpha=0.7)
    ax.tick_params(axis='x', labelsize=8)
    ax.tick_params(axis='y', labelsize=8)

# --- Other Charts (modified to accept ax) ---
def plot_pie(ax, data: Counter, title):
    if not data:
        ax.set_title(f"{title}\n(No data available)", fontsize=14, color='red')
        ax.set_visible(False)
        return
    
    if 'Total_Categorized' in data:
        del data['Total_Categorized']

    filtered_data = {label: count for label, count in data.items() if count > 0}
    if not filtered_data:
        ax.set_title(f"{title}\n(No non-zero data available)", fontsize=14, color='red')
        ax.set_visible(False)
        return

    labels, sizes = zip(*filtered_data.items())
    ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140, textprops={'fontsize': 9})
    ax.set_title(title, fontsize=12)
    ax.axis('equal') # Equal aspect ratio ensures that pie is drawn as a circle.

def plot_wordcloud(ax, freq, title):
    if not freq:
        ax.set_title(f"{title}\n(No data available)", fontsize=14, color='red')
        ax.set_visible(False)
        return
    cleaned_freq = {word: int(count) for word, count in freq.items() if count > 0}
    if not cleaned_freq:
        ax.set_title(f"{title}\n(No non-zero data available)", fontsize=14, color='red')
        ax.set_visible(False)
        return

    wordcloud = WordCloud(width=800, height=400, background_color='white', collocations=False).generate_from_frequencies(cleaned_freq)
    ax.imshow(wordcloud, interpolation='bilinear')
    ax.axis('off') # Hide axes for wordcloud
    ax.set_title(title, fontsize=12)

def plot_movie_sentiments(ax, movie_sent_dict, title):
    if not movie_sent_dict:
        ax.set_title(f"{title}\n(No data available)", fontsize=14, color='red')
        ax.set_visible(False)
        return
    
    # Sort and take top 10 (or fewer if less than 10)
    sorted_movies = sorted(movie_sent_dict.items(), key=lambda x: x[1], reverse=True)[:10]
    
    if not sorted_movies:
        ax.set_title(f"{title}\n(No valid data after sorting/filtering)", fontsize=14, color='red')
        ax.set_visible(False)
        return

    labels, values = zip(*sorted_movies)
    ax.bar(labels, values, color='skyblue')
    ax.set_xlabel("Movies", fontsize=10)
    ax.set_ylabel("Average Sentiment", fontsize=10)
    ax.set_title(title, fontsize=12)
    ax.set_xticks(range(len(labels))) # Ensure all labels are set
    ax.set_xticklabels(labels, rotation=45, ha='right', fontsize=9)
    ax.tick_params(axis='y', labelsize=8)
    ax.grid(axis='y', linestyle='--', alpha=0.7)


# --- MAIN ---
if __name__ == "__main__":
    download_summaries_from_s3()
    summary_data, sentiment_data, word_freq, movie_sentiments = parse_summary_files()

    if not summary_data and not sentiment_data and not word_freq and not movie_sentiments:
        print("No data was parsed from summary files. Exiting without generating plots.")
    else:
        # Create a single figure to hold all plots
        # Determine number of rows needed based on how many plots you have
        # 3 bar charts + 1 pie + 1 wordcloud + 1 movie sentiment = 6 plots
        # We'll arrange them in 3 rows with 2 columns.
        fig = plt.figure(figsize=(15, 20)) # Width, Height in inches. Adjust as needed.
                                           # 15 inches wide, 20 inches tall is a good starting point for 6 plots.
                                           # You might need to go taller if labels overlap.

        gs = GridSpec(3, 2, figure=fig, hspace=0.6, wspace=0.3) # 3 rows, 2 columns. hspace and wspace for spacing

        # Throughput Plot (Row 0, Col 0)
        ax0 = fig.add_subplot(gs[0, 0])
        plot_grouped_bar(ax0, summary_data, 'throughput', 'Reviews/Words/Hashtags per Second (Throughput)', 'Throughput by Method and Task')

        # Latency Plot (Row 0, Col 1)
        ax1 = fig.add_subplot(gs[0, 1])
        plot_grouped_bar(ax1, summary_data, 'latency', 'Seconds per Item (Latency)', 'Latency by Method and Task')

        # Total Time Plot (Row 1, Col 0)
        ax2 = fig.add_subplot(gs[1, 0])
        plot_grouped_bar(ax2, summary_data, 'time', 'Total Time (Seconds)', 'Total Time by Method and Task')

        # Sentiment Distribution Pie Chart (Row 1, Col 1)
        ax3 = fig.add_subplot(gs[1, 1])
        plot_pie(ax3, sentiment_data, 'Overall Sentiment Distribution')

        # Top Words WordCloud (Row 2, Col 0) - Wordcloud needs a dedicated, potentially wider space
        # We can make it span both columns in the last row for better visibility if needed
        ax4 = fig.add_subplot(gs[2, 0]) # Was gs[2,0] but now span both, adjust if other plot is there
        plot_wordcloud(ax4, word_freq, 'Top Words WordCloud')

        # Top Movies Sentiment Bar Chart (Row 2, Col 1) - Or place next to word cloud
        ax5 = fig.add_subplot(gs[2, 1]) # Place it next to word cloud in last row
        plot_movie_sentiments(ax5, movie_sentiments, 'Top 10 Movies by Average Sentiment')

        # Adjust layout to prevent overlaps
        plt.tight_layout(rect=[0, 0, 1, 0.98]) # Adjust rect to make space for a potential suptitle
        fig.suptitle('Comprehensive Performance Analysis', fontsize=18, y=1.0) # Overall title

        # Save the combined figure
        combined_file_path = os.path.join('.', COMBINED_GRAPH_FILENAME) # Saves in current directory
        plt.savefig(combined_file_path, format='pdf', bbox_inches='tight', dpi=300) # dpi for raster content in PDF if any
        print(f"Saved combined plots to: {combined_file_path}")

        # Upload the combined file to S3
        upload_to_s3(combined_file_path, GRAPH_OUTPUT_PREFIX)
        
        print("All plots generated and uploaded.")
