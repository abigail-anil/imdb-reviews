import streamlit as st
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from utils import load_summary_dataframe
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Real-Time IMDB Dashboard", layout="wide")
st.title(" Real-Time IMDB Sentiment Dashboard")

REFRESH_INTERVAL = 10  # seconds
st_autorefresh(interval=REFRESH_INTERVAL * 1000, key="auto-refresh")

df = load_summary_dataframe()

if df.empty:
    st.warning("No data available yet.")
else:
    latest_time = df["timestamp_utc"].max()
    recent_df = df[df["timestamp_utc"] > latest_time - pd.Timedelta(minutes=5)]
    latest_row = df.iloc[-1]

    # --- Section: Sentiment Distribution (Pie) ---
    with st.expander(" Sentiment Distribution (Pie Chart)", expanded=True):
        sentiment_counts = {
            "Positive": latest_row["positive_count_total"],
            "Neutral": latest_row["neutral_count_total"],
            "Negative": latest_row["negative_count_total"]
        }
        pie_df = pd.DataFrame({
            "Sentiment": list(sentiment_counts.keys()),
            "Count": list(sentiment_counts.values())
        })
        fig_pie = px.pie(pie_df, names="Sentiment", values="Count", title="Sentiment Distribution")
        st.plotly_chart(fig_pie, use_container_width=True)

    # --- Section: Throughput & Latency Over Time ---
    with st.expander(" Throughput & Latency", expanded=True):
        col1, col2 = st.columns(2)

        with col1:
            fig_throughput = px.line(
                recent_df,
                x="timestamp_utc",
                y="throughput_rec_per_sec",
                title="Throughput (records/sec)"
            )
            st.plotly_chart(fig_throughput, use_container_width=True)

        with col2:
            fig_latency = px.line(
                recent_df,
                x="timestamp_utc",
                y="avg_latency_sec",
                title="Latency (seconds)"
            )
            st.plotly_chart(fig_latency, use_container_width=True)

    # --- Section: Sentiment Counts (Bar) ---
    with st.expander(" Sentiment Count Totals", expanded=True):
        st.bar_chart(sentiment_counts)

    # --- Section: Top Words Word Cloud ---
    with st.expander("☁️ Top 10 Words (Word Cloud)", expanded=True):
        try:
            top_words = latest_row["top_words_window"][:10]
            word_freq = {word: count for word, count in top_words}
            wordcloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(word_freq)
            fig_wc, ax = plt.subplots(figsize=(10, 5))
            ax.imshow(wordcloud, interpolation='bilinear')
            ax.axis("off")
            st.pyplot(fig_wc)
        except Exception as e:
            st.warning("Word cloud not available.")
            st.text(str(e))

    # --- Section: Movie Sentiment Bar ---
    with st.expander(" Top 10 Movies Sentiment", expanded=True):
        try:
            movie_sentiments = latest_row["sentiment_by_movie_current_window"]
            movie_df = pd.DataFrame(movie_sentiments.items(), columns=["Movie", "Avg Sentiment"])
            fig_movies = px.bar(movie_df.head(10), x="Movie", y="Avg Sentiment", title="Top 10 Movies Sentiment")
            st.plotly_chart(fig_movies, use_container_width=True)
        except Exception as e:
            st.warning("Movie sentiment data not available.")
            st.text(str(e))

