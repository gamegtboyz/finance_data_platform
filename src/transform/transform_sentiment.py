import json
import pandas as pd
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

_analyzer = None

def _get_analyzer():
    """
    The wrapper function of nltk (Natural Language ToolKit and Vader sentiment analysis)
    """
    global _analyzer
    if _analyzer is None:
        try:
            _analyzer = SentimentIntensityAnalyzer()
        except LookupError:
            nltk.download('vader_lexicon', quiet=True)
            _analyzer = SentimentIntensityAnalyzer()
    return _analyzer

def transform_news_sentiment(filepath:str, symbol: str) -> pd.DataFrame:
    with open(filepath, 'r') as f:
        data = json.load(f)

    articles = data.get("articles", [])
    records = []
    for art in articles:
        if not art.get("title") or art["title"] == "[Removed]":
            continue
        if not art.get("publishedAt"):
            continue
        records.append({
            "symbol": symbol,
            "date": pd.to_datetime(art["publishedAt"]).normalize(),
            "headline": art.get("title", None),
            "sentiment_score": _get_analyzer().polarity_scores(art["title"])["compound"],
            "source": art.get("source", {}).get("name"),
            "url": art.get("url", None)
        })
    
    df = pd.DataFrame(records)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])

    return df