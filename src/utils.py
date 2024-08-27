import pandas as pd
import nltk
from nltk.tokenize import word_tokenize
from nltk import pos_tag, ne_chunk
from collections import Counter

nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('maxent_ne_chunker')
nltk.download('words')

def extract_countries_from_article_content(text):
    """
    Extracts countries (Geopolitical Entities) from a given article text.
    """
    words = word_tokenize(text)
    tagged_words = pos_tag(words)
    named_entities = ne_chunk(tagged_words)
    countries = [chunk[0] for chunk in named_entities if hasattr(chunk, 'label') and chunk.label() == 'GPE']
    return countries

def find_popular_articles(df, max_rows=100):
    """
    Processes articles to find the most common countries mentioned.
    """
    # List to store all mentioned countries
    all_countries = []
    
    # Iterate over rows of the DataFrame
    for index, row in df.iterrows():
        # Extract countries from the article content
        countries = extract_countries_from_article_content(row['content'])
        all_countries.extend(countries)
        
        # Stop processing if max_rows is reached
        if index + 1 >= max_rows:
            break
    
    # Count occurrences of each country
    country_counts = Counter(all_countries)
    
    # Get the most common countries
    return country_counts




def website_sentiment_distribution(data):
    # Generate sentiment counts for each domain
    sentiment_counts = data.groupby(['source_name', 'title_sentiment']).size().unstack(fill_value=0)
    
    # Ensure that sentiment types are present, and handle dynamic cases if needed
    sentiment_types = ['Positive', 'Neutral', 'Negative']
    for sentiment in sentiment_types:
        if sentiment not in sentiment_counts.columns:
            sentiment_counts[sentiment] = 0
    
    # Calculate total, mean, and median sentiment counts for each domain
    sentiment_counts['Total'] = sentiment_counts.sum(axis=1)
    sentiment_counts['Mean'] = sentiment_counts[sentiment_types].mean(axis=1)
    sentiment_counts['Median'] = sentiment_counts[sentiment_types].median(axis=1)

    return sentiment_counts
