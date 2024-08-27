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


