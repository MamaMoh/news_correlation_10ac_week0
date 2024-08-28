import pandas as pd
import nltk
from nltk.tokenize import word_tokenize
from nltk import pos_tag, ne_chunk
from collections import Counter
from keybert import KeyBERT
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import matplotlib.pyplot as plt
from nltk.corpus import stopwords
import mlflow
import mlflow.sklearn
from bertopic import BERTopic

nltk.download('stopwords')
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

def keybert_keyword_extraction(news_data):
    kw_model = KeyBERT()

    title_keywords_list = []
    content_keywords_list = []

    for index, row in news_data.iterrows():
        title_text = row['title']
        content_text = row['content']

        # Extract keywords
        title_keywords = kw_model.extract_keywords(title_text, top_n=5)
        content_keywords = kw_model.extract_keywords(content_text, top_n=5)

        title_keywords_list.append(title_keywords)
        content_keywords_list.append(content_keywords)

    return title_keywords_list, content_keywords_list

def calculate_similarity(title_keywords_list, content_keywords_list):
    similarity_list = []

    for title_keywords, content_keywords in zip(title_keywords_list, content_keywords_list):
        # Convert the lists of tuples to dictionaries for easy access
        title_dict = dict(title_keywords)
        content_dict = dict(content_keywords)

        # Get the set of all unique keywords
        all_keywords = list(set(title_dict.keys()).union(set(content_dict.keys())))
        # Create vectors based on the relevance scores for the title and content
        title_vector = np.array([title_dict.get(keyword, 0) for keyword in all_keywords])
        content_vector = np.array([content_dict.get(keyword, 0) for keyword in all_keywords])
       
        # Calculate cosine similarity
        if np.linalg.norm(title_vector) == 0 or np.linalg.norm(content_vector) == 0:
            similarity = 0
        else:
            similarity = cosine_similarity([title_vector], [content_vector])[0][0]

        similarity_list.append(similarity)

    return similarity_list


def remove_stopwords(text):
    stop_words = set(stopwords.words('english'))
    words = text.split()
    filtered_words = [word for word in words if word.lower() not in stop_words]
    return ' '.join(filtered_words)

def perform_topic_modeling_with_mlflow(dataframe):
    # Start MLflow run
    with mlflow.start_run():
        # Sample data and combine title and content
        sampled_data = dataframe.sample(1000)
        sampled_data['text'] = sampled_data['title'] + ' ' + sampled_data['content']
        sampled_list = sampled_data['text'].tolist()
        
        # Remove stopwords from the text
        sampled_list = [remove_stopwords(text) for text in sampled_list]
        
        # Fit the BERTopic model
        topic_model = BERTopic()
        topics, probs = topic_model.fit_transform(sampled_list)
        print('Model fitting Done!')
        
        # Log model and parameters to MLflow
        mlflow.log_param("model_type", "BERTopic")
        mlflow.log_param("n_topics", len(set(topics)))
        mlflow.log_param("n_samples", len(sampled_list))
        
        # Log the model
        mlflow.sklearn.log_model(topic_model, "bertopic_model")
        
        # Log the topics as an artifact (CSV)
        topic_info = topic_model.get_topic_info()
        topic_info.to_csv("topic_info.csv", index=False)
        mlflow.log_artifact("topic_info.csv")
        
        print("Logged to MLflow successfully!")
        
        return topic_info, topic_model