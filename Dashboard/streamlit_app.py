import streamlit as st 
import plotly.express as px 
import pandas as pd
import os, sys
import warnings
warnings.filterwarnings('ignore')
if os.path.abspath("..") not in sys.path:
    sys.path.insert(0, os.path.abspath(".."))
from src.loader import NewsDataLoader
from src.db import *

st.set_page_config(page_title='News Analysis', page_icon=':loudspeaker:', layout='wide')

st.title(":loudspeaker: News EDA")
st.markdown('<style>div.block-container{padding-top:2rem;}</style>', unsafe_allow_html=True)

domains_df = read_domains()
locations_df = read_domain_locations()
traffic_df = read_traffic_data()
articles_df = read_articles()

col1, col2 = st.columns((2))

# Assuming articles_df is already loaded
articles_df["published_at"] = pd.to_datetime(articles_df["published_at"])

# Date pickers for start and end date
st.sidebar.header("Filter by Published Date")
start_date = st.sidebar.date_input("Start Date", articles_df["published_at"].min().date())
end_date = st.sidebar.date_input("End Date", articles_df["published_at"].max().date())

# Filter the DataFrame based on selected dates
filtered_df = articles_df[(articles_df["published_at"] >= pd.Timestamp(start_date)) & (articles_df["published_at"] <= pd.Timestamp(end_date))]

# Group the DataFrame by 'source_name' and count the number of 'id'
category_df = filtered_df.groupby(by=["source_name"], as_index=False)["id"].count()

# Rename the columns
category_df.rename(columns={"source_name": "Domain", "id": "Count"}, inplace=True)

# Sort the DataFrame to get the top 5
top_category_df = category_df.sort_values(by="Count", ascending=False).head(5)

# Sort the DataFrame to get the bottom 5
bottom_category_df = category_df.sort_values(by="Count", ascending=True).head(5)

# Plotting the top 5 and bottom 5 domains
with col1:
    st.subheader("Top 5 Domains by Number of Articles")
    fig_top = px.bar(top_category_df, x="Domain", y="Count", 
                     text=['{:,.2f}'.format(x) for x in top_category_df["Count"]],
                     template="seaborn")
    st.plotly_chart(fig_top, use_container_width=False, height=200)

with col2:
    st.subheader("Bottom 5 Domains by Number of Articles")
    fig_bottom = px.bar(bottom_category_df, x="Domain", y="Count", 
                        text=['{:,.2f}'.format(x) for x in bottom_category_df["Count"]],
                        template="seaborn")
    st.plotly_chart(fig_bottom, use_container_width=False, height=200)

import plotly.graph_objects as go

# Merge domains_df with locations_df to include the country information
merged_df = pd.merge(domains_df, locations_df, left_on='domain_locations_id', right_on='id')

# Grouping the merged DataFrame by country
country_df = merged_df.groupby("country", as_index=False)["domain_name"].count()
country_df.rename(columns={"domain_name": "Domain Count", "country": "Country"}, inplace=True)

# Sorting by domain count to get the top countries
country_df = country_df.sort_values(by="Domain Count", ascending=False)

# Select the top 5 countries
top_5_df = country_df.head(5)

# Group the rest as "Other"
other_df = pd.DataFrame({
    "Country": ["Other"],
    "Domain Count": [country_df["Domain Count"].iloc[5:].sum()]
})

# Combine the top 5 with the "Other" category
final_df = pd.concat([top_5_df, other_df], ignore_index=True)

# Create the pie chart with larger size
st.subheader("Distribution of Media/Domain by Country")

fig_pie = go.Figure(data=[go.Pie(labels=final_df["Country"], values=final_df["Domain Count"],
                                hole=0.3, textinfo='label+percent', 
                                marker=dict(colors=px.colors.sequential.Plasma))])

fig_pie.update_layout(
    title="Distribution of Domains by Country",
    template="seaborn",
    width=800,  # Set the width of the figure
    height=600  # Set the height of the figure
)

st.plotly_chart(fig_pie, use_container_width=False)


 # Convert 'published_at' to datetime if it's not already
articles_df["published_at"] = pd.to_datetime(articles_df["published_at"])

# Group by date and count the number of articles
time_series_df = articles_df.groupby(articles_df["published_at"].dt.date).size().reset_index(name="Article Count")

# Plotting the time series graph
st.subheader("Number of Articles Over Time")

fig_ts = px.line(time_series_df, x="published_at", y="Article Count", 
                 title="Number of Articles Over Time", 
                 template="seaborn", markers=True)
fig_ts.update_layout(
    xaxis_title="Date",
    yaxis_title="Number of Articles",
    xaxis=dict(
        tickformat="%Y-%m-%d",
        title_standoff=0
    ),
    yaxis=dict(
        title_standoff=0
    )
)

st.plotly_chart(fig_ts, use_container_width=True)