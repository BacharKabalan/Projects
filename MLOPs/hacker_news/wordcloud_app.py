import streamlit as st
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import psycopg2
import pandas as pd
from serpapi import search
import re

# Function to fetch text data from PostgreSQL database
def fetch_data_from_database():
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="mysecretpassword",
            host="localhost",
            port="5434"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT comments FROM questions;")
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        return data
    except psycopg2.Error as e:
        st.error(f"Database error: {e}")
        return []

# Function to generate word cloud from fetched text data
def generate_wordcloud(data):
    text = ' '.join([row[0] for row in data])
    wordcloud = WordCloud(width=800, height=400, background_color='white').generate(text)
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.show()
    plt.savefig('wordcloud.png')  # Save the plot to a file
    return plt.gcf()


# def google_job_search():
#     params = {
#     "engine": "google",
#     "q": "glassdoor mlops engineer jobs",
#     "location": "United Kingdom",
#     "hl": "en",
#     "gl": "uk",
#     "google_domain": "google.com",
#     "num": "20",
#     "start": "10",
#     "safe": "active",
#     "api_key": "c5c1ab2e9b960344525d642036948de7e7ed02e62c28ff06d3b4b0c3356ff9bd"
#     }

#     results = search(params)
#     titles = [result['title'] for result in results['organic_results']]
#     title_links = [result['link'] for result in results['organic_results']]


#     def find_numbers(strings):
#         numbers = []
#         for string in strings:
#             # Use regular expression to find all numbers in the string
#             found_numbers = re.findall(r'\d+', string)
#             # Convert the found numbers from strings to integers and add them to the list
#             numbers.extend(map(int, found_numbers))
#         return numbers

#     # Example list of strings

#     # Find all numbers in the list of strings
#     result = find_numbers(titles)
#     data = {params['q']: titles, 'title_links':title_links}
#     df = pd.DataFrame(data)
#     return df

# def google_job_search():
#     params = {
#     "engine": "google",
#     "q": "glassdoor mlops engineer jobs",
#     "location": "United Kingdom",
#     "hl": "en",
#     "gl": "uk",
#     "google_domain": "google.com",
#     "num": "20",
#     "start": "10",
#     "safe": "active",
#     "api_key": "c5c1ab2e9b960344525d642036948de7e7ed02e62c28ff06d3b4b0c3356ff9bd"
#     }
#     def find_numbers(strings):
#         numbers = []
#         for string in strings:
#             # Use regular expression to find all numbers in the string
#             found_numbers = re.findall(r'\d+', string)
#             # Convert the found numbers from strings to integers and add them to the list
#             if found_numbers:
#                 numbers.extend(map(int, found_numbers))
#             else:
#                 numbers.extend(map(int, '0'))
#         return numbers
#     results = search(params)
#     titles = [result['title'] for result in results['organic_results']]
#     title_links = [result['link'] for result in results['organic_results']]
#     job_numbers_per_result = find_numbers(titles)
#     data = {params['q']: titles, 'job_count': job_numbers_per_result, 'title_links':title_links}
#     df = pd.DataFrame(data)
#     return df

def google_job_search():
    params = {
    "engine": "google",
    "q": "glassdoor mlops engineer jobs",
    "location": "United Kingdom",
    "hl": "en",
    "gl": "uk",
    "google_domain": "google.com",
    "num": "20",
    "start": "10",
    "safe": "active",
    "api_key": "c5c1ab2e9b960344525d642036948de7e7ed02e62c28ff06d3b4b0c3356ff9bd"
    }
    
    search_results = search(params)
    google_query = params['q']
    return search_results, google_query

def find_numbers(strings):
    numbers = []
    for string in strings:
        # Use regular expression to find all numbers in the string
        found_numbers = re.findall(r'\d+\s', string)
        # Convert the found numbers from strings to integers and add them to the list
        
        if found_numbers:
            first_number = [found_numbers[0]]
            if first_number[0] != '2024':
                numbers.extend(map(int, first_number))
            else: 
                numbers.extend(map(int, '0'))
        else:
            numbers.extend(map(int, '0'))
    return numbers
def put_google_search_results_in_df(search_results, google_query):
    
    titles = [result['title'] for result in search_results['organic_results']]
    title_links = [result['link'] for result in search_results['organic_results']]
    job_numbers_per_result = find_numbers(titles)
    data = {google_query: titles,'job_count': job_numbers_per_result, 'title_links':title_links} # 
    df = pd.DataFrame(data)
    return df



    # Example list of strings

    # Find all numbers in the list of strings




def main():
    st.title("Word Cloud from PostgreSQL Database")
    
    # Fetch data from database
    data = fetch_data_from_database()
    data = data[:90]
    search, google_query = google_job_search()
    top_10_google_search_results_df = put_google_search_results_in_df(search, google_query)


    if not data:
        st.warning("No data fetched from the database.")
    else:
        # Generate and display word cloud
        word_cloud_plot = generate_wordcloud(data)
    
    st.pyplot(word_cloud_plot)
    st.table(top_10_google_search_results_df)
    
if __name__ == "__main__":
    main()
