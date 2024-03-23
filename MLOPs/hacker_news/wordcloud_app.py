import streamlit as st
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import psycopg2

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
    st.pyplot(plt.gcf())

def main():
    st.title("Word Cloud from PostgreSQL Database")
    
    # Fetch data from database
    data = fetch_data_from_database()
    data = data[:90]


    if not data:
        st.warning("No data fetched from the database.")
    else:
        # Generate and display word cloud
        generate_wordcloud(data)

if __name__ == "__main__":
    main()
