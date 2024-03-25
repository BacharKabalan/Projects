from prefect import flow, task
from serpapi import search
import pandas as pd
import datetime
import re
import psycopg2





@flow(flow_run_name = 'google_mlops_job_search')
def google_mlops_job_search():
    
    search_results, google_query = top_10_google_results()

    titles = [result['title'] for result in search_results['organic_results']]
    title_links = [result['link'] for result in search_results['organic_results']]
    job_numbers_per_result = find_numbers(titles)
    data = {google_query: titles,'job_count': job_numbers_per_result, 'title_links':title_links} # 
    top_10_google_search_results_df = pd.DataFrame(data)    

    conn_params = 'postgres://postgres:mysecretpassword@localhost:5434/postgres'
    create_sql_table_google_results(conn_params)
    # Get today's date
    current_date = datetime.date.today()

    # Iterate over DataFrame rows and insert each row into the SQL table
    for index, row in top_10_google_search_results_df.iterrows():
        insert_row_into_table(conn_params, current_date, row)





@task
def top_10_google_results():
    params = {
    "engine": "google",
    "q": "glassdoor mlops engineer jobs",
    "location": "United Kingdom",
    "hl": "en",
    "gl": "uk",
    "google_domain": "google.com",
    "num": "10",
    "start": "10",
    "safe": "active",
    "api_key": "c5c1ab2e9b960344525d642036948de7e7ed02e62c28ff06d3b4b0c3356ff9bd"
    }
    
    search_results = search(params)
    google_query = params['q']
    return search_results, google_query

@task
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

@task
def create_sql_table_google_results(conn_params):
    """
    Task to create a table if it doesn't exist.
    """
    try:
        with psycopg2.connect(conn_params) as conn:
            with conn.cursor() as cur:
                create_table_query = """
                CREATE TABLE IF NOT EXISTS my_dataframe (
                id SERIAL PRIMARY KEY,
                insertion_date DATE,
                search_result_title VARCHAR(255),
                job_count INTEGER,
                title_links TEXT
                );
                """
                cur.execute(create_table_query)
                conn.commit()
    except Exception as e:
        print(f"Error creating table: {e}")




@task
def insert_row_into_table(conn_params, current_date, row):
    try:
        with psycopg2.connect(conn_params) as conn:
            with conn.cursor() as cur:
                # Check if the question already exists in the table
                question_exists_query = """
                SELECT EXISTS(SELECT 1 FROM my_dataframe WHERE search_result_title = %s)
                """
                cur.execute(question_exists_query, (row[0],))
                question_exists = cur.fetchone()[0]
                if not question_exists:
                    # If the question does not exist, then insert it
                    insert_query = """
                    INSERT INTO my_dataframe (insertion_date, search_result_title, job_count, title_links)
                    VALUES (%s, %s, %s, %s)
                    """
                    cur.execute(insert_query, (current_date, ) + tuple(row))
                    conn.commit()
                else:
                    print("Question already exists in the table, skipping insertion.")
    except Exception as e:
        print(f"Error inserting row into SQL table: {e}")
