import httpx
from bs4 import BeautifulSoup
from prefect import flow, task
import re
import psycopg2
import json
from serpapi import search
import pandas as pd
import datetime


@flow(name = 'Daily digest')
def daily_digest():

    fetch_all_HN_questions()
    google_mlops_job_search()




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











@flow(flow_run_name='Fetch Today\'s HN Questions')
def fetch_all_HN_questions():  
    '''This is the main flow that will parse the main page and then initiate 
    the subflows and tasks to fetch the questions, their age, the comments and save 
    everything in a postgresql database'''
    ##### Make a Web Request and feed the HTML into a parser #####
    res = httpx.get('https://news.ycombinator.com/ask')
    soup = BeautifulSoup(res.text, 'html.parser')
    ##### Use the Parser to find all the 'tr' HTML tags that have a #####
    ##### class of 'athing' #####
    question_tags = soup.find_all('tr', class_='athing')
    age_tags =  soup.find_all('span', class_='age')
    question_age_zip = zip(question_tags,age_tags)
    age_pattern =re.compile(r'(\d+)\s+(day|days|hour|hours|minute|minutes)\s+ago')
    # question_pattern = re.compile(r'<span class="titleline"><a href="item\?id=\d+">(.*?)</a></span>')
    for question, age in question_age_zip:
        age_matches = age_pattern.search(str(age))
        if 'days' not in age_matches[0]:
            age = age_matches[0]
            question_id = question.find('a', href=re.compile(r'item\?id=\d+'))
            question_id = question_id['href']
            question_soup = question.find_all('span', class_='titleline')[0]
            question = question_soup.a.get_text(separator=' ', strip=True)
            filter_daily_questions(question_id, question, age)


@flow(flow_run_name='Filter today\'s questions {question_id}')
def filter_daily_questions(question_id, question,age):
    '''takes all the questions in the scraped page and filters those that were added in the alst 24 hours'''
    comments = scrape_question_comments(question_id)
    # comments = json.dumps(comments)#serializing the list of strings into json so i can save it as TEXT in the sql table
    pg_con = 'postgres://postgres:mysecretpassword@localhost:5434/postgres'
    create_table(pg_con)
    insert_question(pg_con, question, age, comments)

@task
def scrape_question_comments(question_id):
    def extract_comments(comments_html):
        # Initialize an empty list to store the extracted sentences
        extracted_comments = []
        # Iterate through each comment
        for comment_html in comments_html:
            # Create a BeautifulSoup object for the current comment
            soup = BeautifulSoup(str(comment_html), 'html.parser')
            
            # Extract text from the comment (remove HTML tags)
            comment_text = soup.get_text(separator=' ', strip=True)
            
            # Append the extracted text to the list of sentences
            extracted_comments.append(comment_text)
        return extracted_comments
    
    res_question_body = httpx.get('https://news.ycombinator.com/'+question_id)
    soup_question_body = BeautifulSoup(res_question_body , 'html.parser')
    comments_html= soup_question_body.find_all('span', class_=["commtext c00", "commtext c5a"])
    question_comments = extract_comments(comments_html)
    return question_comments


@task
def create_table(conn_params):
    """
    Task to create a table if it doesn't exist.
    """
    try:
        with psycopg2.connect(conn_params) as conn:
            with conn.cursor() as cur:
                create_table_query = """
                CREATE TABLE IF NOT EXISTS questions (
                id SERIAL PRIMARY KEY,
                insertion_date DATE,
                question VARCHAR(255),
                age VARCHAR(255),
                comments TEXT
                );
                """
                cur.execute(create_table_query)
                conn.commit()
    except Exception as e:
        print(f"Error creating table: {e}")


@task
def insert_question(conn_params, question, age, comments):
    '''Task to insert a new question into the table.'''
    try:
        current_date = datetime.date.today()  # Get today's date
        with psycopg2.connect(conn_params) as conn:
            with conn.cursor() as cur:
                # Check if the question already exists
                check_query = "SELECT COUNT(*) FROM questions WHERE question = %s"
                cur.execute(check_query, (question,))
                count = cur.fetchone()[0]
                if count == 0:  # Question doesn't exist, insert it
                    insert_query = """
                    INSERT INTO questions (insertion_date, question, age, comments) VALUES (%s, %s, %s, %s)
                    """
                    comments_text = ' '.join(comments)
                    cur.execute(insert_query, (current_date, question, age, comments_text))
                    conn.commit()
    except Exception as e:
        print(f"Error inserting question: {e}")


if __name__ == "__main__":
    daily_digest()
