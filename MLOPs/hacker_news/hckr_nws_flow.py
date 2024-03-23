import httpx
from bs4 import BeautifulSoup
from prefect import flow, task
import re
import psycopg2
import json


@flow(name='Fetch Today\'s HN Questions')
def fetch_all_questions():  
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
    age_pattern =re.compile(r'(\d+)\s+(day|days|hour|hours)\s+ago')
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
    comments = json.dumps(comments)#serializing the list of strings into json so i can save it as TEXT in the sql table
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
        with psycopg2.connect(conn_params) as conn:
            with conn.cursor() as cur:
                insert_query = """
                INSERT INTO questions (question, age, comments) VALUES (%s, %s, %s)
                """
                ##### Extract text content from BeautifulSoup Tag objects #####
                # question_text = question.get_text(separator=' ', strip=True)
                # age_text = age.get_text(separator=' ', strip=True)
                # Combine comments into a single string
                comments_text = ' '.join(comments)
                cur.execute(insert_query, (question, age, comments_text))
                conn.commit()
    except Exception as e:
        print(f"Error inserting question: {e}")


if __name__ == "__main__":
    fetch_all_questions()
