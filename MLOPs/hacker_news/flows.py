from prefect import flow
from fetch_HN_questions_tasks import fetch_all_HN_questions
from google_mlops_tasks import google_mlops_job_search

@flow(name='Daily digest 2')
def daily_digest_2():
    fetch_all_HN_questions()
    google_mlops_job_search()
