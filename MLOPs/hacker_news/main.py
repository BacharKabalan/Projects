from prefect import flow
from flows import daily_digest_2
from fetch_HN_questions_tasks import filter_daily_questions
from google_mlops_tasks import google_mlops_job_search

if __name__ == "__main__":
    daily_digest_2()