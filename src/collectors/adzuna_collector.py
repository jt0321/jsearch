import requests
from ..utils.kafka_producer import send_to_kafka
from ..config import ADZUNA_APP_ID, ADZUNA_APP_KEY

def fetch_adzuna_jobs():
    url = f"https://api.adzuna.com/v1/api/jobs/us/search/1"
    params = {
        "app_id": ADZUNA_APP_ID,
        "app_key": ADZUNA_APP_KEY,
        "results_per_page": 50,
        "content-type": "application/json"
    }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        print("Adzuna API error:", response.status_code)
        return []

    jobs = response.json().get("results", [])
    normalized = []
    for job in jobs:
        normalized.append({
            "title": job.get("title"),
            "company": job.get("company", {}).get("display_name"),
            "location": job.get("location", {}).get("display_name"),
            "remote": "remote" in job.get("title", "").lower(),
            "skills": [],
            "salary": job.get("salary_is_predicted", "0"),
            "date_posted": job.get("created"),
            "source": "Adzuna"
        })
    return normalized

def run_adzuna():
    jobs = fetch_adzuna_jobs()
    for job in jobs:
        send_to_kafka(job)
