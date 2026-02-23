import requests
from ..utils.kafka_producer import send_to_kafka
from ..config import RAPIDAPI_KEY

def fetch_jsearch_jobs():
    if not RAPIDAPI_KEY:
        print("RapidAPI Key missing. Skipping JSearch.")
        return []

    url = "https://jsearch.p.rapidapi.com/search"
    querystring = {"query":"remote software engineer","page":"1","num_pages":"1"}
    headers = {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": "jsearch.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)
    if response.status_code != 200:
        print("JSearch (RapidAPI) error:", response.status_code)
        return []

    jobs = response.json().get("data", [])
    normalized = []
    for job in jobs:
        qualifications = job.get("job_highlights", {}).get("Qualifications", [])
        
        normalized.append({
            "title": job.get("job_title"),
            "company": job.get("employer_name"),
            "location": job.get("job_city", "Remote") or "Remote",
            "remote": job.get("job_is_remote", False),
            "skills": qualifications,
            "salary": f"{job.get('job_min_salary') or ''}-{job.get('job_max_salary') or ''}".strip("- "),
            "date_posted": job.get("job_posted_at_datetime_utc"),
            "source": "JSearch (RapidAPI)"
        })
    return normalized

def run_jsearch():
    jobs = fetch_jsearch_jobs()
    for job in jobs:
        send_to_kafka(job)
