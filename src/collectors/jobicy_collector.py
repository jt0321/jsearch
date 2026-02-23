import requests
from ..utils.kafka_producer import send_to_kafka

def fetch_jobicy_jobs():
    url = "https://jobicy.com/api/v2/remote-jobs"
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    if response.status_code != 200:
        print("Jobicy API error:", response.status_code)
        return []

    jobs = response.json().get("jobs", [])
    normalized = []
    for job in jobs:
        normalized.append({
            "title": job.get("jobTitle"),
            "company": job.get("companyName"),
            "location": job.get("jobGeo") or "Remote",
            "remote": True,
            "skills": [job.get("jobIndustry")] if job.get("jobIndustry") else [],
            "salary": f"{job.get('annualSalaryMin', '')}-{job.get('annualSalaryMax', '')} {job.get('salaryCurrency', '')}".strip("- "),
            "date_posted": job.get("pubDate"),
            "source": "Jobicy"
        })
    return normalized

def run_jobicy():
    jobs = fetch_jobicy_jobs()
    for job in jobs:
        send_to_kafka(job)
