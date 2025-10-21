import requests
from ..utils.kafka_producer import send_to_kafka

def fetch_remoteok_jobs():
    url = "https://remoteok.com/api"
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    if response.status_code != 200:
        print("RemoteOK API error:", response.status_code)
        return []

    jobs = response.json()[1:]  # first item is metadata
    normalized = []
    for job in jobs:
        normalized.append({
            "title": job.get("position"),
            "company": job.get("company"),
            "location": job.get("location") or "Remote",
            "remote": True,
            "skills": job.get("tags", []),
            "salary": job.get("salary") or "",
            "date_posted": job.get("date"),
            "source": "RemoteOK"
        })
    return normalized

def run_remoteok():
    jobs = fetch_remoteok_jobs()
    for job in jobs:
        send_to_kafka(job)
