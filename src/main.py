from collectors.remoteok_collector import run_remoteok
from collectors.adzuna_collector import run_adzuna
from collectors.jobicy_collector import run_jobicy
from collectors.jsearch_collector import run_jsearch

def main():
    print("Fetching jobs...")
    run_remoteok()
    run_adzuna()
    run_jobicy()
    run_jsearch()
    print("Data pushed to Kafka successfully!")

if __name__ == "__main__":
    main()
