from collectors.remoteok_collector import run_remoteok
from collectors.adzuna_collector import run_adzuna

def main():
    print("Fetching jobs...")
    run_remoteok()
    run_adzuna()
    print("Data pushed to Kafka successfully!")

if __name__ == "__main__":
    main()
