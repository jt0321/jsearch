import psycopg2

try:
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        user='admin',
        password='password',
        dbname='jobs_db'
    )
    cur = conn.cursor()
    cur.execute("SELECT * FROM jobs;")
    rows = cur.fetchall()
    print(f"Total rows in jobs table: {len(rows)}")
    for row in rows:
        print(row)
    cur.close()
    conn.close()
except Exception as e:
    print(f"Error connecting to Postgres: {e}")
