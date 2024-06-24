import psycopg2

# connect to postgres database
conn = psycopg2.connect(
    dbname="nutn",
    user="nutn",
    password="nutn@password",
    host="172.18.8.152",
    port="5432",
)
cur = conn.cursor()

cur.execute(
    """
    SELECT city_code, COUNT(*) as cnt
    FROM users
    GROUP BY city_code
    ORDER BY cnt;
"""
)
results = cur.fetchall()

for row in results:
    city_code = row[0]
    cnt = row[1]
    print(f"居住城市: {city_code}, 總人數: {cnt} 人")
