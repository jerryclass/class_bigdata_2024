import pandas
import psycopg2
from psycopg2.extras import DictCursor
import matplotlib.pyplot as plt

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
    ORDER BY cnt DESC;
"""
)
results = cur.fetchall()

for row in results:
    city_code = row[0]
    cnt = row[1]
    print(f"居住城市: {city_code}, 總人數: {cnt} 人")

# fetch data
city_codes = [row[0] for row in results]
cnts = [row[1] for row in results]

# use Matplotlib
plt.figure(figsize=(10, 6))
plt.bar(city_codes, cnts, color="skyblue")
plt.xlabel("City")
plt.ylabel("People Numbers")
plt.title("City People")
plt.xticks(rotation=100)
plt.tight_layout()
plt.savefig("results.png")
