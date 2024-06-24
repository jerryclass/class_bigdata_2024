import pandas as pd
import psycopg2
import warnings
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# database configure
username = "nutn"
password = "nutn@password"
encoded_password = quote_plus(password)
host = "172.18.8.152"
port = "5432"
database = "nutn"
connection_string = (
    f"postgresql+psycopg2://{username}:{encoded_password}@{host}:{port}/{database}"
)

# create database engine
engine = create_engine(connection_string)

# csv configure
chunksize = 10000
csv_file = "data/edu_bigdata_2024.csv"

# connect to postgres database
conn = psycopg2.connect(
    dbname=database,
    user=username,
    password=password,
    host=host,
    port=port,
)
cur = conn.cursor()


int_columns = ["month"]

float_columns = [
    "review_start_timestamp",
    "review_end_timestamp",
    "review_total_time",
    "review_finish_rate",
    "review_plus_timestamp",
    "exam_ans_time",
    "prac_during_time",
    "prac_score_rate",
    "prac_client_items_idle_time",
    "game_time",
]

date_columns = [
    "review_start_time",
    "review_end_time",
    "review_plus_view_time",
    "exam_ans_time",
    "prac_date",
    "prac_start_time",
    "prac_stop_time",
    "action_time",
    "video_start_time",
    "video_end_time",
    "last_modified",
]

# drop temp_table
sql_command = f"DROP TABLE IF EXISTS temp_table"
cur.execute(sql_command)
conn.commit()

# read data to temp tables
for chunk in pd.read_csv(
    csv_file, chunksize=chunksize, na_values=["NULL"], dtype="str"
):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=UserWarning)
        for col in int_columns:
            chunk[col] = pd.to_numeric(chunk[col], errors="coerce").astype(int)
        for col in float_columns:
            chunk[col] = pd.to_numeric(chunk[col], errors="coerce").astype(float)
        for col in date_columns:
            chunk[col] = pd.to_datetime(chunk[col], errors="coerce")

        chunk.to_sql("temp_table", con=engine, if_exists="append", index=False)


# create users tables
cur.execute(f"DROP TABLE IF EXISTS users")
cur.execute(
    f"""
        CREATE TABLE users AS (
            SELECT DISTINCT pseudo_id, city_code FROM temp_table
        );
    """
)
conn.commit()

# create user_organizations tables
cur.execute(f"DROP TABLE IF EXISTS user_organizations")
cur.execute(
    f"""
        CREATE TABLE user_organizations AS (
            SELECT DISTINCT pseudo_id, organization_id, grade, class FROM temp_table WHERE pseudo_id is not null
        )
    """
)
conn.commit()

# create videos tables
cur.execute(f"DROP TABLE IF EXISTS videos")
cur.execute(
    f"""
    CREATE TABLE videos AS (
        SELECT DISTINCT video_item_sn, subject, indicator FROM temp_table WHERE video_item_sn is not null
    )"""
)
conn.commit()

# create user_review_video_logs tables
cur.execute(f"DROP TABLE IF EXISTS user_review_video_logs")
cur.execute(
    f"""
    CREATE TABLE user_review_video_logs AS
    (
        SELECT DISTINCT 
        review_sn, 
        pseudo_id, 
        video_item_sn, 
        review_start_timestamp, 
        review_end_timestamp, 
        review_start_time, 
        review_end_time, 
        review_total_time, 
        review_finish_rate
        FROM temp_table
        WHERE review_sn is not null
    );
"""
)
conn.commit()


# create user_review_video_operation tables
cur.execute(f"DROP TABLE IF EXISTS user_review_video_operation")
cur.execute(
    f"""
    CREATE TABLE user_review_video_operation AS
    (
        SELECT 
        DISTINCT 
        review_plus_sn,
        review_sn,
        review_plus_view_time,
        review_plus_view_action,
        review_plus_timestamp,
        review_plus_turbo
        FROM temp_table
        WHERE review_plus_sn is not null
    );
"""
)
conn.commit()

# create video_question tables
cur.execute(f"DROP TABLE IF EXISTS video_question")
cur.execute(
    f"""
    CREATE TABLE video_question AS
    (
        SELECT
        DISTINCT
        question_sn,
        question_timestamp,
        video_item_sn
        FROM temp_table
        WHERE question_sn is not null
    );
"""
)
conn.commit()

# create user_review_video_answer_log tables
cur.execute(f"DROP TABLE IF EXISTS user_review_video_answer_log")
cur.execute(
    f"""
    CREATE TABLE user_review_video_answer_log AS
    (
        SELECT
        DISTINCT
        video_exam_sn,
        exam_ans_time,
        exam_binary_res,
        exam_timestamp,
        review_sn
        FROM temp_table
        WHERE video_exam_sn is not null
    );
"""
)
conn.commit()


# create user_practise_log tables
cur.execute(f"DROP TABLE IF EXISTS user_practise_log")
cur.execute(
    f"""
    CREATE TABLE user_practise_log AS
    (
        SELECT
        DISTINCT
        prac_sn,
        prac_date,
        prac_during_time,
        prac_score_rate,
        prac_start_time,
        prac_stop_time,
        prac_binary_res,
        prac_questions,
        prac_client_items_idle_time,
        pseudo_id
        FROM temp_table
        WHERE prac_sn is not null
    );
"""
)
conn.commit()

# create user_operate_history tables
cur.execute(f"DROP TABLE IF EXISTS user_operate_history")
cur.execute(
    f"""
    CREATE TABLE user_operate_history AS
    (
        SELECT
        DISTINCT
        sn1,
        action_time,
        action_name,
        classification,
        resource_name,
        video_len,
        video_start_time,
        video_end_time,
        video_action_time,
        search_keyword,
        search_count,
        platform,
        pseudo_id
        FROM temp_table
        WHERE sn1 is not null
    );
"""
)
conn.commit()


# create user_operate_level_history tables
cur.execute(f"DROP TABLE IF EXISTS user_operate_level_history")
cur.execute(
    f"""
    CREATE TABLE user_operate_level_history AS
    (
        SELECT
        DISTINCT
        sn2,
        use_math_view_id,
        use_grade_id,
        use_semester_id,
        unit_id,
        unit_name,
        answer_problem_num,
        is_answer,
        game_time,
        last_modified
        FROM temp_table
        WHERE sn2 is not null
    );
"""
)
conn.commit()

# drop tamp table
cur.execute(f"DROP TABLE IF EXISTS temp_table")
conn.commit()

cur.close()
conn.close()
print("Data written to database successfully.")
