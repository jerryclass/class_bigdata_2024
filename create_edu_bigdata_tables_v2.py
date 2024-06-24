import yaml
import psycopg2
import pandas
import warnings
from sqlalchemy import create_engine
from urllib.parse import quote_plus


def read_config(config_file):
    with open(config_file, "r") as file:
        config = yaml.safe_load(file)
    return config


def create_db_connection(config):
    encoded_password = quote_plus(config["database"]["password"])
    connection_string = f"postgresql+psycopg2://{config['database']['username']}:{encoded_password}@{config['database']['host']}:{config['database']['port']}/{config['database']['dbname']}"
    engine = create_engine(connection_string)
    conn = psycopg2.connect(
        dbname=config["database"]["dbname"],
        user=config["database"]["username"],
        password=config["database"]["password"],
        host=config["database"]["host"],
        port=config["database"]["port"],
    )
    return engine, conn


def create_table(conn, table_name, select_sql):
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {table_name}")
    cur.execute(f"CREATE TABLE {table_name} AS ({select_sql});")
    conn.commit()


def drop_table(conn, table_name):
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.commit()
    cur.close()


def main(config_file):
    config = read_config(config_file)
    engine, conn = create_db_connection(config)

    # drop and create temp table
    drop_table(conn, "temp_table")
    for chunk in pandas.read_csv(
        config["csv"]["file"],
        chunksize=config["csv"]["chunksize"],
        na_values=["NULL"],
        dtype="str",
    ):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=UserWarning)

            for col in config["columns"]["int"]:
                chunk[col] = pandas.to_numeric(chunk[col], errors="coerce").astype(
                    "Int64"
                )
            for col in config["columns"]["float"]:
                chunk[col] = pandas.to_numeric(chunk[col], errors="coerce").astype(
                    "float32"
                )
            for col in config["columns"]["date"]:
                chunk[col] = pandas.to_datetime(chunk[col], errors="coerce")

            chunk.to_sql("temp_table", con=engine, if_exists="append", index=False)

    # drop and create real table
    for table in config["tables"]:
        create_table(conn, table["name"], table["create_sql"])

    # drop tamp table
    drop_table(conn, "temp_table")

    print("Data written to database successfully.")


if __name__ == "__main__":
    main("data/db_config.yml")
