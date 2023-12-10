import os

import psycopg2
from src.constants import DB_FIELDS

# Database connection parameters
dbname = "postgres"
user = "postgres"
password = os.getenv("POSTGRES_PASSWORD")
host = "localhost"

# Connect to the database
conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
cur = conn.cursor()


def try_execute_sql(sql: str):
    try:
        cur.execute(sql)
        conn.commit()
        print(f"Executed {sql} successfully")
    except Exception as e:
        print(f"Couldn't execute {sql} due to exception: {e}")
        conn.rollback()


def alter_table():
    primary_key_sql = f"""
    ALTER TABLE rappel_conso
    ADD PRIMARY KEY (DB_FIELDS[0]);
    """
    try_execute_sql(primary_key_sql)
    for field in DB_FIELDS[1:]:
        alter_table_sql = f"""
        ALTER TABLE rappel_conso
        ADD COLUMN {field} text;
        """
        try_execute_sql(alter_table_sql)

    cur.close()
    conn.close()


if __name__ == "__main__":
    alter_table()
