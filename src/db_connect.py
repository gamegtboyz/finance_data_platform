import psycopg2
import os
from dotenv import load_dotenv
import redshift_connector

load_dotenv()

def db_connect():
    engine = os.getenv("DB_ENGINE", "postgres")
    if  engine == "redshift":
        conn = redshift_connector.connect(
            host = os.getenv("REDSHIFT_HOST"),
            port = os.getenv("REDSHIFT_PORT"),
            database = os.getenv("REDSHIFT_NAME"),
            user = os.getenv("REDSHIFT_USER"),
            password = os.getenv("REDSHIFT_PASSWORD")
        )
    else:
        conn = psycopg2.connect(
            host = os.getenv("DB_HOST"),
            port = os.getenv("DB_PORT"),
            dbname = os.getenv("DB_NAME"),
            user = os.getenv("DB_USER"),
            password = os.getenv("DB_PASSWORD")
        )
    
    return conn