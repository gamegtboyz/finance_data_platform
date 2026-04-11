import redshift_connector
import os
from dotenv import load_dotenv

load_dotenv()

conn = redshift_connector.connect(
    host = os.getenv('REDSHIFT_HOST'),
    port = os.getenv('REDSHIFT_PORT'),
    database = os.getenv('REDSHIFT_NAME'),
    user = os.getenv('REDSHIFT_USER'),
    password = os.getenv('REDSHIFT_PASSWORD')
)

cursor = conn.cursor()
cursor.execute("SELECT current_database(), current_user;")
print(cursor.fetchall())
conn.close()