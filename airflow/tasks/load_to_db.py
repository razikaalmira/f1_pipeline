import psycopg2
from config import config

def connect():
    try:
        conn = None
        params = config()
        conn = psycopg2.connect(**params)
        conn.set_session(autocommit=True)

        # create a cursor
        cur = conn.cursor()
        
        
cur.close()
conn.close()