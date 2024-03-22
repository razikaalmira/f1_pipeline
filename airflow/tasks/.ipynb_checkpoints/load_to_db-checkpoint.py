import psycopg2
from config import config

# def connect():
#     try:
#         conn = None

params = config()
# create connection
conn = psycopg2.connect(**params)
# create cursor
cur = conn.cursor()




        
cur.close()
conn.close()