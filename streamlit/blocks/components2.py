import pandas as pd
import psycopg2
from config import config

param_database = config('postgresql')

def init_connection():
    # create connection to postgresql dev schema
    conn = psycopg2.connect(**param_database)
    cur = conn.cursor()
    return conn, cur

def query_to_csv(conn,cur,query,file_name):
    cur.execute(query)
    tuples_list = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    df = pd.DataFrame(tuples_list,columns=colnames)
    df.to_csv('components2_results/'+file_name)

def close_connections():
    cur.close()
    conn.close()
    print("Database cursor and connections have been closed")


if __name__ == "__main__":

    conn, cur = init_connection()
    
    query_to_csv(conn,cur,
        '''SELECT distinct *
        FROM driver_standings
        WHERE season = extract(year from current_date);''',
        'driver_standings.csv'
    )

    query_to_csv(conn,cur,
        '''SELECT *
        FROM constructor_standings
        WHERE season = extract(year from current_date);''',
        'constructor_standings.csv'
    )

    query_to_csv(conn,cur,
        '''SELECT *
        FROM latest_race;''',
        'latest_race.csv'
    )

    query_to_csv(conn,cur,
        '''SELECT *
        FROM latest_race_result;''',
        'latest_race_result.csv'
    )

    query_to_csv(conn,cur,
        '''SELECT *
        FROM latest_fastest_lap;''',
        'latest_fastest_lap.csv'
    )
    
    query_to_csv(conn,cur,
        '''SELECT *
        FROM driver_cumulative_pts
        WHERE season = extract(year from current_date);''',
        'driver_cumulative_pts.csv'
    )

    query_to_csv(conn,cur,
        '''SELECT *
        FROM constructor_cumulative_pts
        WHERE season = extract(year from current_date);''',
        'constructor_cumulative_pts.csv'
    )

    query_to_csv(conn,cur,
        '''SELECT *
        FROM meetings_this_season;''',
        'meetings_this_season.csv'
    )

    query_to_csv(conn,cur,
        '''SELECT distinct team_name, concat('#',team_colour) colour
        FROM dim_driver;''',
        'team_colour.csv'
    )

    close_connections()