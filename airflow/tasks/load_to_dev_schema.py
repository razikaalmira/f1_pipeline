import pandas as pd
import boto3
from io import StringIO
import json
import psycopg2
from config import config
from sqlalchemy import create_engine


my_bucket = 'formula1-project-bucket'
s3_resource = boto3.resource('s3')  
params = config()


# List all the csv in the bucket
def get_objects_from_s3():
    bucket = s3_resource.Bucket(my_bucket)
    objects = bucket.objects.all()
    object_list = []
    for o in objects:
        object_list.append(o.key)

    # Import all the csv in the bucket as dataframe
    for file_name in object_list:
        obj = s3_resource.Object(bucket_name=my_bucket, key=file_name)
        response = obj.get()
        data = response['Body'].read().decode('utf-8')
        globals()[file_name.split('.')[0]] = pd.read_csv(StringIO(data),on_bad_lines='skip')


def replace_single_to_double_quotes(dataframe,cols_to_replace):
    for column in cols_to_replace:
        dataframe[column] = dataframe[column].apply(lambda x: str(x).replace("'",'"'))


# Lower all column names
def lowercase_columns(dataframe_list):
    for dataframe in dataframe_list:
        dataframe.columns = [x.lower() for x in dataframe.columns]


# import configuration
def init_connection():
    # create connection
    conn = psycopg2.connect(**params,options='-csearch_path=dbo,dev')
    # create cursor
    cur = conn.cursor()
    conn_string = f"postgresql://{params['user']}:{params['password']}@{params['host']}/{params['database']}?options=-csearch_path%3Ddbo,dev"
    engine = create_engine(conn_string).connect()
    return conn, cur, engine

def create_table(conn,cur,sql_query):
    try:
        cur.execute(sql_query)
    except Exception as e:
        print(f"Error: {e}")
        print(f"Query: {sql_query}")
        conn.rollback()
    else:
        conn.commit()
        print(f"table has been created")
        
def insert_to_table(conn,dataframe,table_name):
    try:
        dataframe.to_sql(name=table_name,con=engine,if_exists='replace',index=False)
        # conn.autocommit = True
    except Exception as e:
        print(f"Error: {e}")
    else:
        conn.commit()
        print(f"{table_name} has been inserted")


laps_data_temp = """
    CREATE TABLE IF NOT EXISTS laps_data_temp (
        meeting_key            INTEGER,
        session_key            INTEGER,
        driver_number          INTEGER,
        i1_speed             DECIMAL,
        i2_speed             DECIMAL,
        st_speed             DECIMAL,
        date_start            DATE,
        lap_duration         DECIMAL,
        is_pit_out_lap          BOOLEAN,
        duration_sector_1    DECIMAL,
        duration_sector_2    DECIMAL,
        duration_sector_3    DECIMAL,
        segments_sector_1     VARCHAR(100),
        segments_sector_2     VARCHAR(100),
        segments_sector_3     VARCHAR(100),
        lap_number             INTEGER
        );
        """

meeting_info_temp = """
    CREATE TABLE IF NOT EXISTS meeting_info_temp (
        meeting_name             VARCHAR(100),
        meeting_official_name    VARCHAR(100),
        location                 VARCHAR(100),
        country_key               INTEGER,
        country_code             VARCHAR(100),
        country_name             VARCHAR(100),
        circuit_key               INTEGER,
        circuit_short_name       VARCHAR(100),
        date_start               VARCHAR(100),
        gmt_offset               TIME,
        meeting_key               INTEGER,
        year                      INTEGER,
        meeting_code             VARCHAR(100)
        );
        """

race_result_temp = """
    CREATE TABLE IF NOT EXISTS race_result_temp (
        number            INTEGER,
        position          INTEGER,
        positionText     VARCHAR(100),
        points          DECIMAL,
        Driver           JSONB,
        Constructor      JSONB,
        grid              INTEGER,
        laps              INTEGER,
        status           VARCHAR(100),
        Time             JSONB,
        FastestLap       JSONB,
        season            INTEGER,
        round             INTEGER
        );
        """

season_list_temp = """
    CREATE TABLE IF NOT EXISTS season_list_temp (
        season             INTEGER,
        round              INTEGER,
        url               VARCHAR(100),
        raceName          VARCHAR(100),
        Circuit           JSONB,
        date              DATE,
        time              VARCHAR(100),
        FirstPractice     JSONB,
        SecondPractice    JSONB,
        ThirdPractice     JSONB,
        Qualifying        JSONB,
        Sprint            JSONB
        );
        """

session_info_temp = """
    CREATE TABLE IF NOT EXISTS session_info_temp (
        location              VARCHAR(100),
        country_key            INTEGER,
        country_code          VARCHAR(100),
        country_name          VARCHAR(100),
        circuit_key            INTEGER,
        circuit_short_name    VARCHAR(100),
        session_type          VARCHAR(100),
        session_name          VARCHAR(100),
        date_start            VARCHAR(100),
        date_end              VARCHAR(100),
        gmt_offset            TIME,
        session_key            INTEGER,
        meeting_key            INTEGER,
        year                   INTEGER
        );
        """


weather_info_temp = """
    CREATE TABLE IF NOT EXISTS weather_info_temp (
        meeting_key            INTEGER,
        session_key            INTEGER,
        date                  VARCHAR(100),
        air_temperature      DECIMAL,
        humidity             DECIMAL,
        pressure             DECIMAL,
        rainfall               INTEGER,
        track_temperature    DECIMAL,
        wind_direction         INTEGER,
        wind_speed           DECIMAL
        );
        """


def close_connections():
    cur.close()
    conn.close()
    print("Database cursor and connections closed.")

if __name__ == "__main__":
    get_objects_from_s3()

    replace_single_to_double_quotes(race_result,['Driver','Constructor','Time','FastestLap'])
    replace_single_to_double_quotes(season_list,['Circuit', 'FirstPractice', 'SecondPractice', 'ThirdPractice', 'Qualifying', 'Sprint'])
    race_result.loc[race_result['Time']=='nan','Time'] = {"millis": "", "time": ""}
    lowercase_columns([race_result,laps_data,weather_info,season_list,session_info,meeting_info])
    
    conn, cur, engine = init_connection()

    pairs = [[race_result,'race_result_temp',race_result_temp],
            [laps_data,'laps_data_temp',laps_data_temp],
            [weather_info,'weather_info_temp',weather_info_temp],
            [season_list,'season_list_temp',season_list_temp],
            [session_info,'session_info_temp',session_info_temp],
            [meeting_info,'meeting_info_temp',meeting_info_temp]]
    
    for df,tablename,query in pairs:
        create_table(conn,cur,query)
        insert_to_table(conn,df,tablename)

    close_connections()