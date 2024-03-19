import os
import json
import requests
import pandas as pd
import numpy as np
from datetime import date,datetime
import psycopg2
from io import StringIO
import boto3


current_script_path = os.path.abspath('')
parent_directory = os.path.dirname(current_script_path)
resolved_parent_directory = os.path.abspath(parent_directory)


# Initialize S3 Bucket
my_bucket = 'formula1-project-bucket' # already created on S3
csv_buffer = StringIO()
s3_resource = boto3.resource('s3')


class DataRetriever:
    def __init__(self,start_year,end_year,start_date,end_date):
        self.start_year = start_year
        self.end_year = end_year
        self.start_date = start_date
        self.end_date = end_date
        self.today = date.today()
        self.current_year = date.today().year
    
    def _fetch_data(self,url):
        try:
            response = requests.get(url)
            response.raise_for_status()
            json_data = response.json()
            return json_data
        except requests.exceptions.HTTPError as e:
            return f"Error: {e}"
        except requests.exceptions.RequestException as f:
            return f"Error: {f}"

    
class Ergast(DataRetriever):

    def __init__(self,start_year,end_year,start_date,end_date):
        super().__init__(start_year,end_year,start_date,end_date)
        self.base_url = "http://ergast.com/api/f1"
        self.max_round_todate = None

    def get_season_list(self):
        season_list = []
        for year in range(self.start_year, self.end_year + 1):
            url = f"{self.base_url}/{year}.json"
            fetched = self._fetch_data(url)
            df = pd.DataFrame(fetched['MRData']['RaceTable']['Races'])
            season_list.append(df)
        
        combined_season_df = pd.concat(season_list, ignore_index=True)
        combined_season_df['date'] = pd.to_datetime(combined_season_df['date']).dt.date
        combined_season_df['season'] = combined_season_df['season'].astype('int')
        combined_season_df['round'] = combined_season_df['round'].astype('int')
        
        if self.max_round_todate is None:
            filtered_date = combined_season_df[(combined_season_df['season'] == self.current_year) & (combined_season_df['date'] < self.today)]
            
            if not filtered_date.empty:
                max_round = filtered_date.loc[filtered_date['date'].idxmax(), 'round']
                self.max_round_todate = max_round.astype('int')
            else:
                print("Filtered data is empty. No max round found.")
    
        # upload to S3 bucket
        combined_season_df.to_csv(csv_buffer,index=False)
        s3_resource.Object(my_bucket, 'season_list.csv').put(Body=csv_buffer.getvalue())
        return combined_season_df
        
    def get_race_result(self):
        race_result = []
        for year in range(self.start_year,self.current_year+1):
            if year < self.current_year:
                for race_no in range(1,25):
                    url = f"{self.base_url}/{year}/{race_no}/results.json"
                    fetched = self._fetch_data(url)
                    if 'MRData' in fetched and 'RaceTable' in fetched['MRData'] and 'Races' in fetched['MRData']['RaceTable']:
                        race_data = fetched['MRData']['RaceTable']['Races']
                        if race_data:
                            df = pd.DataFrame(race_data[0]['Results'])
                            df['season'] = year
                            df['round'] = race_no
                            race_result.append(df)

            else:     
                for race_no in range(1,self.max_round_todate+1):
                    url = f"{self.base_url}/{year}/{race_no}/results.json"
                    fetched = self._fetch_data(url)
                    if 'MRData' in fetched and 'RaceTable' in fetched['MRData'] and 'Races' in fetched['MRData']['RaceTable']:
                        race_data = fetched['MRData']['RaceTable']['Races']
                        if race_data:
                            df = pd.DataFrame(race_data[0]['Results'])
                            df['season'] = year
                            df['round'] = race_no
                            race_result.append(df)
        
        combined_race_result = pd.concat(race_result,ignore_index=True)
        # upload to S3 bucket
        combined_race_result.to_csv(csv_buffer,index=False)
        s3_resource.Object(my_bucket, 'race_result.csv').put(Body=csv_buffer.getvalue())
        return combined_race_result


class OpenF1(DataRetriever):

    def __init__(self,start_year,end_year,start_date,end_date):
        super().__init__(start_year,end_year,start_date,end_date)
        self.base_url = "https://api.openf1.org/v1"
        self.session_key = None
    
    def get_meeting_info(self):
        url = f"{self.base_url}/meetings?year>={self.start_year}&year<={self.end_year}"
        fetched = self._fetch_data(url)
        meeting_info = pd.DataFrame(fetched)
        # upload to S3 bucket
        meeting_info.to_csv(csv_buffer,index=False)
        s3_resource.Object(my_bucket, 'meeting_info.csv').put(Body=csv_buffer.getvalue())
        return meeting_info
    
    
    def get_session_info(self):
        url = f"{self.base_url}/sessions?session_name=Race&date_start>={self.start_date}&date_end<={self.end_date}"
        fetched = self._fetch_data(url)
        session_info = pd.DataFrame(fetched)
        if self.session_key is None and not session_info.empty:
            self.session_key = list(set(session_info['session_key']))
            
        # upload to S3 bucket
        session_info.to_csv(csv_buffer,index=False)
        s3_resource.Object(my_bucket, 'session_info.csv').put(Body=csv_buffer.getvalue())
        return session_info
    
    
    def get_weather_info(self):
        weather_info = pd.DataFrame()
        try:
            # pull data only from related session keys (race sessions)
            for session in self.session_key:
                url = f"{self.base_url}/weather?session_key={session}"
                fetched = self._fetch_data(url)
                weather_info = weather_info._append(fetched,ignore_index=True)
                
            # upload to S3 bucket
            weather_info.to_csv(csv_buffer,index=False)
            s3_resource.Object(my_bucket, 'weather_info.csv').put(Body=csv_buffer.getvalue())
            return weather_info
        except TypeError as e:
            return f"Error: {e}, session info is empty, couldn't get weather info."
        except Exception as f:
            return f"Error: {f}"
    
    
    def get_laps_data(self):
        laps_data = pd.DataFrame()
        try:
            # pull data only from related session keys (race sessions)
            for session in self.session_key:
                url = f"{self.base_url}/laps?session_key={session}"
                fetched = self._fetch_data(url)
                laps_data = laps_data._append(fetched,ignore_index=True)

            # upload to S3 bucket
            laps_data.to_csv(csv_buffer,index=False)
            s3_resource.Object(my_bucket, 'laps_data.csv').put(Body=csv_buffer.getvalue())
            return laps_data
        except TypeError as e:
            return f"Error: {e}, session info is empty, couldn't get laps data."
        except Exception as f:
            return f"Error: {f}"


if __name__ == "__main__":
    start_year = date.today().year - 3
    end_year = date.today().year
    start_date = datetime(start_year,1,1).strftime("%Y-%m-%d")
    end_date = date.today()
    
    ergast = Ergast(start_year,end_year,start_date,end_date)
    openf1 = OpenF1(start_year,end_year,start_date,end_date)
    ergast.get_season_list()
    ergast.get_race_result()
    openf1.get_meeting_info()
    openf1.get_session_info()
    openf1.get_weather_info()
    openf1.get_laps_data()