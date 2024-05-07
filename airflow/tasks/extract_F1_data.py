import os
import json
import requests
import pandas as pd
from datetime import date,datetime
from io import StringIO
import boto3


current_script_path = os.path.abspath('')
parent_directory = os.path.dirname(current_script_path)
resolved_parent_directory = os.path.abspath(parent_directory)


# Initialize S3 Bucket
my_bucket = 'formula1-project-bucket' # already created on S3
s3_client = boto3.client('s3')
csv_buffer = StringIO()


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

    def _upload_to_s3(self,dataframe,filename):
        try:
            with StringIO() as csv_buffer:
                dataframe.to_csv(csv_buffer,index=False)
                response = s3_client.put_object(Body=csv_buffer.getvalue(),Bucket=my_bucket,Key=f'{filename}')
                status = response.get("ResponseMetadata",{}).get("HTTPStatusCode")
                if status == 200:
                    status_text = f"Successful S3 put_object {dataframe} response. Status - {status}."
                else:
                    status_text = f"Unsuccessful S3 put_object {dataframe} response. Status - {status}."      
            return status_text
        except Exception as e:
            return f"Error: {e}"
    
# class Ergast(DataRetriever):

#     def __init__(self,start_year,end_year,start_date,end_date):
#         super().__init__(start_year,end_year,start_date,end_date)
#         self.base_url = "http://ergast.com/api/f1"
#         self.max_round_todate = None

#     def get_season_list(self):
#         season_list = []
#         for year in range(self.start_year, self.end_year + 1):
#             url = f"{self.base_url}/{year}.json"
#             fetched = self._fetch_data(url)
#             df = pd.DataFrame(fetched['MRData']['RaceTable']['Races'])
#             season_list.append(df)
            
#         if season_list:
#             season_list_df = pd.concat(season_list, ignore_index=True)
#             season_list_df['date'] = pd.to_datetime(season_list_df['date']).dt.date
#             season_list_df['season'] = season_list_df['season'].astype('int')
#             season_list_df['round'] = season_list_df['round'].astype('int')
            
#             if self.max_round_todate is None:
#                 filtered_date = season_list_df[(season_list_df['season'] == self.current_year) & (season_list_df['date'] < self.today)]
                
#                 if not filtered_date.empty:
#                     max_round = filtered_date.loc[filtered_date['date'].idxmax(), 'round']
#                     self.max_round_todate = max_round.astype('int')
#                 else:
#                     print("Filtered data is empty. No max round found.")
    
            
#             # upload to S3 bucket
#             result = self._upload_to_s3(season_list_df,'season_list.csv')
    
#             return result
#         else:
#             return "No season_list data fetched for any year."
        
#     def get_race_result(self):
#         race_result = []
#         for year in range(self.start_year,self.current_year+1):
#             if year < self.current_year:
#                 for race_no in range(1,25):
#                     url = f"{self.base_url}/{year}/{race_no}/results.json"
#                     fetched = self._fetch_data(url)
#                     if 'MRData' in fetched and 'RaceTable' in fetched['MRData'] and 'Races' in fetched['MRData']['RaceTable']:
#                         race_data = fetched['MRData']['RaceTable']['Races']
#                         if race_data:
#                             df = pd.DataFrame(race_data[0]['Results'])
#                             df['season'] = year
#                             df['round'] = race_no
#                             race_result.append(df)

#             else:     
#                 for race_no in range(1,self.max_round_todate+1):
#                     url = f"{self.base_url}/{year}/{race_no}/results.json"
#                     fetched = self._fetch_data(url)
#                     if 'MRData' in fetched and 'RaceTable' in fetched['MRData'] and 'Races' in fetched['MRData']['RaceTable']:
#                         race_data = fetched['MRData']['RaceTable']['Races']
#                         if race_data:
#                             df = pd.DataFrame(race_data[0]['Results'])
#                             df['season'] = year
#                             df['round'] = race_no
#                             race_result.append(df)
                            
#         if race_result:
#             race_result_df = pd.concat(race_result,ignore_index=True)
    
#             # upload to S3 bucket
#             result = self._upload_to_s3(race_result_df,'race_result.csv')
#             return result
#         else:
#             return "No race_result data fetched for any year."


class OpenF1(DataRetriever):

    def __init__(self,start_year,end_year,start_date,end_date):
        super().__init__(start_year,end_year,start_date,end_date)
        self.base_url = "https://api.openf1.org/v1"
        self.session_key = None
    
    def get_meeting_info(self):
        url = f"{self.base_url}/meetings?year>={self.start_year}&year<={self.end_year}"
        fetched = self._fetch_data(url)
        meeting_info = pd.DataFrame(fetched)
        if not meeting_info.empty:
            # upload to S3 bucket
            result = self._upload_to_s3(meeting_info,'meeting_info.csv')
            return result
        else:
            return "No meeting_info data fetched for any year."
    
    
    def get_session_info(self):
        url = f"{self.base_url}/sessions?session_name=Race&date_start>={self.start_date}&date_end<={self.end_date}"
        fetched = self._fetch_data(url)
        session_info = pd.DataFrame(fetched)
        if self.session_key is None and not session_info.empty:
            self.session_key = list(set(session_info['session_key']))
            
        if not session_info.empty:
            # upload to S3 bucket
            result = self._upload_to_s3(session_info,'session_info.csv')
            return result
        else:
            return "No session_info data fetched for any year."
    
    
    def get_weather_info(self):
        weather_info = pd.DataFrame()
        try:
            # pull data only from related session keys (race sessions)
            for session in self.session_key:
                url = f"{self.base_url}/weather?session_key={session}"
                fetched = self._fetch_data(url)
                weather_info = weather_info._append(fetched,ignore_index=True)
                
            if not weather_info.empty:
                # upload to S3 bucket
                result = self._upload_to_s3(weather_info,'weather_info.csv')
                return result
            else:
                return "No weather_info data fetched for any year."
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

            if not laps_data.empty:
                # upload to S3 bucket
                result = self._upload_to_s3(laps_data,'laps_data.csv')
                return result
            else:
                return "No laps_data data fetched for any year."
        except TypeError as e:
            return f"Error: {e}, session info is empty, couldn't get laps data."
        except Exception as f:
            return f"Error: {f}"

    def get_drivers_data(self):
        drivers_data = pd.DataFrame()
        try:
            for session in self.session_key:
                url = f"{self.base_url}/drivers?session_key={session}"
                fetched = self._fetch_data(url)
                drivers_data = drivers_data.append(fetched,ignore_index=True)
                
            if drivers_data:
                result = self._upload_to_s3(drivers_data,'drivers_data.csv')
                return result
            else:
                return "No drivers_data fetched for any year"
        except TypeError as e:
            return f"Error: {e}, session info is empty, couldn't get laps data."
        except Exception as f:
            return f"Error: {f}"
        
    def get_position_data(self):
        position_data = pd.DataFrame()
        try:  
            for session in self.session_key:
                url = f"{self.base_url}/position?session_key={session}"
                fetched = self._fetch_data(url)
                position_data_initial = position_data.append(fetched,ignore_index=True)
                latest = position_data_initial.groupby(['meeting_key','position'])['date'].idxmax()
                position_data = position_data_initial.loc[latest]

            if position_data:
                result = self._upload_to_s3(position_data,'position_data.csv')
                return result
            else:
                return "No position_data fetched for any year"
        except TypeError as e:
            return f"Error: {e}, session info is empty, couldn't get laps data."
        except Exception as f:
            return f"Error: {f}"

        
           
if __name__ == "__main__":
    start_year = date.today().year - 3
    end_year = date.today().year
    start_date = datetime(start_year,1,1).strftime("%Y-%m-%d")
    end_date = date.today()

    openf1 = OpenF1(start_year,end_year,start_date,end_date)
    openf1.get_meeting_info()
    openf1.get_session_info()
    openf1.get_weather_info()
    openf1.get_laps_data()
    openf1.get_drivers_data()
    openf1.get_position_data()