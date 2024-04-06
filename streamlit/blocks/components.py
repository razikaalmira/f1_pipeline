import streamlit as st
import pandas as pd

# Initialize connection
@st.cache_resource
def init_connection():
    _conn = st.connection("postgresql", type="sql")
    return _conn

@st.cache_data
def run_query(query,_conn) -> pd.DataFrame:
    return _conn.query(query, ttl="10m")


@st.cache_data
def get_driver_standings(_conn) -> pd.DataFrame:
    driver_standings = run_query(
        '''SELECT
            season,
            full_name,
            driver_number,
            constructor_name,
            nationality,
            total_points,
            rank
        FROM driver_standings;''',_conn)
    return driver_standings


@st.cache_data
def get_constructor_standings(_conn) -> pd.DataFrame:
    constructor_standings = run_query(
        '''SELECT
            season,
            constructor_name,
            total_points,
            rank
        FROM constructor_standings;''',_conn)
    return constructor_standings

@st.cache_data
def get_latest_race(_conn) -> pd.DataFrame:
    latest_race = run_query(
        '''SELECT
            season,
            round,
            racename,
            circuit_name,
            latitude,
            longitude,
            race_date,
            concat('Round ',round,': ',racename) racename_text
        FROM latest_race;''',_conn)
    return latest_race

@st.cache_data
def get_latest_race_result(_conn) -> pd.DataFrame:
    latest_race_result = run_query(
        '''SELECT
            season,
            round,
            full_name,
            constructor_name,
            status,
            position,
            positiontext,
            grid,
            points,
            relative_time
        FROM latest_race_result;''',_conn)
    return latest_race_result

@st.cache_data
def get_latest_fastest_lap(_conn) -> pd.DataFrame:
    latest_fastest_lap = run_query(
        '''SELECT
            season,
            round,
            full_name,
            constructor_name,
            fastest_lap_rank,
            lap,
            time,
            average_speed
        FROM latest_fastest_lap;''',_conn)
    return latest_fastest_lap

@st.cache_data
def get_driver_cumulative_pts(_conn) -> pd.DataFrame:
    driver_cumulative_pts = run_query(
        '''SELECT
            round,
            race_date,
            full_name,
            constructor_name,
            points,
            cumulative_points
        FROM driver_cumulative_pts;''',_conn)
    return driver_cumulative_pts

@st.cache_data
def get_constructor_cumulative_pts(_conn) -> pd.DataFrame:
    constructor_cumulative_pts = run_query(
        '''SELECT
            round,
            race_date,
            constructor_name,
            points,
            cumulative_points
        FROM constructor_cumulative_pts;''',_conn)
    return constructor_cumulative_pts

@st.cache_data
def get_meetings(_conn) -> pd.DataFrame:
    meetings = run_query(
        '''SELECT
            season,
            round,
            racename,
            circuit_name,
            country,
            locality,
            latitude::decimal,
            longitude::decimal,
            race_date
        FROM meetings_this_season;''',_conn)
    return meetings
