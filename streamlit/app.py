import streamlit as st
import pandas as pd
import blocks.components as bc
import plotly.express as px
import geopandas as gpd

st.set_page_config(layout="wide",page_title="streamlit: Formula 1 Results",page_icon="🏎️")
# Initialize connection.
_conn = bc.init_connection()

def streamlit_app():
    # retrieve dataframes
    driver_standings = bc.get_driver_standings(_conn)
    constructor_standings = bc.get_constructor_standings(_conn)
    driver_cumulative_pts = bc.get_driver_cumulative_pts(_conn)
    constructor_cumulative_pts = bc.get_constructor_cumulative_pts(_conn)
    latest_race = bc.get_latest_race(_conn)
    latest_race_result = bc.get_latest_race_result(_conn)
    latest_fastest_lap = bc.get_latest_fastest_lap(_conn)
    meetings = bc.get_meetings(_conn)

    with st.container():
        st.markdown(
            '<img height="140" width="140" src="https://cdn.simpleicons.org/f1/10600">',
			unsafe_allow_html=True,
        )
        st.title("Formula 1")
        st.subheader(f"Season {latest_race['season'][0]}")

    tab1,tab2,tab3 = st.tabs([
        "Overview",
        "Race Result",
        "This Season"
    ])

    color_map = {
        "McLaren":"#FF8000",
        "Sauber":"#DE3126",
        "Haas F1 Team":"#E6002B",
        "Ferrari":"#ff2800",
        "Mercedes":"#00A19B",
        "AlphaTauri":"#20394C",
        "Red Bull":"#223971",
        "Aston Martin":"#002420",
        "RB F1 Team":"#1434CB",
        "Alpine F1 Team":"#055CAA",
        "Alfa Romeo":"#A42138",
        "Williams":"#2596BE"
    }
# -------------- Overview Tab -------------
    with tab1:
        st.header("Overview")
        # st.caption(f"Up to the latest race: {latest_race['race_date'][0]}")
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🏎️ Driver Standings")
            driver_std = driver_standings[['rank','total_points','full_name','constructor_name','nationality']]
            st.dataframe(driver_std,column_config={
                "rank":"Rank",
                "total_points":"Total Points",
                "full_name":"Driver",
                "constructor_name":"Constructor",
                "nationality":"Nationality"
            }, hide_index=True)

            st.subheader("	🛠️ Constructor Standings")
            constructor_std = constructor_standings[['rank','total_points','constructor_name']]
            st.dataframe(constructor_std,column_config={
                "rank":"Rank",
                "total_points":"Total Points",
                "constructor_name":"Constructor"
            }, hide_index=True)

        with col2:
            fig = px.line(driver_cumulative_pts, x="round", y="cumulative_points",
                    line_group="full_name",
                    color="constructor_name",
                    color_discrete_map=color_map, 
                    labels={
                        "round":"Round",
                        "cumulative_points":"Cumulative Points",
                        "constructor_name":"Constructor",
                        "full_name":"Driver"
                    },
                    title="Driver Points Throughout the Season",
                    markers=True)
            fig.update_layout(xaxis = dict(dtick = 1))
            st.plotly_chart(fig)

            fig = px.line(constructor_cumulative_pts, x="round", y="cumulative_points",
                    color="constructor_name",
                    color_discrete_map=color_map, 
                    labels={
                        "round":"Round",
                        "cumulative_points":"Cumulative Points",
                        "constructor_name":"Constructor"
                    },
                    title="Constructor Points Throughout the Season",
                    markers=True)
            fig.update_layout(xaxis = dict(dtick = 1))
            st.plotly_chart(fig)

#----------- Latest Race Tab -----------
    with tab2:
        option = st.selectbox(
            label="Use the dropdown to choose a race.",
            options=latest_race['racename_text'],
            placeholder="Choose a race",
            label_visibility="collapsed")
    

        if option:
            round = latest_race[latest_race['racename_text'] == option]['round'].values[0]

            st.subheader(f"{latest_race[latest_race['racename_text'] == option]['racename'].values[0]}")
            st.write(f"	📅 {latest_race[latest_race['racename_text'] == option]['race_date'].values[0]}")
            st.write(f" 📍 {latest_race[latest_race['racename_text'] == option]['circuit_name'].values[0]}")

            col1, col2 = st.columns(2)

            with col1:
                st.subheader("🏁 Race Result")
                latest_race_result = latest_race_result[latest_race_result['round'] == round][['position','positiontext','relative_time','full_name','constructor_name','grid','points']].sort_values(by='position')
                st.dataframe(latest_race_result,column_config={
                    "position":"Position",
                    "positiontext":"Position Text",
                    "relative_time":"Time",
                    "full_name":"Driver",
                    "constructor_name":"Constructor",
                    "grid":"Starting Grid",
                    "points":"Points"
                }, hide_index=True)
            
            with col2:
                st.subheader("🏁 Fastest Lap")
                latest_fastest_lap = latest_fastest_lap[latest_fastest_lap['round'] == round][['full_name','constructor_name','fastest_lap_rank','lap','time','average_speed']].sort_values(by='average_speed',ascending=False)
                st.dataframe(latest_fastest_lap,column_config={
                    "full_name":"Driver",
                    "constructor_name":"Constructor",
                    "fastest_lap_rank":"Fastest Lap Rank",
                    "lap":"Lap",
                    "time":"Time",
                    "average_speed":"Average Speed"
                }, hide_index=True)

    with tab3:
        col1,col2 = st.columns(2)

        with col1:
            st.subheader(f"All meetings this {meetings['season'][0]} season")
            meetings_table = meetings[['round','race_date','racename','circuit_name','country']].sort_values(by='round')
            st.dataframe(meetings_table,column_config={
                "round":"Round",
                "race_date":"Race Date",
                "racename":"Race Name",
                "circuit_name":"Circuit",
                "country":"Country"
            }, hide_index=True)

        with col2:
            meetings_loc = meetings[['latitude','longitude','racename']]
            fig = px.scatter_mapbox(meetings_loc,
                                lat='latitude',
                                lon='longitude')
                                # hover_name='racename')
                                # projection='natural earth')
            st.plotly_chart(fig)

if __name__ == "__main__":
    streamlit_app()
