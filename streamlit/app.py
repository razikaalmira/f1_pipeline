import streamlit as st
import pandas as pd
import plotly.express as px
# import geopandas as gpd

st.set_page_config(layout="wide",page_title="streamlit: Formula 1 Results",page_icon="🏎️")
# Initialize connection.
# _conn = bc.init_connection()

def streamlit_app():
    # retrieve dataframes from RDS
    # driver_standings = bc.get_driver_standings(_conn)
    # constructor_standings = bc.get_constructor_standings(_conn)
    # driver_cumulative_pts = bc.get_driver_cumulative_pts(_conn)
    # constructor_cumulative_pts = bc.get_constructor_cumulative_pts(_conn)
    # latest_race = bc.get_latest_race(_conn)
    # latest_race_result = bc.get_latest_race_result(_conn)
    # latest_fastest_lap = bc.get_latest_fastest_lap(_conn)
    # meetings = bc.get_meetings(_conn)

    # retrieve dataframes from local
    driver_standings = pd.read_csv(r'./blocks/driver_standings.csv') 
    constructor_standings = pd.read_csv(r'./blocks/constructor_standings.csv') 
    driver_cumulative_pts = pd.read_csv(r'./blocks/driver_cumulative_pts.csv') 
    constructor_cumulative_pts = pd.read_csv(r'./blocks/constructor_cumulative_pts.csv') 
    latest_race = pd.read_csv(r'./blocks/latest_race.csv') 
    latest_race_result = pd.read_csv(r'./blocks/latest_race_result.csv') 
    latest_fastest_lap = pd.read_csv(r'./blocks/latest_fastest_lap.csv') 
    meetings = pd.read_csv(r'./blocks/meetings_this_season.csv') 
    team_colour = pd.read_csv(r'./blocks/team_colour.csv')

    with st.container():
        st.markdown(
            '<img height="140" width="140" src="https://cdn.simpleicons.org/f1/10600">',
			unsafe_allow_html=True,
        )
        st.title("Formula 1")
        st.subheader(f"Season {latest_race['season'][0]}")

    tab1,tab2 = st.tabs([
        "Overview",
        "Race Result"
        # "This Season"
    ])
    
    color_map = pd.Series(team_colour.colour.values,index=team_colour.team_name).to_dict()
# -------------- Overview Tab -------------
    with tab1:
        st.header("Overview")
        # st.caption(f"Up to the latest race: {latest_race['race_date'][0]}")
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🏎️ Driver Standings")
            driver_std = driver_standings[['rank','total_points','full_name','headshot_url','team_name','country_code']].sort_values(by='rank',ascending=True)

            st.dataframe(driver_std,column_config={
                "rank":"Rank",
                "total_points":"Total Points",
                "full_name":"Driver",
                "team_name":"Constructor",
                "country_code":"Country",
                "headshot_url":st.column_config.ImageColumn("photo", width="medium"),
            }, hide_index=True)
            st.subheader("	🛠️ Constructor Standings")
            constructor_std = constructor_standings[['rank','total_points','team_name']].sort_values(by='total_points',ascending=False)
            st.dataframe(constructor_std,column_config={
                "rank":"Rank",
                "total_points":"Total Points",
                "constructor_name":"Constructor"
            }, hide_index=True)

        with col2:
            fig = px.line(driver_cumulative_pts, x="round", y="cumulative_points",
                    line_group="full_name",
                    color="team_name",
                    color_discrete_map=color_map, 
                    labels={
                        "round":"Round",
                        "cumulative_points":"Cumulative Points",
                        "team_name":"Constructor",
                        "full_name":"Driver"
                    },
                    title="Driver Points Throughout the Season",
                    markers=True)
            fig.update_layout(xaxis = dict(dtick = 1))
            st.plotly_chart(fig)

            fig = px.line(constructor_cumulative_pts, x="round", y="cumulative_points",
                    color="team_name",
                    color_discrete_map=color_map, 
                    labels={
                        "round":"Round",
                        "cumulative_points":"Cumulative Points",
                        "team_name":"Constructor"
                    },
                    title="Constructor Points Throughout the Season",
                    markers=True)
            fig.update_layout(xaxis = dict(dtick = 1))
            st.plotly_chart(fig)

#----------- Latest Race Tab -----------
    with tab2:
        option = st.selectbox(
            label="Use the dropdown to choose a race.",
            options=latest_race['meeting_name'],
            placeholder="Choose a race",
            label_visibility="collapsed")

        
        if option:
            round = latest_race[latest_race['meeting_name'] == option]['round'].values[0]

            st.subheader(f"{latest_race[latest_race['meeting_name'] == option]['meeting_official_name'].values[0]}")
            st.write(f"	📅 {latest_race[latest_race['meeting_name'] == option]['race_date'].values[0]}")
            st.write(f" 📍 {latest_race[latest_race['meeting_name'] == option]['circuit_short_name'].values[0]}")

            col1,col2 = st.columns(2)
            with  col1:
                st.subheader("🏁 Race Result")
                latest_race = latest_race_result[latest_race_result['round'] == round][['position','full_name','team_name','points']].sort_values(by='position')
                
                st.dataframe(latest_race,column_config={
                "position":"Rank",
                "full_name":"Full Name",
                "team_name":"Constructor",
                "points":"Points"
                }, hide_index=True)
            
            
            with col2:
                st.subheader("🏁 Fastest Lap")
                latest_fastest_lap = latest_fastest_lap[latest_fastest_lap['round'] == round][['full_name','team_name','fastest_lap_rank','lap_number','duration']].sort_values(by='duration',ascending=True)
                
                st.dataframe(latest_fastest_lap,column_config={
                    "full_name":"Full Name",
                    "team_name":"Team Name",
                    "fastest_lap_rank":"Fastest Lap Rank",
                    "lap_number":"Lap Number",
                    "duration":"Duration (seconds)"
                }, hide_index=True)


if __name__ == "__main__":
    streamlit_app()
