import streamlit as st
import pandas as pd
# import psycopg2
# import sqlalchemy
import blocks.components as bc
import plotly.express as px
import geopandas as gpd

# Initialize connection.
# conn = st.connection("postgresql", type="sql")


# Perform query.
# driver_standings = conn.query('SELECT * FROM driver_standings;', ttl="10m")
# driver_standings = conn.query(
#     'SELECT * FROM driver_standings;'
#     , ttl="10m")


# constructor_standings = conn.query('SELECT * FROM constructor_standings;', ttl="10m")

# df_latest_race = conn.query('SELECT * FROM latest_race;', ttl="10m")

# df4 = conn.query('SELECT * FROM latest_race_result order by positiontext desc;', ttl="10m")
# df5 = conn.query('SELECT * FROM latest_fastest_lap;', ttl="10m")
# # # Print results.
# # for row in df.itertuples():
# #     st.write(f"{row.driver_id}")

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
        st.subheader(f"Season {latest_race['season'][0]}, round {latest_race['round'][0]}")

    tab1,tab2,tab3 = st.tabs([
        "Overview",
        "Latest Race",
        "This season"
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
        st.caption(f"Up to the latest race: {latest_race['race_date'][0]}")
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🏎️ Driver Standings")
            # driver_standings = driver_standings.sort_values(by=['total_points'],ascending=False)
            # fig = px.bar(driver_standings,x='full_name',y='total_points',orientation='h',color_discrete_map=color_map)
            # fig.show()
            driver_std = driver_standings[['rank','total_points','full_name','constructor_name']]
            st.dataframe(driver_std,column_config={
                "rank":"Rank",
                "total_points":"Total Points",
                "full_name":"Driver",
                "constructor_name":"Constructor",
            }, hide_index=True)

            st.subheader("	🛠️ Constructor Standings")
            # fig = px.bar(constructor_standings,x='constructor_name',y='total_points',orientation='h',color_discrete_map=color_map)
            # fig.show()
            constructor_std = constructor_standings[['rank','total_points','constructor_name']]
            st.dataframe(constructor_std,column_config={
                "rank":"Rank",
                "total_points":"Total Points",
                "constructor_name":"Constructor",
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
                    title="Constructor Points Throughout the Season",
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
        st.subheader(f"{latest_race['racename'][0]}")
        st.write(f"	📅 {latest_race['race_date'][0]}")
        st.write(f" 📍 {latest_race['circuit_name'][0]}")
        
        st.subheader("🏁 Latest Race Result")
        latest_race_result = latest_race_result[['position','positiontext','relative_time','full_name','constructor_name','grid','points']].sort_values(by='position')
        st.dataframe(latest_race_result,column_config={
            "position":"Position",
            "positiontext":"Position Text",
            "relative_time":"Time",
            "full_name":"Driver",
            "constructor_name":"Constructor",
            "grid":"Starting Grid",
            "points":"Points"
        }, hide_index=True)

        st.subheader("🏁 Latest Fastest Lap")
        latest_fastest_lap = latest_fastest_lap[['full_name','constructor_name','fastest_lap_rank','lap','time','average_speed']].sort_values(by='average_speed',ascending=False)
        st.dataframe(latest_fastest_lap,column_config={
            "full_name":"Driver",
            "constructor_name":"Constructor",
            "fastest_lap_rank":"Fastest Lap Rank",
            "lap":"Lap",
            "time":"Time",
            "average_speed":"Average Speed"
        }, hide_index=True)

    with tab3:
        st.subheader(f"All meetings this {meetings['season'][0]} season")
        meetings_table = meetings[['round','race_date','racename','circuit_name','country']].sort_values(by='round')
        st.dataframe(meetings_table,column_config={
            "round":"Round",
            "race_date":"Race Date",
            "racename":"Race Name",
            "circuit_name":"Circuit",
            "country":"Country"
        }, hide_index=True)

        meetings_loc = meetings[['latitude','longitude','racename']]
        fig = px.scatter_geo(meetings_loc,
                             lat='latitude',
                             lon='longitude',
                             hover_name='racename',
                             projection='natural earth')
        st.plotly_chart(fig)

if __name__ == "__main__":
    streamlit_app()