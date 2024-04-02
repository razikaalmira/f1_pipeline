# Formula 1 Data Pipeline
## Overview
This repository showcases a Formula 1 data pipeline which the result can be accessed through this [Streamlit dashboard](https://formulaone.streamlit.app).
## Architecture
![image](https://github.com/razikaalmira/f1_pipeline/assets/67381922/2e6d7bb7-c785-447c-88d1-8106b0f6ccd0)
This pipeline is orchestrated through Apache Airflow, deployed on an Amazon EC2 instance. The pipeline extracts data from Ergast and OpenF1 APIs, transforms it into CSV format, uploads it to an Amazon S3 bucket serving as a datalake, loads it into an Amazon RDS PostgreSQL database, and finally transforms the data using dbt (data build tool). The transformed data is then used to generate a dashboard using a Streamlit web application.


Please note that several architectural choices and decisions in this project may not prioritize efficiency but are made for the purpose of practice and learning.

## Usage
1. Airflow DAGs: Explore the Airflow DAGs (`airflow/dags/main_dags.py`) to understand the pipeline workflow and scheduled tasks.
2. Configuration: Configure the necessary connection settings for Ergast API, OpenF1 API, Amazon S3, Amazon RDS PostgreSQL, and dbt in the Airflow environment.
3. dbt DAGs: Explore the dbt nodes (`dbt/`) to understand the order of the data transformations.
4. Data Pipeline: Execute the data pipeline by triggering Airflow DAGs manually or on a predefined schedule.
5. Dashboard: Access the Streamlit dashboard (`streamlit/app.py`) to visualize Formula 1 data.
