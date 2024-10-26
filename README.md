# ETL-with-Airflow-and-Snowflake-Batch-Processing-of-Weather-Data-on-GCP
## Weather Data ETL Pipeline Repository

This repository contains a set of files that are part of an ETL pipeline designed to fetch, process, and store weather data using Apache Airflow, Snowflake, and Open-Meteo API.

## Files Description

- **api-test.py**: This Python script is used to fetch historical weather data from the Open-Meteo API. It demonstrates how to send requests to the API and handle responses.

- **data.csv**: A CSV file that stores the weather data retrieved from the Open-Meteo API. This file contains processed and formatted weather data that can be easily consumed for analysis or reporting.

- **table_creation.sql**: Contains SQL commands for creating a table in Snowflake designed to store weather data. This script sets up the schema and defines the data types and comments for clarity.

- **weather_data_to_snowflake_dag.py**: This Python file contains an Apache Airflow DAG (Directed Acyclic Graph) that automates the entire process of fetching weather data from the Open-Meteo API, processing the data, and loading it into a Snowflake database table.

## How to Use

1. **Setting up the environment**: Ensure that Python, Apache Airflow, and Snowflake are set up in your environment. Install necessary libraries such as `requests_cache`, `pandas`, and `openmeteo_requests`.

2. **Running the scripts**:
   - To test the API and see the data retrieval process, run the `api-test.py` script.
   - Use `table_creation.sql` in your Snowflake environment to set up the database schema required for storing the weather data.
   - Deploy the `weather_data_to_snowflake_dag.py` in your Airflow environment to schedule and automate the data fetching and loading processes.

3. **Data Analysis**: Once the data is loaded into Snowflake, you can perform various analytical queries to derive insights from the weather data.

## Contributions

Feel free to fork this repository or contribute by providing feedback, enhancements, or new features through pull requests.

\

