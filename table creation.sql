USE DATABASE WEATHERFORCASTING;
USE SCHEMA BRISTOL;

CREATE OR REPLACE TABLE weather_data (
    date TIMESTAMP_NTZ COMMENT 'Date of the weather forecast (non-timezone-specific)',
    weather_code NUMBER(5, 2) COMMENT 'Weather code, typically an integer or decimal indicator',
    temperature_2m_max FLOAT COMMENT 'Max temperature at 2 meters above ground in Celsius',
    temperature_2m_min FLOAT COMMENT 'Min temperature at 2 meters above ground in Celsius',
    apparent_temperature_max FLOAT COMMENT 'Max apparent temperature considering wind/humidity',
    apparent_temperature_min FLOAT COMMENT 'Min apparent temperature considering wind/humidity',
    sunrise TIME COMMENT 'Sunrise time as a Snowflake TIME data type',
    sunset TIME COMMENT 'Sunset time as a Snowflake TIME data type',
    daylight_duration FLOAT COMMENT 'Duration of daylight in seconds',
    sunshine_duration FLOAT COMMENT 'Total sunshine duration in seconds',
    uv_index_max NUMBER(3, 1) COMMENT 'Maximum UV index for the day'
) COMMENT='Table storing daily weather forecast data for Bristol';