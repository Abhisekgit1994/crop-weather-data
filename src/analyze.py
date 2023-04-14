import pandas as pd
import datetime
import logging
import psycopg2
import numpy as np
import os
import time
from db_conn import Database
from data_operations import FetchData


class Analysis:
    def __init__(self):
        self.db = Database()
        self.weather_table_name = 'weather_data'
        self.fetch_queries = {
            self.weather_table_name: f"SELECT * FROM {self.weather_table_name} WHERE 1=1",
        }
        self.create_table = f"CREATE TABLE IF NOT EXISTS weather_data_stats (year integer, station_id varchar(100), avg_max_temperature float, avg_min_temperature float, total_precipitation_amount float)"
        self.insert_query = f"INSERT INTO weather_data_stats (year, station_id, avg_max_temperature, avg_min_temperature, total_precipitation_amount) VALUES (%s, %s, %s, %s, %s)"
        self.duplicate_check = f"SELECT EXISTS(SELECT 1 FROM weather_data_stats WHERE year=%s AND station_id=%s AND avg_max_temperature=%s AND avg_min_temperature=%s AND total_precipitation_amount=%s )"

    def analyzeData(self):
        """
        For every year, for every weather station, calculate:

        * Average maximum temperature (in degrees Celsius)  data/10
        * Average minimum temperature (in degrees Celsius)  data/10
        * Total accumulated precipitation (in centimeters) data/100  10*mm -> /10 -> /10 -> cm

        Ignore missing data when calculating these statistics.
        :return: dataframe after analysis
        """
        self.fetch_queries[self.weather_table_name] += f" AND max_temperature != -9999 "
        self.fetch_queries[self.weather_table_name] += f" AND min_temperature != -9999 "
        self.fetch_queries[self.weather_table_name] += f" AND precipitation_amount != -9999 "
        df = pd.read_sql(self.fetch_queries[self.weather_table_name], self.db.conn)
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['year'] = [int(x.year) for x in df['date']]
        analysis = pd.DataFrame(df.groupby(['year', 'station_id']).agg({'max_temperature': 'mean', 'min_temperature': 'mean', 'precipitation_amount': 'sum'}).reset_index())
        analysis['max_temperature'] = analysis['max_temperature']/10
        analysis['min_temperature'] = analysis['min_temperature'] / 10
        analysis['precipitation_amount'] = analysis['precipitation_amount'] / 100
        return analysis

    def insertStatsData(self):
        # Execute SQL query to create table for statistics data
        self.db.cursor.execute(self.create_table)
        # Analyze weather data to create statistics
        data = self.analyzeData()
        start_time = time.time()
        logging.info(f"Data ingestion process started at {start_time}")
        try:
            # Create an empty list to store non-duplicate rows
            res = []
            for idx, row in data.iterrows():
                # Execute SQL query to check for duplicates
                self.db.cursor.execute(self.duplicate_check, tuple(row))
                # If no duplicate is found, add the row to the list of non-duplicates
                if not self.db.cursor.fetchone()[0]:
                    res.append(tuple(row))
            # Count number of non-duplicates
            records = len(res)
            if len(res) > 0:
                # If there are non-duplicates, insert them into the table
                self.db.cursor.executemany(self.insert_query, res)
            else:
                records = 0
            # Commit changes to the database
            self.db.conn.commit()
            end_time = time.time()
            # Log end time and number of non-duplicates inserted into the table
            logging.info(f"Data ingestion process ended at {end_time}. Number of non duplicate records ingested into the table [weather_data_stats] : {records}")

        except Exception as e:
            # If an error occurs during data ingestion, log the error
            logging.info(f"Error {e} occurred at {start_time} while inserting data into [weather_data_stats]")





