import datetime
import logging

import psycopg2
import pandas as pd
import numpy as np
import os
import time
from src.db_conn import Database

logging.basicConfig(filename='../logs.log', level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


class DataOperations:
    def __init__(self):
        # Initialize instance variables with the paths to the weather and yield data directories
        self.weather_data_path = "../wx_data/"
        self.yield_data_path = '../yld_data/'

    def createYieldData(self):
        # Get a list of all the text files in the yield data directory
        all_files = os.listdir(self.yield_data_path)
        # Extract only the files that end with .txt
        txt_files = [f for f in all_files if f.endswith('.txt')]
        # Define the column names for the yield dataframe
        cols = ['year', 'amount']
        # Create an empty data frame with the above column names
        df = pd.DataFrame(columns=cols)
        # Loop through each text file in the directory
        for file in txt_files:
            # Read the file into a pandas data frame, using the above column names and tab delimiter
            temp = pd.read_csv(self.yield_data_path + file, names=cols, delimiter='\t')
            # Concatenate the data frame for this file with the previous data frames
            df = pd.concat([df, temp], ignore_index=True)
        # Return the final data frame containing all the yield data
        return df

    def createWeatherData(self):
        # Get a list of all the text files in the weather data directory
        all_files = os.listdir(self.weather_data_path)
        # extracting only the text files in the folder
        txt_files = [f for f in all_files if f.endswith('.txt')]
        # Define the column names for the weather dataframe
        cols = ['station_id', "date_column", "max_temperature", "min_temperature", "precipitation_amount"]
        df = pd.DataFrame(columns=cols)
        # Loop through each text file in the directory
        for file in txt_files:
            # Read the file into a pandas data frame, using the above column names and tab delimiter
            temp = pd.read_csv(self.weather_data_path + file, names=["date_column", "max_temperature", "min_temperature", "precipitation_amount"], delimiter='\t')
            # extract the station_id from the filenames station_id.txt using split
            temp['station_id'] = file.split('.')[0]
            df = pd.concat([df, temp], ignore_index=True)
        # Convert the date column to a pandas datetime object
        df['date_column'] = pd.to_datetime(df['date_column'], format='%Y%m%d')
        return df


class InsertData:
    def __init__(self):
        self.yield_table_name = 'yield_data'
        self.weather_table_name = 'weather_data'
        self.insert_queries = {
            self.weather_table_name: f"INSERT INTO {self.weather_table_name} (station_id, date, max_temperature, min_temperature, precipitation_amount) VALUES (%s, %s, %s, %s, %s)",
            self.yield_table_name: f""" INSERT INTO {self.yield_table_name} (year, value) VALUES (%s, %s)"""
        }
        self.duplicate_check = {
            self.weather_table_name: f"SELECT EXISTS(SELECT 1 FROM {self.weather_table_name} WHERE station_id=%s AND date=%s AND max_temperature=%s AND min_temperature=%s AND precipitation_amount=%s )",
            self.yield_table_name: f"SELECT EXISTS(SELECT 1 FROM {self.yield_table_name} WHERE year=%s AND value=%s)"
        }
        self.dataop = DataOperations()
        self.db = Database()
        # get the table names that has been created in the database
        self.db.cursor.execute("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname='public';")
        self.tables = [each[0] for each in self.db.cursor.fetchall()]
        # check if both tables are there for both types of data if not create them
        if self.yield_table_name or self.weather_table_name not in self.tables:
            self.db.createTable()

    def insertData(self, table_name):
        """
        Method to insert data into the specified table.
        :param table_name: parameter table name where the data need to be stored
        :return: None
        Insert data from directory to database tables based on table name input
        """
        assert table_name == self.weather_table_name or table_name == self.yield_table_name, "Please enter a valid table name"
        start_time = time.time()
        try:
            if table_name == self.weather_table_name:
                data = self.dataop.createWeatherData()
            if table_name == self.yield_table_name:
                data = self.dataop.createYieldData()
            insert_query = self.insert_queries[table_name]
            logging.info(f"Data ingestion process started at {start_time}")
            res = []
            for idx, row in data.iterrows():
                self.db.cursor.execute(self.duplicate_check[table_name], tuple(row))
                if not self.db.cursor.fetchone()[0]:
                    res.append(tuple(row))
            records = len(res)
            if len(res) > 0:
                self.db.cursor.executemany(insert_query, res)
            else:
                records = 0
            self.db.conn.commit()
            end_time = time.time()
            logging.info(f"Data ingestion process ended at {end_time}. Number of non duplicate records ingested into the table [{table_name}] : {records}")
        except Exception as e:
            logging.info(f"Error {e} occurred at {start_time} while inserting data into {table_name}")


class FetchData:
    def __init__(self, start_date, end_date, station_id, page_size=20, page_number=1):
        """
        Initialize the FetchData class with the given parameters.
        :param start_date: The start date of the weather data to fetch in the format 'YYYY-MM-DD'.
        :param end_date: The end date of the weather data to fetch in the format 'YYYY-MM-DD'.
        :param station_id: The ID of the weather station to fetch data for.
        :param page_size: The number of rows to return per page for pagination. Defaults to 20.
        :param page_number: The current page number for pagination. The first page has a default page_number of 1.
        """
        self.db = Database()
        self.weather_table_name = 'weather_data'
        self.page_size = int(page_size)
        self.page_num = int(page_number)
        self.fetch_queries = {
            self.weather_table_name: f"SELECT * FROM {self.weather_table_name} WHERE 1=1",
        }
        if start_date and start_date != 'None' or None:
            self.fetch_queries[self.weather_table_name] += f" AND date >= '{start_date}'"
        if end_date and end_date != 'None' or None:
            self.fetch_queries[self.weather_table_name] += f" AND date <= '{end_date}'"
        if station_id and station_id != 'None' or None:
            self.fetch_queries[self.weather_table_name] += f" AND station_id = '{station_id}'"

    def fetchData(self):
        """
        Fetch weather data from the database based on the user input and return it as a Pandas dataframe.
        :return: return dataframe of records based on the user input
        """
        # page_size is the number of rows to return per page, and page_number is the current page number.
        # The first page has a page_number of 1, and the OFFSET is calculated as (page_number - 1) * page_size.
        self.fetch_queries[self.weather_table_name] += f" LIMIT {self.page_size} OFFSET {(self.page_num-1) * self.page_size}"
        df = pd.read_sql(self.fetch_queries[self.weather_table_name], self.db.conn)
        return df

    def fetchDataStats(self):
        """
        Fetch weather data from the database based on the user input, remove records with missing values which is -9999,
        and return the result as a Pandas dataframe.
        :return: return dataframe of records without missing values based on the user input
        """
        self.fetch_queries[self.weather_table_name] += f" AND max_temperature <> -9999 "
        self.fetch_queries[self.weather_table_name] += f" AND min_temperature <> -9999 "
        self.fetch_queries[self.weather_table_name] += f" AND precipitation_amount <> -9999 "
        # page_size is the number of rows to return per page, and page_number is the current page number.
        # The first page has a page_number of 1, and the OFFSET is calculated as (page_number - 1) * page_size
        self.fetch_queries[self.weather_table_name] += f" LIMIT {self.page_size} OFFSET {((self.page_num - 1) * self.page_size)}"
        df = pd.read_sql(self.fetch_queries[self.weather_table_name], self.db.conn)
        return df




