# Import in-built libraries
import os
# Import argument parser library
import argparse
# Import python library for postgres sql
import psycopg2
# Import data manipulation libraries
import pandas as pd
import numpy as np
# Import python classes from python modules
from data_operations import InsertData
from analyze import Analysis


def main():
    # create an instance of argparse
    parser = argparse.ArgumentParser(description="User prompt for data analysis or insert data into sql tables")
    # define command-line arguments
    # Argument for analyzing the whole data and inserting into weather_data_stats table
    parser.add_argument('--analyse_insert', type=bool, required=False, help='get weather data and store the analysis in weather_data_stats table : Boolean')

    parser.add_argument('--insert_dir_data', type=bool, required=False, help='Inserts weather and yield directory data into Postgres SQL tables : Boolean')
    parser.add_argument('--tbl_name', type=str, required=False, help='pass the table name where the data will be stored')
    # parse command-line arguments
    args = parser.parse_args()  # Create an object to accept input parameters to command line scripts

    # call appropriate functions based on command-line arguments
    if args.analyse_insert:
        al = Analysis()
        al.insertStatsData()

    if args.insert_dir_data:
        if args.tbl_name:
            ins = InsertData()
            ins.insertData(args.tbl_name)


if __name__ == '__main__':
    main()
