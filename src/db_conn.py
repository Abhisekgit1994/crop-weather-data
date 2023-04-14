import psycopg2
import pandas as pd
import numpy as np
import configparser

config = configparser.ConfigParser()
config.read('src/CONFIG.ini')

db_host = config['database']['host']
db_port = config['database']['port']
db_name = config['database']['database']
db_user = config['database']['user']
db_password = config['database']['password']


class Database:
    def __init__(self):
        # Initializes a connection to the PostgreSQL database server.
        self.conn = psycopg2.connect(
            host=db_host,  # hostname where the database server is running
            port=db_port,  # port number on which the database server is listening
            dbname=db_name,  # name of the database to which the connection is made
            user=db_user,  # username used to authenticate
            password=db_password  # password used to authenticate
        )
        # Initializes a cursor object to execute database queries.
        self.cursor = self.conn.cursor()

    def createTable(self):
        # Reads SQL queries from a file and executes them to create a new table in the database.
        with open("create table.sql", 'r') as file:
            for line in file.readlines():
                query = line.strip()
                self.cursor.execute(query)
                self.conn.commit()

    def alterTable(self):
        with open("alter_query.sql", 'r') as file:
            # Reads SQL queries from a file and executes them to modify an existing table in the database.
            for line in file.readlines():
                query = line.strip()
                self.cursor.execute(query)
                self.conn.commit()







