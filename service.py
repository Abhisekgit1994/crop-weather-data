import json

from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
from flask_swagger import swagger
import flask_swagger_ui
import psycopg2
from src.data_operations import FetchData
from src.db_conn import Database
import warnings
warnings.filterwarnings('ignore')

app = Flask(__name__)
CORS(app)  # cross origin request support

Swagger_URL = '/swagger'
api_URL = '/swagger.json'

swagger_blueprint = flask_swagger_ui.get_swaggerui_blueprint(
    Swagger_URL,
    api_URL,
    config={
        'app_name': 'Crop and Weather Data API'
    }
)

app.register_blueprint(swagger_blueprint, url_prefix=Swagger_URL)


@app.route('/swagger.json')
def swagger_json():
    with open('swagger.json', 'r') as f:
        swagger_json = json.load(f)
    return jsonify(swagger_json)


@app.route("/api/weather", methods=['GET'])
def get_weather_data():
    # Set default page size and page number
    pageSize = 20  # LIMIT
    pageNumber = 1  # OFFSET
    args = request.args.to_dict()
    # we can have the params or not, if there is no params it should return all the results
    # Get request parameters
    start_date = args.get('start_date')
    end_date = args.get('end_date')
    station_id = args.get('station_id')
    # Override default page size and page number if provided in request parameters
    if 'page_size' in args:
        pageSize = args.get('page_size')

    if 'page_number' in args:
        pageNumber = args.get('page_number')
    # Fetch data from external module using provided request parameters
    fd = FetchData(start_date, end_date, station_id, pageSize, pageNumber)
    data = fd.fetchData()
    # Convert fetched data to JSON format
    data['date'] = pd.to_datetime(data['date'], errors='coerce')
    data['date'] = data['date'].dt.strftime('%Y-%m-%d')
    res = data.to_json(orient='records')
    # Return response with success status and fetched data in JSON format
    if not res:
        resp = {'status': 'success', 'message': '0 records found'}
        return json.dumps(resp)

    return res


@app.route('/api/weather/stats', methods=['GET'])
def get_weather_data_stats():
    """
    :return: json result of the statistics on data extracted per user parameters
    """
    # default page size  and page number for pagination
    pageSize = 20  # LIMIT
    pageNum = 1  # OFFSET
    args = request.args.to_dict()  # get query parameters as a dictionary
    # we can have the params or not if there is no params it should return all the results
    start_date = args.get('start_date')  # get the 'start_date' query parameter, returns None if not found
    end_date = args.get('end_date')  # get the 'end_date' query parameter, returns None if not found
    station_id = args.get('station_id')  # get the 'station_id' query parameter, returns None if not found

    if 'page_size' in args:  # if 'page_size' query parameter is present, overwrite the default value
        pageSize = args.get('page_size')

    if 'page_number' in args:  # if 'page_number' query parameter is present, overwrite the default value
        pageNum = args.get('page_number')

    fd = FetchData(start_date, end_date, station_id, pageSize, pageNum)
    data = fd.fetchDataStats()
    # convert the 'date' column to a datetime format, setting invalid dates to NaT
    data['date'] = pd.to_datetime(data['date'], errors='coerce')
    data['year'] = [x.year for x in data['date']]
    analysis = pd.DataFrame(data.groupby(['year', 'station_id']).agg({'max_temperature': 'mean', 'min_temperature': 'mean', 'precipitation_amount': 'sum'}).reset_index())
    analysis = analysis.rename(columns={'max_temperature':'avg_max_temperature', 'min_temperature':'avg_min_temperature', 'precipitation_amount': 'total_precipitation_amount'})
    # convert 'max_temperature' from tenths of degrees Celsius to degrees Celsius
    analysis['avg_max_temperature'] = analysis['avg_max_temperature'] / 10
    analysis['avg_min_temperature'] = analysis['avg_min_temperature'] / 10
    # convert 'precipitation_amount' from tenths of millimeters to centimeters
    analysis['total_precipitation_amount'] = analysis['total_precipitation_amount'] / 100
    # convert the analysis DataFrame to JSON format with records orientation
    res = analysis.to_json(orient='records')

    if not res:
        resp = {'status': 'success', 'message': '0 records found'}
        return json.dumps(resp)
    # return the JSON response
    return jsonify(res)

