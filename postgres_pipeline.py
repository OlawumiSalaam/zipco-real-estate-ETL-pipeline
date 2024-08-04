import requests
import json
import pandas as pd
import csv
import psycopg2
from psycopg2 import sql

# Define constants
API_URL = "https://realty-mole-property-api.p.rapidapi.com/randomProperties"
QUERYSTRING = {"limit": "100000"}
HEADERS = {
    "x-rapidapi-key": "3e7f4fa5ecmsh410c7d813c60e5cp1dbf74jsn09e7c2369927",
    "x-rapidapi-host": "realty-mole-property-api.p.rapidapi.com"
}

# Fetch data from API
response = requests.get(API_URL, headers=HEADERS, params=QUERYSTRING)
data = response.json()

# Save data to a file
filename = 'data/PropertyRecords.json'
with open(filename, 'w') as file:
    json.dump(data, file, indent=4)

# Read into a DataFrame
property_record = pd.read_json(filename)

# Data imputation
property_record.fillna({
    'ownerOccupied': 'Unknown',
    'legalDescription': 'Unknown',
    'subdivision': 'Unknown',
    'zoning': 'Unknown',
    'propertyType': 'Unknown',
    'lastSaleDate': 'Unknown',
    'taxAssessment': 'Unknown',
    'propertyTaxes': 'Unknown',
    'owner': 'Unknown',
    'addressLine2': 'Unknown',
    'features': 'Unknown'
}, inplace=True)

numerical_cols = ['bedrooms', 'bathrooms', 'squareFootage', 'lotSize']
for col in numerical_cols:
    property_record[col].fillna(property_record[col].median(), inplace=True)

property_record['lastSalePrice'].fillna(property_record['lastSalePrice'].mean(), inplace=True)
property_record['yearBuilt'].fillna(property_record['yearBuilt'].mode()[0], inplace=True)

# Convert 'features' column to JSON strings if necessary
def convert_to_json_string(value):
    if isinstance(value, dict):
        return json.dumps(value)
    return value

property_record['features'] = property_record['features'].apply(convert_to_json_string)

# Verify and adjust data types
property_record['lastSaleDate'] = pd.to_datetime(property_record['lastSaleDate'], errors='coerce')
placeholder_date = pd.Timestamp('1900-01-01')
property_record['lastSaleDate'].fillna(placeholder_date, inplace=True)

# Prepare tables
fact_columns = ['addressLine1', 'city', 'state', 'zipCode', 'formattedAddress',
                'squareFootage', 'yearBuilt', 'bathrooms', 'bedrooms', 'lotSize',
                'propertyType', 'longitude', 'latitude']
fact_table = property_record[fact_columns]

location_dim = property_record[['addressLine1', 'city', 'state', 'zipCode', 'county', 'longitude', 'latitude']].drop_duplicates().reset_index(drop=True)
location_dim.index.name = 'location_id'

sales_dim = property_record[['lastSalePrice', 'lastSaleDate']].drop_duplicates().reset_index(drop=True)
sales_dim.index.name = 'sales_id'

features_dim = property_record[['features', 'propertyType', 'zoning']].drop_duplicates().reset_index(drop=True)
features_dim.index.name = 'features_id'

fact_table.to_csv('data/property_fact_table.csv', index=False)
location_dim.to_csv('data/location_dimension.csv', index=False)
sales_dim.to_csv('data/sales_dimension.csv', index=False)
features_dim.to_csv('data/features_dimension.csv', index=False)

# Define function to connect to PostgreSQL
def get_db_connection():
    return psycopg2.connect(
        host='localhost',
        database='postgres',
        user='postgres',
        password='Abdazeez@0312'
    )

# Create tables in PostgreSQL
def create_tables():
    conn = get_db_connection()
    cursor = conn.cursor()
    create_table_query = '''
    CREATE SCHEMA IF NOT EXISTS zapbank;

    DROP TABLE IF EXISTS zapbank.fact_table;
    DROP TABLE IF EXISTS zapbank.location_dim;
    DROP TABLE IF EXISTS zapbank.sales_dim;
    DROP TABLE IF EXISTS zapbank.features_dim;

    CREATE TABLE zapbank.fact_table(
        addressLine1 VARCHAR(255),
        city VARCHAR(100),
        state VARCHAR(50),
        zipCode INTEGER,
        formattedAddress VARCHAR(255),
        squareFootage NUMERIC,
        yearBuilt NUMERIC,
        bathrooms NUMERIC,
        bedrooms NUMERIC,
        lotSize NUMERIC,
        propertyType VARCHAR(255),
        longitude NUMERIC,
        latitude NUMERIC
    );

    CREATE TABLE zapbank.location_dim(
        location_id SERIAL PRIMARY KEY,
        addressLine1 VARCHAR(255),
        city VARCHAR(100),
        state VARCHAR(50),
        zipCode INTEGER,
        county VARCHAR(100),
        longitude NUMERIC,
        latitude NUMERIC
    );

    CREATE TABLE zapbank.sales_dim(
        sales_id SERIAL PRIMARY KEY,
        lastSalePrice NUMERIC,
        lastSaleDate DATE
    );

    CREATE TABLE zapbank.features_dim(
        features_id SERIAL PRIMARY KEY,
        features JSONB,
        propertyType TEXT,
        zoning TEXT
    );
    '''
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()

create_tables()

# Load CSV data into PostgreSQL tables
def load_data_from_csv_to_table(csv_path, table_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    with open(csv_path, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        for row in reader:
            placeholders = ', '.join(['%s'] * len(row))
            query = f'INSERT INTO {table_name} VALUES ({placeholders});'
            cursor.execute(query, row)
    conn.commit()
    cursor.close()
    conn.close()

# Load tables into database
fact_csv_path = 'data/property_fact_table.csv'
load_data_from_csv_to_table(fact_csv_path, 'zapbank.fact_table')

location_csv_path = 'data/location_dimension.csv'
load_data_from_csv_to_table(location_csv_path, 'zapbank.location_dim')

features_csv_path = 'data/features_dimension.csv'
load_data_from_csv_to_table(features_csv_path, 'zapbank.features_dim')

sales_csv_path = 'data/sales_dimension.csv'
load_data_from_csv_to_table(sales_csv_path, 'zapbank.sales_dim')

print("All the data has been loaded successfully into their respective schema and tables.")
