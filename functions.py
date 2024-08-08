import requests
import os
import json
import pandas as pd
import csv
import psycopg2
from psycopg2 import sql

# Fetch environment variables
api_url = os.getenv('REALTY_MOLE_API_URL')
headers = {
    "x-rapidapi-key": os.getenv('REALTY_MOLE_API_KEY'),
    "x-rapidapi-host": os.getenv('REALTY_MOLE_API_HOST')
}
querystring = {"limit": "100000"}

# Fetch data from API
def fetch_data_from_api(api_url, headers, querystring):
    response = requests.get(api_url, headers=headers, params=querystring)
    response.raise_for_status()  # Ensure we raise an error for bad status codes
    print("Data fetched from API")
    return response.json()

# Save data to a file
def save_data_to_file(data, filename):
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"Data saved to file: {filename}")

# Convert 'features' column to JSON strings if necessary
def convert_to_json_string(value):
    if isinstance(value, dict):
        return json.dumps(value)
    return value

# Data imputation
def impute_data(df):
    df.fillna({
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
        df[col].fillna(df[col].median(), inplace=True)

    df['lastSalePrice'].fillna(df['lastSalePrice'].mean(), inplace=True)
    df['yearBuilt'].fillna(df['yearBuilt'].mode()[0], inplace=True)

    df['features'] = df['features'].apply(convert_to_json_string)

    df['lastSaleDate'] = pd.to_datetime(df['lastSaleDate'], errors='coerce')
    placeholder_date = pd.Timestamp('1900-01-01')
    df['lastSaleDate'].fillna(placeholder_date, inplace=True)
    print("Data imputation completed")
    return df

# Prepare tables
def prepare_tables(df):
    fact_columns = ['addressLine1', 'city', 'state', 'zipCode', 'formattedAddress',
                    'squareFootage', 'yearBuilt', 'bathrooms', 'bedrooms', 'lotSize',
                    'propertyType', 'longitude', 'latitude']
    fact_table = df[fact_columns]

    location_dim = df[['addressLine1', 'city', 'state', 'zipCode', 'county', 'longitude', 'latitude']].drop_duplicates().reset_index(drop=True)
    location_dim.index.name = 'location_id'

    sales_dim = df[['lastSalePrice', 'lastSaleDate']].drop_duplicates().reset_index(drop=True)
    sales_dim.index.name = 'sales_id'

    features_dim = df[['features', 'propertyType', 'zoning']].drop_duplicates().reset_index(drop=True)
    features_dim.index.name = 'features_id'

    print("Tables prepared: fact_table, location_dim, sales_dim, features_dim")
    return fact_table, location_dim, sales_dim, features_dim

# Save tables to CSV
def save_tables_to_csv(tables, filenames):
    for table, filename in zip(tables, filenames):
        table.to_csv(filename, index=False)
    print(f"Tables saved to CSV files: {', '.join(filenames)}")

# Connect to PostgreSQL
def get_db_connection():
    print("Connecting to PostgreSQL database")
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        database=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
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
    print("Tables created in PostgreSQL")

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
    print(f"Data loaded into table {table_name} from CSV file {csv_path}")
