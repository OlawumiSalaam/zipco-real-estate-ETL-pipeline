import json
import pandas as pd
from functions import (
    api_url, headers, querystring, fetch_data_from_api, save_data_to_file,
    impute_data, prepare_tables, save_tables_to_csv, create_tables,
    load_data_from_csv_to_table
)

def main():

    
    # Fetch data from API
    data = fetch_data_from_api(api_url, headers, querystring)

    # Save data to a file
    filename = 'data/PropertyRecords.json'
    save_data_to_file(data, filename)

    # Read into a DataFrame
    property_record = pd.read_json(filename)

    # Data imputation
    property_record = impute_data(property_record)

    # Prepare tables
    fact_table, location_dim, sales_dim, features_dim = prepare_tables(property_record)

    # Save tables to CSV
    filenames = ['data/property_fact_table.csv', 'data/location_dimension.csv', 'data/sales_dimension.csv', 'data/features_dimension.csv']
    save_tables_to_csv([fact_table, location_dim, sales_dim, features_dim], filenames)

    # Create tables in PostgreSQL
    create_tables()

    # Load tables into database
    load_data_from_csv_to_table('data/property_fact_table.csv', 'zapbank.fact_table')
    load_data_from_csv_to_table('data/location_dimension.csv', 'zapbank.location_dim')
    load_data_from_csv_to_table('data/features_dimension.csv', 'zapbank.features_dim')
    load_data_from_csv_to_table('data/sales_dimension.csv', 'zapbank.sales_dim')

    print("All the data has been loaded successfully into their respective schema and tables.")

if __name__ == "__main__":
    main()
