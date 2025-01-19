import os
import argparse
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database connection parameters
db_config = {
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT'),
    'database': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD')
}

def create_database_and_table(csv_file_path, table_name):
    """
    Create the database and table if they don't exist.
    """
    try:
        # Connect to the specified database
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        # Read the CSV file to infer column names
        df = pd.read_csv(csv_file_path, nrows=100)  # Sample 100 rows for better inference

        # Generate the CREATE TABLE SQL statement
        columns_with_types = []
        for col in df.columns:
            # Use TEXT as the default column type to avoid data type mismatches
            columns_with_types.append(f"{col} TEXT")

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(columns_with_types)}
        );
        """

        # Execute the CREATE TABLE query
        cur.execute(create_table_query)
        conn.commit()
        print(f"Table {table_name} created or already exists.")

    except Exception as e:
        print(f"Table creation error: {e}")

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def load_csv_to_postgres(csv_file_path, table_name):
    """
    Load data from a CSV file into a PostgreSQL table.
    """
    try:
        # Read CSV file into a pandas DataFrame
        df = pd.read_csv(csv_file_path, low_memory=False)  # Suppress DtypeWarning

        # Convert DataFrame to a list of tuples
        data_tuples = [tuple(x) for x in df.to_numpy()]

        # Get column names
        columns = ','.join(df.columns)

        # SQL query to insert data
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES %s"

        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        # Execute the insert query
        execute_values(cur, insert_query, data_tuples)

        # Commit the transaction
        conn.commit()
        print(f"Data from {csv_file_path} inserted successfully into {table_name}!")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the cursor and connection
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Load CSV data into a PostgreSQL table.")
    parser.add_argument("csv_file_path", help="Path to the CSV file.")
    parser.add_argument("table_name", help="Name of the PostgreSQL table to insert data into.")

    # Parse arguments
    args = parser.parse_args()

    # Create the database and table if they don't exist
    create_database_and_table(args.csv_file_path, args.table_name)

    # Call the function to load CSV data into PostgreSQL
    load_csv_to_postgres(args.csv_file_path, args.table_name)
