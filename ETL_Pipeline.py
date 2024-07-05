# -------------------------------
# Main: Data Extraction and Loading
# -------------------------------

# Introduction
# In the realm of data engineering and analytics, efficient and accurate data movement between different
# database systems is crucial. This process, often referred to as Extract, Transform, Load (ETL), involves
# extracting data from a source system, transforming it as needed, and loading it into a target system. This
# enables organizations to consolidate data from various sources into a unified database for analysis and
# reporting.

# Goal
# The goal of this project is to automate the extraction of data from Microsoft SQL Server (using the
# AdventureWorksDW2019 database) and load it into PostgreSQL (using the AdventureWorks database). The
# AdventureWorks databases are commonly used sample databases provided by Microsoft, designed to showcase
# various SQL Server features and functionalities.
""" 
Project Overview
Data Sources:

Source Database: Microsoft SQL Server hosting the AdventureWorksDW2019 database. This database contains dimensional and fact tables typically used for data warehousing and analytics.
Target Database:

Destination Database: PostgreSQL database named AdventureWorks. This will serve as the target database where data from AdventureWorksDW2019 will be loaded.
ETL Process:

Extract: Connect to the SQL Server database (AdventureWorksDW2019), extract data from specified tables.
Transform: Perform any necessary transformations on the data (if applicable). In this project, transformations like data type conversions are minimal since we're focusing on loading.
Load: Load the transformed data into corresponding tables in the PostgreSQL database (AdventureWorks).
Implementation Steps:

Establish connections to both SQL Server and PostgreSQL using SQLAlchemy.
Define SQLAlchemy metadata and table schemas for the etl_metadata table in PostgreSQL.
Implement functions for data extraction (extract()), data loading (load()), and metadata management (update_metadata()).
Automate the ETL process for multiple tables (DimProduct, DimProductSubcategory, DimProductCategory, DimSalesTerritory, FactInternetSales) using Python.
Additional Features:

Upsert: Implement upsert (insert or update) functionality for managing metadata (etl_metadata) in PostgreSQL.
Error Handling: Incorporate error handling mechanisms to manage exceptions during data extraction and loading.
Logging: Integrate logging to capture informational and error messages during the ETL process for easier troubleshooting. 
 """
# Import necessary libraries
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
import pyodbc
import os

# Set environment variables (replace with your actual credentials)
# Define SQL Server and PostgreSQL connection details
# Replace with your actual credentials and connection strings
os.environ['PGPASS'] = 'demopass'
os.environ['PGUID'] = 'etl'

# Retrieve environment variables
pwd = os.environ['PGPASS']
uid = os.environ['PGUID']

# SQL Server details
driver = "ODBC Driver 17 for SQL Server"
server = "localhost"
database = "AdventureWorksDW2019"

# PostgreSQL details
pg_server = "localhost"
pg_database = "AdventureWorks"
pg_engine = create_engine(f'postgresql://{uid}:{pwd}@{pg_server}:5432/{pg_database}')

# Define metadata and table schema for etl_metadata table
metadata = MetaData()
etl_metadata = Table(
    'etl_metadata', metadata,
    Column('table_name', String, primary_key=True),
    Column('load_timestamp', DateTime),
    Column('rows_imported', Integer)
)

def extract(table_name):
    src_engine = create_engine(f'mssql+pyodbc://{uid}:{pwd}@{server}/{database}?driver={driver}')
    try:
        # Use SQLAlchemy engine to execute query and retrieve data
        query = f'SELECT * FROM {table_name}'
        df = pd.read_sql_query(query, src_engine)
        return df
    except Exception as e:
        print(f"Data extract error for {table_name}: {str(e)}")
        return pd.DataFrame()
    finally:
        src_engine.dispose()  # Close SQLAlchemy engine after use


def load(df, table_name):
    try:
        # Insert or update metadata in PostgreSQL using SQLAlchemy
        with pg_engine.connect() as conn:
            # Construct insert statement for etl_metadata table
            stmt = insert(etl_metadata).values(
                table_name=table_name,
                load_timestamp=pd.Timestamp.now(),
                rows_imported=len(df)
            )
            # On conflict, update the existing rows_imported and load_timestamp
            stmt = stmt.on_conflict_do_update(
                constraint='etl_metadata_pkey',  # Assuming 'etl_metadata_pkey' is the primary key constraint name
                set_=dict(
                    load_timestamp=pd.Timestamp.now(),
                    rows_imported=len(df)
                )
            )
            # Execute the statement
            conn.execute(stmt)

        # Insert data into PostgreSQL using SQLAlchemy engine
        df.to_sql(f'stg_{table_name}', pg_engine, if_exists='replace', index=False)
        print(f"Data imported successfully for {table_name}")
    except Exception as e:
        print(f"Data load error for {table_name}: {str(e)}")


def main():
    tables_to_process = ['DimProduct', 'DimProductSubcategory', 'DimProductCategory', 'DimSalesTerritory', 'FactInternetSales']
    
    for table_name in tables_to_process:
        df = extract(table_name)
        if not df.empty:
            load(df, table_name)

# Call the main function directly
main()
