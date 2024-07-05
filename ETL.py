
#Introduction
In the realm of data engineering and analytics, efficient and accurate data movement between different database systems is crucial. 
This process, often referred to as Extract, Transform, Load (ETL), involves extracting data from a source system, transforming it as needed, and loading it into a target system. 
This enables organizations to consolidate data from various sources into a unified database for analysis and reporting.


from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
import pyodbc
import os

# Set environment variables (replace with your actual credentials)
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
