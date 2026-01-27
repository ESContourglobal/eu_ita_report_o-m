import pyodbc
from config import setup_logger, memory
import pandas as pd
from datetime import datetime, date
from sqlalchemy import create_engine
import sqlite3
import numpy as np

logger = setup_logger()


sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat())
sqlite3.register_converter("DATE", lambda s: datetime.strptime(s.decode("utf-8"), "%Y-%m-%d").date())
sqlite3.register_converter("DATETIME", lambda s: datetime.strptime(s.decode("utf-8"), "%Y-%m-%d %H:%M:%S"))

# Database connection settings
DB_CONFIG = {
    'server': 'cgusazadfsql.database.windows.net',
    'database': 'DM',
    'username': 'adfsql',
    'password': '{xRTv#<;v776PcaD6}',
    'driver': '{ODBC Driver 18 for SQL Server}'
}


def get_connection():
    """
    Establish and return a connection and cursor to the database.
    """
    conn_str = (
        f"DRIVER={DB_CONFIG['driver']};"
        f"SERVER={DB_CONFIG['server']},1433;"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
        f"Authentication=SqlPassword;"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    return conn, cursor

def read_from_db(query):
#     read query from db
    conn, cursor = get_connection()
    df = pd.read_sql(query, conn)
    cursor.close()
    conn.close()
    return df

def wrap_column(col):
    """
    Replace spaces, slashes, or dashes in column names with underscores.
    """
    return col.replace(" ", "_").replace("/", "_").replace("-", "_").replace("(", "").replace(")", "")

def get_monthly_measures(month, year):
    conn,cursor = get_connection()
    query = f"SELECT * FROM risk.vw_italy_plants_measures WHERE month = {month} AND year = {year}"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def create_table(cursor, table_name, sql_columns, df, staging=False):
    dtype_mapping = {
        'int64': 'INT',
        'int32': 'INT',
        'float64': 'FLOAT',
        'object': 'VARCHAR(255)',  # Default VARCHAR for string-like objects
        'datetime64[ns]': 'DATETIME',
        'datetime64': 'DATETIME',
        'datetime64[ns, CET]': 'DATETIMEOFFSET(7)',
    }

    # Generate column definitions
    columns = []
    for col, dtype in df.dtypes.items():
        sql_type = dtype_mapping.get(str(dtype), 'VARCHAR(255)')

        column_definition = f"[{col}] {sql_type} NULL"  # Allow NULL by default
        columns.append(column_definition)

    # Combine column definitions into a CREATE TABLE statement
    columns_definition = ',\n'.join(columns)

    if staging == True:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name};")
        cursor.execute(f"CREATE TABLE {table_name} ({columns_definition});")
    else:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NULL CREATE TABLE {table_name} ({columns_definition});")

def bulk_insert_to_staging(df, conn, staging_table_name):
    """
    Bulk insert a DataFrame into a staging table, ensuring compatibility for SQL Server DATE columns.
    """
    data = [tuple(row) for row in df.to_numpy()]  # Convert to list of tuples
    placeholders = ', '.join(['?' for _ in df.columns])
    insert_query = f"INSERT INTO {staging_table_name} ({', '.join(df.columns)}) VALUES ({placeholders})"

    cursor = conn.cursor()
    cursor.fast_executemany = True  # Optimize for large inserts
    try:
        cursor.executemany(insert_query, data)
        conn.commit()
    except pyodbc.Error as e:
        logger.error(f"Bulk insert failed: {e}")
        for i, row in enumerate(data):
            try:
                cursor.execute(insert_query, row)
            except pyodbc.Error as row_error:
                logger.error(f"Error with row {i}: {row} -> {row_error}")
                break  # Exit on the first problematic row
        raise



def merge_from_staging(cursor, staging_table_name, target_table_name, primary_keys, sql_columns):
    """
    Perform a single MERGE query to upsert data from the staging table into the target table.
    """
    # Construct ON condition for the primary keys
    on_condition = ' AND '.join([f"target.{key} = staging.{key}" for key in primary_keys])

    # Construct the UPDATE SET clause
    update_columns = [col for col in sql_columns if col not in primary_keys]
    update_clause = ', '.join([f"target.{col} = staging.{col}" for col in update_columns])

    # Construct the INSERT clause
    insert_columns = ', '.join(sql_columns)
    insert_values = ', '.join([f"staging.{col}" for col in sql_columns])

    # Build the MERGE query
    merge_query = f'''
    MERGE INTO {target_table_name} AS target
    USING {staging_table_name} AS staging
    ON {on_condition}
    WHEN MATCHED THEN
        UPDATE SET {update_clause}
    WHEN NOT MATCHED THEN
        INSERT ({insert_columns})
        VALUES ({insert_values});
    '''

    # Execute the MERGE query
    cursor.execute(merge_query)


def create_staging_table(cursor, staging_table_name, df):
    """
    Dynamically create a staging table based on the DataFrame schema.
    """
    # Default type mapping based on pandas dtypes
    dtype_mapping = {
        'int64': 'INT',
        'int32': 'INT',
        'float64': 'FLOAT',
        'object': 'VARCHAR(255)',  # Default VARCHAR for string-like objects
        'datetime64[ns]': 'DATETIME',
        'datetime64': 'DATETIME',
        'datetime64[ns, CET]': 'DATETIMEOFFSET(7)'
    }

    # Generate column definitions
    columns = []
    for col, dtype in df.dtypes.items():
        sql_type = dtype_mapping.get(str(dtype), 'VARCHAR(255)')

        column_definition = f"[{col}] {sql_type} NULL"  # Allow NULL by default
        columns.append(column_definition)

    # Combine column definitions into a CREATE TABLE statement
    columns_sql = ',\n'.join(columns)
    create_table_query = f'''
    IF OBJECT_ID('{staging_table_name}', 'U') IS NOT NULL DROP TABLE {staging_table_name};
    CREATE TABLE {staging_table_name} (
        {columns_sql}
    );
    '''

    # Execute the query to create the table
    cursor.execute(create_table_query)

def write_to_db(df, table_name, primary_keys):

    """
    Write the DataFrame to the risk db table in Azure SQL Database dynamically using bulk insert.
    """
    # Establish connection
    conn, cursor = get_connection()

    # Get all column names dynamically
    sql_columns = [wrap_column(col) for col in df.columns.tolist()]
    df.columns = sql_columns  # Update column names to match SQL
    staging_table_name = f"risk.{table_name}_staging"
    target_table_name = f"risk.{table_name}"
    df = df.replace({pd.NA: None})
    df = df.replace({np.nan: None})

    try:
        create_table(cursor, staging_table_name, sql_columns, df, staging=True)
        create_table(cursor, target_table_name, sql_columns, df, staging=False)
        # Bulk insert data into the staging table
        placeholders = ', '.join(['?' for _ in sql_columns])
        bulk_insert_query = f"INSERT INTO {staging_table_name} ({', '.join(sql_columns)}) VALUES ({placeholders})"
        cursor.fast_executemany = True
        cursor.executemany(bulk_insert_query, df.to_numpy().tolist())
        conn.commit()

        # Perform the `MERGE` query for upserts
        merge_query = f'''
        MERGE INTO {target_table_name} AS target
        USING {staging_table_name} AS source
        ON ({" AND ".join([f"target.{key} = source.{key}" for key in primary_keys])})
        WHEN MATCHED THEN
            UPDATE SET {", ".join([f"target.{col} = source.{col}" for col in sql_columns if col not in primary_keys])}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(sql_columns)})
            VALUES ({', '.join([f"source.{col}" for col in sql_columns])});
        '''
        cursor.execute(merge_query)
        conn.commit()

        logger.info(f"Data successfully merged into {target_table_name}")

    except Exception as e:
        conn.rollback()
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        cursor.close()
        conn.close()