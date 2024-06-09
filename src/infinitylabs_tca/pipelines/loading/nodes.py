import boto3
import json
import pandas as pd
from sqlalchemy import create_engine

def get_secret():
    secret_name = "your-secret-name"  # Replace with your secret name
    region_name = "your-region"  # Replace with your AWS region
    # Create a Secrets Manager client
    client = boto3.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        return None
    secret = get_secret_value_response['SecretString']
    return json.loads(secret)

def tca_engine():
    secrets = get_secret()
    if secrets:
        # Connection details
        DB_SERVER = secrets['DB_SERVER']
        DB_PORT = secrets['DB_PORT']
        DB_USER = secrets['DB_USER']
        DB_PASS = secrets['DB_PASS']
        DB_NAME = secrets['DB_NAME']
        DB_DRIVER = secrets['DB_DRIVER']

        # Create the connection string
        connection_string = (
            f"mssql+pyodbc://{DB_USER}:{DB_PASS}@{DB_SERVER}:{DB_PORT}/{DB_NAME}?driver={DB_DRIVER}"
        )

        # Create the SQLAlchemy engine
        engine = create_engine(connection_string)
        return engine
    else:
        print("Failed to retrieve secrets")
        return None


def get_table(engine, table_name):
    try:
        # Read data from a table (replace 'your_table_name' with your actual table name)
        df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
        return df
    except Exception as e:
        print(f"Error reading data: {e}")
        return None
    
def save_tables(table_names: list) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame):

    # Declare empty DataFrames for all tables
    dataframes = {
        "agencias": pd.DataFrame(),
        "canales": pd.DataFrame(),
        "empresas": pd.DataFrame(),
        "estatus_reservaciones": pd.DataFrame(),
        "paises": pd.DataFrame(),
        "paquetes": pd.DataFrame(),
        "reservaciones": pd.DataFrame(),
        "segmentos_comp": pd.DataFrame(),
        "tipos_habitaciones": pd.DataFrame()
    }

    engine = tca_engine()
    for table_name in table_names:
        df = get_table(engine, table_name)
        if df is None:
            print(f"Failed to save table {table_name}")
        else:
            dataframes[table_name] = df

        
    return dataframes['agencias'], dataframes['canales'], dataframes['empresas'], dataframes['estatus_reservaciones'], dataframes['paises'], dataframes['paquetes'], dataframes['reservaciones'], dataframes['segmentos_comp'], dataframes['tipos_habitaciones']


