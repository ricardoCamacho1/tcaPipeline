import s3fs
import pandas as pd
import pickle
import boto3

def save_data_to_s3(reservaciones_dashboard: pd.DataFrame, features_dashboard: pd.DataFrame, features_model: pd.DataFrame, model_data) -> str:
    
    # Define the S3 bucket and file path
    bucket_name = 'tcadata'

    # Save the DataFrames to Parquet files on S3
    reservaciones_dashboard.to_parquet(f's3://{bucket_name}/reservaciones_dashboard.parquet', engine='pyarrow', index=False, storage_options={'anon': False})
    features_model.to_parquet(f's3://{bucket_name}/features_model.parquet', engine='pyarrow', index=False, storage_options={'anon': False})
    features_dashboard.to_parquet(f's3://{bucket_name}/features_dashboard.parquet', engine='pyarrow', index=False, storage_options={'anon': False})

    # Initialize the boto3 S3 resource
    s3 = boto3.resource('s3')

    # Serialize and upload the model to S3 as a pickle file
    model_data = pickle.dumps(model_data)
    s3.Object(bucket_name, 'model_data.pkl').put(Body=model_data)

    return f'DataFrame and model successfully saved to s3://{bucket_name}'

