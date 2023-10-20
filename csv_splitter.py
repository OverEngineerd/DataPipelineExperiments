import pandas as pd
import os
import boto3
import json

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    for record in event['Records']:
        #pull the body out & json load it
        json_record = (record["body"])
        json_record = json.loads(json_record)
        object_key = json_record['Records'][0]['s3']['object']['key']
        upload_bucket = json_record['Records'][0]['s3']['bucket']['name']
        chunk_size = 10000
        
        # code to load the working_bucket environment varible
        working_bucket = os.environ['working_bucket']
        
        # given the bucket name and object name, get the object from s3
        file_name = f"/tmp/{object_key}"
        s3_client.download_file(upload_bucket, object_key, file_name)
    
        df = pd.read_csv(f"{file_name}")
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i+chunk_size]
            chunk.to_csv(f"{file_name[:-4]}_chunk{i//chunk_size}.csv", index=False)
            s3_client.upload_file(f"{file_name[:-4]}_chunk{i//chunk_size}.csv", working_bucket, f"{object_key[:-4]}/{file_name[5:-4]}_chunk_{i//chunk_size}.csv")
            os.remove(f"{file_name[:-4]}_chunk{i//chunk_size}.csv")
            print(f"Chunk {i//chunk_size} uploaded to S3 bucket {working_bucket}")
            
    return {
        'statusCode': 200,
        'body': json.dumps(f"File(s) have been split and uploaded to {working_bucket}.")
    }


