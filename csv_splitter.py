# USING AMAZON CODEWHISPERER: https://www.codewhisperer.com/
# I want to write a Lambda function for splitting a CSV file into chunks, which should be stored in other CSV files. Each file should be uploaded to an S3 bucket. Please use the Pandas library for the chunking.

import pandas as pd
import os
import boto3

def split_csv_async(file_name, chunk_size, bucket_name, s3_client):
    df = pd.read_csv(file_name)
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i+chunk_size]
        chunk.to_csv(f"{file_name[:-4]}_chunk{i//chunk_size}.csv", index=False)
        s3_client.upload_file(f"{file_name[:-4]}_chunk{i//chunk_size}.csv", bucket_name, f"{file_name[:-4]}_chunk{i//chunk_size}.csv")
        os.remove(f"{file_name[:-4]}_chunk{i//chunk_size}.csv")
        print(f"Chunk {i//chunk_size} uploaded to S3 bucket {bucket_name}")

# write the code to create an S3 client
s3_client = boto3.client('s3')

# write the code to call the above function
split_csv_async("sample_data/annual-enterprise-survey-2021-financial-year-provisional-csv.csv", 10000, "working-directory", s3_client)


