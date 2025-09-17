
import os
import json

import dotenv
import boto3


class S3Uploader:

    def __init__(self,name:str, s3_client:boto3.client,  bucket_name:str):
        self.name = name
        self.s3_client = s3_client
        self.bucket_name = bucket_name

    
    def upload_file(self, file_path:str, s3_path:str):
        
        try:
            self.s3_client.upload_file(file_path, self.bucket_name, s3_path)
            print(f"File {file_path} uploaded to {self.bucket_name}/{s3_path}")
        
        except Exception as e:
            print(f"Failed to upload {file_path} to {self.bucket_name}/{s3_path}: {e}")


    def run(self):
        for file in os.listdir(f"data/{self.name}"):
            if file.endswith(".parquet"):
                self.upload_file(f"data/{self.name}/{file}", f"{self.name}/{file}")


def execute():
    dotenv.load_dotenv('.env')

    with open('config.json', 'r') as f:
        configs = json.load(f)

    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')
    )
    
    for config in configs.values():
        s3_uploader = S3Uploader(config['name'], s3_client, os.getenv('S3_BUCKET_NAME'))
        s3_uploader.run()


if __name__ == "__main__":
    execute()