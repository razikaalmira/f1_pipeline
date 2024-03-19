import boto3
import pathlib

# script_path = pathlib.Path(__file__).parent.resolve()
# print(script_path)


# s3 = boto3.client('s3')
# s3.upload_file('','','')

BUCKET_NAME = "formula1-project-bucket"

s3 = boto3.client("s3")