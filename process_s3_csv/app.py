import boto3
import urllib.parse
import pandas as pd
import io

s3 = boto3.client('s3')


def get_df_from_s3_csv(s3_client, bucket_name, key_path):
    response = s3_client.get_object(Bucket=bucket_name, Key=key_path)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(f"Successful s3.get_object({bucket_name}, {key_path})")
        df = pd.read_csv(response.get("Body"))
        return df
    else:
        print(f"Unsuccessful S3.get_object({bucket_name}, {key_path})")
        return None


def put_df_to_s3_csv(s3_client, bucket_name, key_path, df):
    with io.StringIO() as csv_buffer:
        df.to_csv(csv_buffer, index=False)

        response = s3_client.put_object(Bucket=bucket_name, Key=key_path, Body=csv_buffer.getvalue())
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            print(f"Successful s3.put_object({bucket_name}, {key_path})")
        else:
            print(f"Unsuccessful S3.put_object({bucket_name}, {key_path})")
            raise Exception(f'Unsuccessful S3.put_object({bucket_name}, {key_path})')


def lambda_handler(event, context):
    print(event)

    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        # Read input from s3 bucket
        df = get_df_from_s3_csv(s3_client=s3, bucket_name=bucket, key_path=key)

        # Processing data
        # TODO

        # Output to s3 bucket
        key_output = 'output.csv'
        put_df_to_s3_csv(s3_client=s3, bucket_name=bucket, key_path=key_output, df=df)
    except Exception as e:
        print(e)
        raise e
