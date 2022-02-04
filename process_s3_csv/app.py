import io
import logging
import os
import traceback
import urllib.parse
import zipfile
from io import BytesIO
from pathlib import Path, PurePosixPath

import boto3
import numpy as np
import pandas as pd

"""
Lambda Function

"""

CHUNKSIZE = int(os.environ.get('CHUNKSIZE', '10000'))
NOTIFY_ERROR_ARN = os.environ.get("NOTIFY_ERROR_ARN", "")
UPLOAD_BUCKET_NAME = os.environ.get("UPLOAD_BUCKET_NAME", "")
UPLOAD_DATA_PREFIX = os.environ.get("UPLOAD_DATA_PREFIX", "")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
sns = boto3.client("sns")


def get_df_from_s3_csv(s3_client, bucket_name, key_path, delimiter=','):
    """
    Read a csv file from s3 bucket and return it as a dataframe
    """
    response = s3_client.get_object(Bucket=bucket_name, Key=key_path)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        logger.info(f"Successful s3.get_object({bucket_name}, {key_path})")
        df = pd.read_csv(response.get("Body"), delimiter, dtype=str)
        return df
    else:
        logger.warning(f"Unsuccessful S3.get_object({bucket_name}, {key_path})")
        raise Exception(f'Unsuccessful S3.get_object({bucket_name}, {key_path})')


def get_df_from_s3_xlxs(s3_client, bucket_name, key_path):
    """
    Read a csv file from s3 bucket and return it as a dataframe
    """
    response = s3_client.get_object(Bucket=bucket_name, Key=key_path)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        logger.info(f"Successful s3.get_object({bucket_name}, {key_path})")
        df = pd.read_excel(io.BytesIO(response.get("Body").read()), dtype=str)
        return df
    else:
        logger.warning(f"Unsuccessful S3.get_object({bucket_name}, {key_path})")
        raise Exception(f'Unsuccessful S3.get_object({bucket_name}, {key_path})')


def put_df_to_s3_csv(s3_client, bucket_name, key_path, df):
    """
    save a dataframe as csv file in a s3 bucket
    """
    with io.StringIO() as csv_buffer:
        df.to_csv(csv_buffer, index=False)

        response = s3_client.put_object(Bucket=bucket_name, Key=key_path, Body=csv_buffer.getvalue())
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            logger.info(f"Successful s3.put_object({bucket_name}, {key_path})")
        else:
            logger.warning(f"Unsuccessful s3.put_object({bucket_name}, {key_path})")
            raise Exception(f'Unsuccessful S3.put_object({bucket_name}, {key_path})')


def extract_from_s3_zip_file(s3_client, bucket_name, key_path, target_file_name):
    """
    Extract a target file from zip file in s3 bucket and return the file object
    """
    response = s3_client.get_object(Bucket=bucket_name, Key=key_path)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        logger.info(f"Successful s3.get_object({bucket_name}, {key_path})")
        buffer = BytesIO(response.get("Body").read())
        z = zipfile.ZipFile(buffer)

        # Process each file within the zip
        for filename in z.namelist():
            if filename == target_file_name:
                logger.info(f"Extracting file {filename}")
                return z.open(filename)
    else:
        logger.warning(f"Unsuccessful extract file from S3 ({bucket_name}, {key_path})")
        raise Exception(f'Unsuccessful extract file from S3 ({bucket_name}, {key_path})')


def clean_df(df):
    """Main operation to clean dataframe. Refer to file docs on the list of operations. """
    # TODO
    return df


def lambda_handler(event, context):
    """
    Clean csv data file from PRPP
    """
    logger.info(event)

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        # Read input from s3 bucket
        df = get_df_from_s3_xlxs(s3_client=s3, bucket_name=bucket, key_path=key)

        # Processing data
        cleaned_df = clean_df(df)

        # Split dataframe by max 10k rows
        groups = cleaned_df.groupby(np.arange(len(cleaned_df.index)) // CHUNKSIZE)
        for (frame_id, frame) in groups:
            logger.info(f'Frame: {frame_id}')
            # Output to s3 bucket
            key_path = Path(UPLOAD_DATA_PREFIX).joinpath(Path(key).stem + f'_{frame_id}.csv')
            key_str = str(PurePosixPath(key_path))
            put_df_to_s3_csv(s3_client=s3, bucket_name=UPLOAD_BUCKET_NAME, key_path=key_str, df=frame)

        # Delete file after processing
        s3.delete_object(Bucket=bucket, Key=key)

    except Exception as e:
        error_msg = (
            f"Error processing object: ({key}) in bucket ({bucket}): "
            f"{str(e)}: {traceback.format_exc()}"
        )
        sns.publish(TopicArn=NOTIFY_ERROR_ARN,
                    Subject="PRPP Contact Interim File Error",
                    Message=error_msg)
        logger.exception(e)


if __name__ == '__main__':
    # For Testing
    CHUNKSIZE = 100000
    NOTIFY_ERROR_ARN = 'arn:aws:sns:ap-southeast-1:825935993978:vision-sns-ihis-error'
    UPLOAD_BUCKET_NAME = "temp-zhangqinjie-904766938874"
    UPLOAD_DATA_PREFIX = "prpp_oupput/"
    event = {
        "Records": [
            {
                "eventVersion": "2.0",
                "eventSource": "aws:s3",
                "awsRegion": "{region}",
                "eventTime": "1970-01-01T00:00:00Z",
                "eventName": "ObjectCreated:Put",
                "userIdentity": {
                    "principalId": "EXAMPLE"
                },
                "requestParameters": {
                    "sourceIPAddress": "127.0.0.1"
                },
                "responseElements": {
                    "x-amz-request-id": "EXAMPLE123456789",
                    "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
                },
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "testConfigRule",
                    "bucket": {
                        "name": "temp-zhangqinjie-904766938874",
                        "ownerIdentity": {
                            "principalId": "EXAMPLE"
                        },
                        "arn": "arn:{partition}:s3:::temp-zhangqinjie-904766938874"
                    },
                    "object": {
                        "key": "PRPP/SwabOrdered_Daily_202110080600.xlsx",
                        "size": 1024,
                        "eTag": "0123456789abcdef0123456789abcdef",
                        "sequencer": "0A1B2C3D4E5F678901"
                    }
                }
            }
        ]
    }

    lambda_handler(event, None)
