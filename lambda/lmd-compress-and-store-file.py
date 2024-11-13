from dmlib import general_utils, boto3_utils
import boto3
import os
from pathlib import Path

LAMBDA_NAME = "LAMBDA_NAME"

def lambda_handler(event, context):
    global LAMBDA_NAME
    LAMBDA_NAME = os.environ["LAMBDA_NAME"]

    print(general_utils.LAMBDA_STRINGS.LAMBDA_START_MSG.format(LAMBDA_NAME))

    # event parameters init
    source_bucket_name = event["bucket_name"]
    key_name = event["key_name"]
    file_name = event["file_name"]
    table_name = event["table_name"]
    archive_data_file_once_done = event["archive_data_file_once_done"]

    # env init
    bucket_name_completed = os.environ["bucket_name_completed"]

    if archive_data_file_once_done == "False":
        print(f"File {file_name} will not be archived.")

        print(general_utils.LAMBDA_STRINGS.LAMBDA_SUCCESS_MSG.format(LAMBDA_NAME))
        return general_utils.LAMBDA_STRINGS.LAMBDA_SUCCESS_MSG.format(LAMBDA_NAME)

    s3 = boto3.client("s3")

    try:
        print("Beginning streaming file compression and upload.")
        boto3_utils.S3_ZIP(show_progress=True).zip(
            source_bucket_name=source_bucket_name,
            source_key_name=key_name,
            dest_bucket_name=bucket_name_completed,
            dest_key_name=f"""{table_name}/{f"{Path(file_name).stem}.zip"}"""
        )
        print(f"Compressed file uploaded to {bucket_name_completed}.")

        s3.delete_object(
            Bucket=source_bucket_name,
            Key=key_name
        )
        print(f"File deleted from {source_bucket_name}.")
    except Exception as e:
        print(f"[ERROR] {e}")
        
        print(general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME))
        raise Exception(general_utils.format_lambda_exception(
            lambda_name=LAMBDA_NAME,
            error_type=type(e).__name__,
            exception_cause=e.args[0]
        ))

    print(general_utils.LAMBDA_STRINGS.LAMBDA_SUCCESS_MSG.format(LAMBDA_NAME))
    return general_utils.LAMBDA_STRINGS.LAMBDA_SUCCESS_MSG.format(LAMBDA_NAME)
