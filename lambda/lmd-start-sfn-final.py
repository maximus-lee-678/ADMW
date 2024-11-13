from dmlib import boto3_utils, general_utils, pymysql_utils
import boto3
import s3fs
from pathlib import Path
import os
import re
import json

LAMBDA_NAME = "LAMBDA_NAME"

# starts with a alphabetical string that can contain underscores, followed by exactly one '~',
# followed by digits, followed by exactly one period, followed by digits, followed by exactly one period, and ends with any alphanumeric characters
REGEX_VALID_DATA_FILE = r"^[a-zA-Z_]+~\d+\.\d+\.[a-zA-Z0-9]*$"


def lambda_handler(event, context):
    global LAMBDA_NAME
    LAMBDA_NAME = os.environ["LAMBDA_NAME"]

    print(general_utils.LAMBDA_STRINGS.LAMBDA_START_MSG.format(LAMBDA_NAME))

    # event parameters init
    file_name = event["file_name"]
    target_table_to_load = event["target_table_to_load"]
    archive_data_file_once_done = event["archive_data_file_once_done"]

    # env init
    bucket_name_config = os.environ["bucket_name_config"]
    bucket_name_processing = os.environ["bucket_name_processing"]
    object_path_cfg = os.environ["object_path_cfg"]
    step_function_name_final = os.environ["step_function_name_final"]

    if not re.match(REGEX_VALID_DATA_FILE, file_name):
        print("Invalid file name specified.")

        print(general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME))
        return general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME)

    file_metadata = general_utils.decompose_file_name(file_name)

    dict_cfg = ""
    with s3fs.S3FileSystem().open(f"""s3://{bucket_name_config}/{object_path_cfg}""", "r") as file:
        dict_cfg = file.read()

    dict_cfg = json.loads(dict_cfg)

    executable_tables = dict_cfg["table_executes_which_flow"][file_metadata["table_name"]]

    if target_table_to_load not in executable_tables:
        print(f"Table {target_table_to_load} is not configured to be loaded with this file!")

        print(general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME))
        return general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME)

    matched_files = boto3_utils.find_objects(
        bucket_name=bucket_name_processing,
        prefix=f"""{file_metadata["table_name"]}/{file_name}"""
    )
    if len(matched_files) != 1:
        print("File was not found in processing bucket.")

        print(general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME))
        return general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME)

    try:
        # Invoke Step Function
        step_function_input = {
            "bucket_name": bucket_name_processing,
            "key_name": f"""{file_metadata["table_name"]}/{file_metadata["file_name"]}""",
            "file_name": file_metadata["file_name"],
            "table_name": target_table_to_load,
            "file_datetime": file_metadata["file_datetime"],
            "archive_data_file_once_done": str(archive_data_file_once_done)
        }

        step_function_client = boto3.client("stepfunctions")

        print("Invoking step function.")
        step_function_client.start_execution(
            stateMachineArn=boto3_utils.get_resource_arn("state_machine", step_function_name_final),
            input=json.dumps(step_function_input)
        )
        print("Step function invoked.")

        print(general_utils.LAMBDA_STRINGS.LAMBDA_SUCCESS_MSG.format(LAMBDA_NAME))
        return general_utils.LAMBDA_STRINGS.LAMBDA_SUCCESS_MSG.format(LAMBDA_NAME)
    except Exception as e:
        print(f"[ERROR] {e}")
        print(general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME))
        return general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME)
