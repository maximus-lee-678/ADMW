from dmlib import boto3_utils, general_utils, pymysql_utils
import boto3
from pathlib import Path
import pandas as pd
import s3fs
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

    # env init
    bucket_name_config = os.environ["bucket_name_config"]
    bucket_name_processing = os.environ["bucket_name_processing"]
    object_path_cfg = os.environ["object_path_cfg"]
    glue_connector_name = os.environ["glue_connector_name"]
    step_function_name_prelim = os.environ["step_function_name_prelim"]

    if not re.match(REGEX_VALID_DATA_FILE, file_name):
        print("Invalid file name specified.")

        print(general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME))
        return general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME)
    
    file_metadata = general_utils.decompose_file_name(file_name)
    
    matched_files = boto3_utils.find_objects(
        bucket_name=bucket_name_processing,
        prefix=f"""{file_metadata["table_name"]}/{file_name}"""
    )
    if len(matched_files) != 1:
        print("File not found in processing bucket.")

        print(general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME))
        return general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME)


    print("Retrieving configuration details from S3.")

    dict_cfg = ""
    with s3fs.S3FileSystem().open(f"""s3://{bucket_name_config}/{object_path_cfg}""", "r") as file:
        dict_cfg = file.read()

    dict_cfg = json.loads(dict_cfg)
    executable_tables = dict_cfg["table_executes_which_flow"][file_metadata["table_name"]]
    table_name_prelim_mysql = dict_cfg["tables"][target_table_to_load]["prelim_name"]

    if target_table_to_load not in executable_tables:
        print(f"Table {target_table_to_load} is not configured to be loaded with this file!")

        print(general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME))
        return general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME)

    # get connection details
    conn_details = boto3_utils.get_mysql_conn_details_password(glue_connector_name=glue_connector_name)

    try:
        print("Deleting matching records from preliminary table.")
        query = f"""DELETE FROM {table_name_prelim_mysql} WHERE pre_admw_origin_file_name=%s;"""
        task = (file_name)
        mysql_response = pymysql_utils.safe_transaction(conn_details, query, task)
        if not mysql_response["transaction_successful"]:
            print(f"""[ERROR] {mysql_response["exception"]}""")
            print(general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME))
            return general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME)
        print("Matching records successfully deleted.")        

        print("File migration status entry is being cleared.")
        query = f"""UPDATE ADMW_TRACKER_TABLE SET crt_upd_dt_time={pymysql_utils.get_sql_time_now()}, \
mig_sts="", error_msg="", recon_summary="" WHERE file_name=%s AND executing_for_table=%s;"""
        task = (file_name, target_table_to_load)
        mysql_response = pymysql_utils.safe_transaction(conn_details, query, task)
        if not mysql_response["transaction_successful"]:
            print(f"""[ERROR] {mysql_response["exception"]}""")
            print(general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME))
            return general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME)
        print("File migration status entry successfully cleared.")

        # Invoke Step Function
        step_function_input = {
            "bucket_name": bucket_name_processing,
            "key_name": f"""{file_metadata["table_name"]}/{file_metadata["file_name"]}""",
            "file_name": file_metadata["file_name"],
            "table_name": target_table_to_load,
            "file_datetime": file_metadata["file_datetime"]
        }

        step_function_client = boto3.client("stepfunctions")

        print("Invoking step function.")
        step_function_client.start_execution(
            stateMachineArn=boto3_utils.get_resource_arn("state_machine", step_function_name_prelim),
            input=json.dumps(step_function_input)
        )
        print("Step function invoked.")

        print(general_utils.LAMBDA_STRINGS.LAMBDA_SUCCESS_MSG.format(LAMBDA_NAME))
        return general_utils.LAMBDA_STRINGS.LAMBDA_SUCCESS_MSG.format(LAMBDA_NAME)
    except Exception as e:
        print(f"[ERROR] {e}")
        print(general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME))
        return general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME)
