from dmlib import boto3_utils, general_utils, pymysql_utils
import boto3
import s3fs
from enum import Enum
from pathlib import Path
import os
import re
import json

LAMBDA_NAME = "LAMBDA_NAME"

# starts with a alphabetical string that can contain underscores, followed by exactly one '~',
# followed by digits, followed by exactly one period, followed by digits, followed by exactly one period, and ends with any alphanumeric characters
REGEX_VALID_DATA_FILE = r"^[a-zA-Z_]+~\d+\.\d+\.[a-zA-Z0-9]*$"


class FILE_LOC(Enum):
    STORAGE = 0
    PROCESSING = 1


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
    bucket_name_storage = os.environ["bucket_name_storage"]
    glue_connector_name = os.environ["glue_connector_name"]
    object_path_cfg = os.environ["object_path_cfg"]
    step_function_name_prelim = os.environ["step_function_name_prelim"]

    if not re.match(REGEX_VALID_DATA_FILE, file_name):
        print("Invalid file name specified.")

        print(general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME))
        return general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME)

    file_metadata = general_utils.decompose_file_name(file_name)

    file_stored_in = None
    matched_files = boto3_utils.find_objects(
        bucket_name=bucket_name_storage,
        prefix=file_name
    )
    if len(matched_files) == 1:
        file_stored_in = FILE_LOC.STORAGE
    else:
        print("File was not found in storage bucket.")

    if not file_stored_in:
        # check if file is already in processing bucket, happens when a data file is needed for multiple tables and
        # processing has been started for at least one of these tables
        matched_files = boto3_utils.find_objects(
            bucket_name=bucket_name_processing,
            prefix=f"""{file_metadata["table_name"]}/{Path(file_name).with_suffix("")}"""
        )
        if len(matched_files) == 1:
            print("File found in processing bucket, skipping copy operation.")
            file_stored_in = FILE_LOC.PROCESSING
        else:
            print("File was not found in processing bucket as well.\nFile not found anywhere, please check file name and try again.")

            print(general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME))
            return general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME)

    dict_cfg = ""
    with s3fs.S3FileSystem().open(f"""s3://{bucket_name_config}/{object_path_cfg}""", "r") as file:
        dict_cfg = file.read()

    dict_cfg = json.loads(dict_cfg)

    executable_tables = dict_cfg["table_executes_which_flow"][file_metadata["table_name"]]

    if target_table_to_load not in executable_tables:
        print(f"Table {target_table_to_load} is not configured to be loaded with this file!")

        print(general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME))
        return general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME)

    if file_stored_in == FILE_LOC.STORAGE:
        # define source and destination paths
        source_file_name = f"{bucket_name_storage}/{file_name}"
        destination_file_name = f"""{bucket_name_processing}/{file_metadata["table_name"]}/{file_name}"""

        print("Defined source, destination and file metadata.")

        move_outcome = None
        file_suffix = Path(file_name).suffix.lower()
        if file_suffix in [".csv", ".txt"]:
            move_outcome = file_is_csv(source_file_name, destination_file_name)
        elif file_suffix in [".zip"]:
            move_outcome = file_is_archive(source_file_name, destination_file_name)
            # change file name to write to use csv extension
            if move_outcome:
                file_metadata["file_name"] = move_outcome
        else:
            print(f"[ERROR] Invalid file format!")

            print(general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME))
            return general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME)

        if not move_outcome:
            print(general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME))
            return general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME)

    # get connection details
    conn_details = boto3_utils.get_mysql_conn_details_password(glue_connector_name=glue_connector_name)

    try:
        step_function_client = boto3.client("stepfunctions")

        print("File migration status entry is being created.")
        query = f"""INSERT INTO ADMW_TRACKER_TABLE (file_name,executing_for_table,crt_upd_dt_time,mig_sts,error_msg,recon_summary) \
VALUES (%s,%s,{pymysql_utils.get_sql_time_now()},%s,%s,%s) \
AS alias \
ON DUPLICATE KEY UPDATE \
crt_upd_dt_time=alias.crt_upd_dt_time, \
mig_sts=alias.mig_sts, \
error_msg=alias.error_msg, \
recon_summary=alias.recon_summary;"""
        task = (file_name, target_table_to_load, "", "", "")
        mysql_response = pymysql_utils.safe_transaction(conn_details, query, task)
        if not mysql_response["transaction_successful"]:
            print(f"""[ERROR] {mysql_response["exception"]}""")
            print(general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME))
            return general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME)
        print("File migration status entry created successfully.")

        # Invoke Step Function
        step_function_input = {
            "bucket_name": bucket_name_processing,
            "key_name": f"""{file_metadata["table_name"]}/{file_metadata["file_name"]}""",
            "file_name": file_metadata["file_name"],
            "table_name": target_table_to_load,
            "file_datetime": file_metadata["file_datetime"]
        }

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


def file_is_csv(source_object_path, destination_object_path):
    """
    | Moves a csv file from one bucket to another.

    :param string source_object_path: as its name suggests.
    :param string destination_object_path: as its name suggests.
    """

    try:
        source_bucket = source_object_path.split("/", 1)[0]
        source_key = source_object_path.split("/", 1)[1]
        target_bucket = destination_object_path.split("/", 1)[0]
        target_key = destination_object_path.split("/", 1)[1]
        s3 = boto3.client("s3")

        # copy file into processing bucket
        boto3_utils.S3_COPY(show_progress=True).copy(
            source_bucket_name=source_bucket,
            source_key_name=source_key,
            dest_bucket_name=target_bucket,
            dest_key_name=target_key
        )
        print("File Copied to processing bucket.")

        # Delete file in landing bucket
        s3.delete_object(
            Bucket=source_bucket,
            Key=source_key
        )
        print("Original file deleted.")

        return True
    except Exception as e:
        print(f"[ERROR] {e}")

        return False


def file_is_archive(source_object_path, destination_object_path):
    """
    | Unzips a csv file while moving it from one bucket to another.

    :param string source_object_path: as its name suggests.
    :param string destination_object_path: as its name suggests.
    """
        
    split_source = source_object_path.split("/", 1)
    split_dest = destination_object_path.split("/", 1)

    source_bucket_name = split_source[0]
    source_key_name = split_source[1]
    dest_bucket_name = split_dest[0]
    dest_key_path_obj = Path(split_dest[1]).with_suffix(".csv")
    dest_key_name = dest_key_path_obj.as_posix()
    true_file_name = dest_key_path_obj.name

    s3 = boto3.client("s3")

    try:
        print("Beginning streaming file decompression and upload.")
        boto3_utils.S3_UNZIP(show_progress=True).unzip(
            source_bucket_name=source_bucket_name,
            source_key_name=source_key_name,
            dest_bucket_name=dest_bucket_name,
            dest_key_name=dest_key_name
        )
        print("Deompressed file uploaded to PROCESSING bucket.")

        s3.delete_object(
            Bucket=source_bucket_name,
            Key=source_key_name
        )
        print("Original file Deleted.")

        return true_file_name
    except Exception as e:
        print(f"[ERROR] {e}")

        return False
