from dmlib import boto3_utils, pymysql_utils, general_utils
import boto3
from pymysql import MySQLError
from pathlib import Path
import os
import re

LAMBDA_NAME = "LAMBDA_NAME"

SNS_SUCCESS_MESSAGE = "Stage {0} data pipeline for file {1}, targetting table {2} is complete.\nRecon summary:\n{3}"
COMPLETED_STATUS = ["STAGE 1 COMPLETED", "STAGE 2 COMPLETED"]
RECON_STATUS = ["STAGE 1 COMPLETED", "STAGE 2 REPORT GENERATED"]

S3_SEARCH_PREFIX_TEMPLATE = "{0}/{1}"
# accepts values, if any values are strings
# periods (.) must be suffixed with \\ (.\\)
VALID_FILES_REGEX_TEMPLATE = r"^{0}_STAGE{1}_[^\.]*\.[a-zA-Z0-9]*$"
RECON_FILE_NAME_SUMMARY_CONTAINS = "_RECON1"


def lambda_handler(event, context):
    global LAMBDA_NAME
    LAMBDA_NAME = os.environ["LAMBDA_NAME"]

    print(general_utils.LAMBDA_STRINGS.LAMBDA_START_MSG.format(LAMBDA_NAME))

    # event parameters init
    file_name = event["file_name"]
    table_name = event["table_name"]
    status = event["status"]
    pipeline_stage = event["pipeline_stage"]

    # env init
    glue_connector_name = os.environ["glue_connector_name"]
    bucket_name_recon = os.environ["bucket_name_recon"]
    topic_name = os.environ["topic_name"]

    try:
        conn_details = boto3_utils.get_mysql_conn_details_password(glue_connector_name=glue_connector_name)
    except Exception as e:
        print(f"[ERROR] {e}")

        print(general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME))
        raise Exception(general_utils.format_lambda_exception(
            lambda_name=LAMBDA_NAME,
            error_type=type(e).__name__,
            exception_cause=e.args[0]
        ))

    recon_summary = None
    try:
        if status in RECON_STATUS:
            recon_summary = get_recon_summary(file_name, table_name, pipeline_stage, bucket_name_recon)
            update_tracker(conn_details, file_name, table_name, status, recon_summary)
        else:
            update_tracker(conn_details, file_name, table_name, status, None)

        if status in COMPLETED_STATUS:
            if not recon_summary:
                recon_summary = get_recon_summary(file_name, table_name, pipeline_stage, bucket_name_recon)
            publish_sns_message(topic_name, pipeline_stage, file_name, table_name, recon_summary)
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


def get_recon_summary(file_name, table_name, pipeline_stage, bucket_name_recon):
    # search prep
    file_name_stemmed = Path(file_name).stem
    s3_search_prefix_formatted = S3_SEARCH_PREFIX_TEMPLATE.format(table_name, file_name_stemmed)
    file_name_regex_ready = file_name_stemmed.replace(".", "\\.")  # as earlier mentioned, periods (.) must be suffixed with \\ (.\\)
    valid_files_regex_formatted = VALID_FILES_REGEX_TEMPLATE.format(file_name_regex_ready, pipeline_stage)

    # search
    report_names = []
    s3_resource = boto3.Session().resource("s3")
    bucket_report = s3_resource.Bucket(bucket_name_recon)
    report_names = [object.key for object in bucket_report.objects.filter(Prefix=s3_search_prefix_formatted)]

    if not report_names:
        raise RuntimeError("No reconciliation files matched prefix!")

    print("Reconciliation filenames retrieved.")

    # consider only the final part of the key when doing regex checks
    report_names = [item for item in report_names if re.search(valid_files_regex_formatted, item.split("/")[-1])]

    if not report_names:
        raise RuntimeError("No reconciliation files matched regex check!")

    print("Reconciliation filenames filtered.")

    s3_path_recon_summary = next((element for element in report_names if RECON_FILE_NAME_SUMMARY_CONTAINS in element), None)

    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket_name_recon, Key=s3_path_recon_summary)
    recon_summary_contents = response["Body"].read().decode("utf-8")

    # formats recon 2 contents from multiline string to single line string with each line being surrounded by square brackets
    # assumes empty newline at end of string
    recon_summary_formatted = recon_summary_contents.replace("\n", "][")
    recon_summary_formatted = f"STAGE {pipeline_stage} - [{recon_summary_formatted}"
    recon_summary_formatted = recon_summary_formatted[:-1]

    return recon_summary_formatted


def update_tracker(conn_details, file_name, table_name, status, recon_summary):
    print("Updating file migration status.")
    
    if recon_summary:
        query = f"""UPDATE ADMW_TRACKER_TABLE SET crt_upd_dt_time=\
{pymysql_utils.get_sql_time_now()}, mig_sts=%s, error_msg="", recon_summary=%s WHERE file_name=%s AND executing_for_table=%s;"""
        task = (status, recon_summary, file_name, table_name)
        print(recon_summary)
    else:
        query = f"""UPDATE ADMW_TRACKER_TABLE SET crt_upd_dt_time=\
{pymysql_utils.get_sql_time_now()}, mig_sts=%s, error_msg="" WHERE file_name=%s AND executing_for_table=%s;"""
        task = (status, file_name, table_name)

    mysql_response = pymysql_utils.safe_transaction(conn_details, query, task)
    if not mysql_response["transaction_successful"]:
        print(f"""[ERROR] {mysql_response["exception"]}""")

        print(general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME))
        raise MySQLError(mysql_response["exception"])

    print("File migration status updated successfully.")


def publish_sns_message(topic_name, pipeline_stage, file_name, table_name, recon_summary):
    sns = boto3.client("sns")

    sns.publish(
        TopicArn=boto3_utils.get_resource_arn("sns", topic_name),
        Message=SNS_SUCCESS_MESSAGE.format(pipeline_stage, file_name, table_name, recon_summary)
    )
    print("Notification published.")
