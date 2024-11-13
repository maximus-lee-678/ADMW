from dmlib import boto3_utils, pymysql_utils, general_utils
import boto3
import pymysql
import os
import json

LAMBDA_NAME = "LAMBDA_NAME"

ERROR_STATUS = "STAGE {0} ERROR"
FALLBACK_ERROR_MESSAGE = "<UNKNOWN>"
ERROR_MESSAGE_SUBBABLE = "Stage {0} data pipeline for file {1} is failing at job {2} with error:\n[{3}] {4}.\n\nIf any fields are specified as \"UNKNOWN\", check step function execution directly, as specific details cannot be obtained due to Step Function limitations."

PIPELINE_STAGE_STAGING = 1
PIPELINE_STAGE_FINAL = 2


def lambda_handler(event, context):
    global LAMBDA_NAME
    LAMBDA_NAME = os.environ["LAMBDA_NAME"]

    print(general_utils.LAMBDA_STRINGS.LAMBDA_START_MSG.format(LAMBDA_NAME))

    # event parameters init
    pipeline_stage = event["pipeline_stage"]
    file_name = event["file_name"]
    table_name = event["table_name"]

    # env init
    glue_connector_name = os.environ["glue_connector_name"]
    topic_name = os.environ["topic_name"]

    error_type = event["error_details"]["Error"]
    if error_type == "Exception":   # lambda handled exception
        error_message = get_lambda_managed_exception_error_message(
            pipeline_stage=pipeline_stage,
            file_name=file_name,
            cause_dump=event["error_details"]["Cause"]
        )
    elif error_type in ["States.TaskFailed", "Glue.ConcurrentRunsExceededException"]: # glue exception
        error_message = get_glue_failure_error_message(
            pipeline_stage=pipeline_stage,
            file_name=file_name,
            cause_dump=event["error_details"]["Cause"]
        )
    elif error_type in ["Lambda.Unknown", "Lambda.AWSLambdaException"]:  # lambda unhandled exception (happens when failure is not caused by a runtime exception)
        error_message = get_lambda_unknown_error_message(
            pipeline_stage=pipeline_stage,
            file_name=file_name,
            error_type=error_type,
            cause_dump=event["error_details"]["Cause"]
        )
    else:
        print(f"[ERROR] Unhandled exception type: {error_type}.")
        print(general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME))
        raise Exception("Unhandled exception thrown.")

    try:
        conn_details = boto3_utils.get_mysql_conn_details_password(glue_connector_name=glue_connector_name)
    except Exception as e:
        print(f"[ERROR] {e}")

        print(general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME))
        raise e

    print("File migration status is being updated.")
    query = f"UPDATE ADMW_TRACKER_TABLE SET crt_upd_dt_time=\
{pymysql_utils.get_sql_time_now()}, mig_sts=%s, error_msg=%s WHERE file_name=%s AND executing_for_table=%s;"
    task = (ERROR_STATUS.format(pipeline_stage), error_message, file_name, table_name)

    mysql_response = pymysql_utils.safe_transaction(conn_details, query, task)
    if not mysql_response["transaction_successful"]:
        print(f"""[ERROR] {mysql_response["exception"]}""")
        print(general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME))
        raise pymysql.Error(mysql_response["exception"])
    
    print("File migration status updated successfully.")

    try:
        sns = boto3.client("sns")
        sns.publish(
            TopicArn=boto3_utils.get_resource_arn("sns", topic_name),
            Message=error_message
        )
        print("Notification published.")
    except Exception as e:
        print(f"[ERROR] {e}")
        print(general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME))
        raise e

    print(general_utils.LAMBDA_STRINGS.LAMBDA_SUCCESS_MSG.format(LAMBDA_NAME))
    return general_utils.LAMBDA_STRINGS.LAMBDA_SUCCESS_MSG.format(LAMBDA_NAME)


def get_lambda_managed_exception_error_message(pipeline_stage, file_name, cause_dump):
    """
    | Extracts error message from a lambda managed exception.

    :param string pipeline_stage: as its name suggests.
    :param string file_name: as its name suggests.
    :param string cause_dump: error cause key value contents.

    :returns: string
    """

    cause = json.loads(cause_dump)

    error_message = json.loads(cause["errorMessage"])

    error_type = error_message["error_type"]
    exception_cause = error_message["exception_cause"]
    lambda_name = error_message["lambda_name"]

    return ERROR_MESSAGE_SUBBABLE.format(pipeline_stage, file_name, lambda_name, error_type, exception_cause)


def get_glue_failure_error_message(pipeline_stage, file_name, cause_dump):
    """
    | Extracts error message from a glue exception.

    :param string pipeline_stage: as its name suggests.
    :param string file_name: as its name suggests.
    :param string cause_dump: error cause key value contents.

    :returns: string
    """
        
    cause = json.loads(cause_dump)

    for key in list(cause.keys()):
        cause[key.lower()] = cause.pop(key)

    # message is sometimes not sent by glue
    error_type = "Glue Runtime Error"
    exception_cause = cause.get("errormessage", FALLBACK_ERROR_MESSAGE)
    job_name = cause["jobname"]

    return ERROR_MESSAGE_SUBBABLE.format(pipeline_stage, file_name, job_name, error_type, exception_cause)


def get_lambda_unknown_error_message(pipeline_stage, file_name, error_type, cause_dump):
    """
    | Extracts error message from a non-runtime related Lambda exception.

    :param string pipeline_stage: as its name suggests.
    :param string file_name: as its name suggests.
    :param string error_type: error type key value contents.
    :param string cause_dump: error cause key value contents.

    :returns: string
    """
        
    match error_type:
        case "Lambda.Unknown":
            cause = json.loads(cause_dump.split("Returned payload: ")[1])
        case "Lambda.AWSLambdaException":
            cause = cause_dump

    exception_cause = cause["errorMessage"]
    lambda_name = FALLBACK_ERROR_MESSAGE

    return ERROR_MESSAGE_SUBBABLE.format(pipeline_stage, file_name, lambda_name, error_type, exception_cause)
