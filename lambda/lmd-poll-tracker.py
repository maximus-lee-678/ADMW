from dmlib import boto3_utils, general_utils, pymysql_utils
import boto3
import os

LAMBDA_NAME = "LAMBDA_NAME"

TRACKER_TABLE_COLS = ["id", "file_name", "executing_for_table", "crt_upd_dt_time", "mig_sts", "error_msg", "recon_summary"]

def lambda_handler(event, context):
    """
    | Event Payload:
    | 
    **Target Table: tracker** - {
        "select_columns": "?", # csv, optional

        "id": "?",  # optional
        "file_name": "?",  # optional
        "executing_for_table": "?",  # optional
        "mig_sts": "?",  # optional
        "error_msg": "?"  # optional
        "recon_summary": "?"  # optional
        "crt_upd_dt_time": "?",  # optional
    }
    """
        
    global LAMBDA_NAME
    LAMBDA_NAME = os.environ["LAMBDA_NAME"]

    print(general_utils.LAMBDA_STRINGS.LAMBDA_START_MSG.format(LAMBDA_NAME))

    # event parameters init
    select_columns = event.get("select_columns")
    where_columns = {}
    for key in TRACKER_TABLE_COLS:
        if key in event:
            where_columns[key] = event[key]

    # env init
    glue_connector_name = os.environ["glue_connector_name"]
    
    # get connection details
    conn_details = boto3_utils.get_mysql_conn_details_password(glue_connector_name=glue_connector_name)

    # query assembly
    # to ensure select outcome does not provide datetime or Decimal objects, cast to CHAR
    if select_columns:
        select_column_names = tuple(select_col.strip() for select_col in select_columns.split(",") if select_col in TRACKER_TABLE_COLS)
        select_column_list = [
            f"CAST({select_col} AS CHAR) AS {select_col}" for select_col in select_column_names if select_col in TRACKER_TABLE_COLS
        ]
    else:
        select_column_names = tuple(TRACKER_TABLE_COLS)
        select_column_list = [f"CAST({select_column} AS CHAR) AS {select_column}" for select_column in TRACKER_TABLE_COLS]

    where_column_string = " AND ".join([f"{key}=%s" for key in where_columns.keys()])

    query = f"""SELECT {",".join(select_column_list)} FROM ADMW_TRACKER_TABLE \
{f" WHERE {where_column_string}" if where_column_string else ""};"""
    task = tuple(value for value in where_columns.values())

    # query execution
    mysql_response = pymysql_utils.safe_select(conn_details, query, task, get_type="all")
    if not mysql_response["select_successful"]:
        print(f"""[ERROR] {mysql_response["exception"]}""")

        print(general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME))
        return general_utils.LAMBDA_STRINGS.LAMBDA_FAILED_MSG.format(LAMBDA_NAME)

    # console out
    print(f"""Selected from tracking table. Got {mysql_response["num_rows"]} row(s):""")
    print(select_column_names)
    for row in mysql_response["content"]:
        print(row)

    print(general_utils.LAMBDA_STRINGS.LAMBDA_SUCCESS_MSG.format(LAMBDA_NAME))
    return general_utils.LAMBDA_STRINGS.LAMBDA_SUCCESS_MSG.format(LAMBDA_NAME)
