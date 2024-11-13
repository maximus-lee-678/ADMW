from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import expr, when, col
import pyspark
from dmlib import spark_glue_utils, boto3_utils
import admw_glue_common_vars
import admw_glue_transforms
import admw_glue_prelim_hashing
from datetime import datetime
from enum import Enum
from pathlib import Path
import sys
import boto3
import s3fs
import json
import io
import os
import csv


COLUMN_NAME_TEMP_HASH_RESULT = ""

JOB_NAME = "JOB_NAME"


def main():
    args = getResolvedOptions(sys.argv,
                              ["JOB_NAME", "bucket_name", "key_name", "file_name", "table_name", "file_datetime", "pipeline_stage",
                               "bucket_name_config", "bucket_name_recon", "bucket_name_processing", "bucket_name_hashes",
                               "object_path_cfg", "glue_connector_name"])
    global JOB_NAME
    JOB_NAME = args["JOB_NAME"]

    print(spark_glue_utils.GLUE_STRINGS.GLUE_START_MSG.format(JOB_NAME))

    conf = pyspark.SparkConf().setAll([
        ("spark.driver.maxResultSize", "4g"),
        ("spark.sql.execution.arrow.pyspark.enabled", "true"),
        ("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED"),
        ("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED"),
        ("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED"),
        ("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    ])
    spark_context = SparkContext.getOrCreate(conf=conf)
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(JOB_NAME, args)

    print("Retrieving configuration file from S3.")
    s3_config_path = f"""s3://{args["bucket_name_config"]}/{args["object_path_cfg"]}"""

    dict_cfg = ""
    with s3fs.S3FileSystem().open(s3_config_path, "r") as file:
        dict_cfg = file.read()

    dict_cfg = json.loads(dict_cfg)
    print("Successfully retrieved configuration file.")

    config_info = {
        "prelim_name": dict_cfg["tables"][args["table_name"]]["prelim_name"],
        "primary_keys": dict_cfg["tables"][args["table_name"]]["primary_keys"],
        "column_mappings": dict_cfg["tables"][args["table_name"]]["mappings_prelim"],
        "pre_exclude_from_hash": dict_cfg["tables"][args["table_name"]]["pre_exclude_from_hash"]
    }

    # stop early if table is excluded from reconciliation checks
    if config_info["pre_exclude_from_hash"]:
        print(f"""Table {args["table_name"]} has been excluded from reconciliation checks.""")

        s3_path_recon_1 = admw_glue_common_vars.RECON_S3_PATH.format(
            Path(args["file_name"]).stem, args["pipeline_stage"], "RECON1", "csv"
        )

        s3 = boto3.client("s3")
        s3.put_object(
            Body=admw_glue_common_vars.EMPTY_RECON_CSV.encode("utf-8"),
            Bucket=args["bucket_name_recon"],
            Key=f"""{args["table_name"]}/{s3_path_recon_1}"""
        )
        print("Wrote empty reconcilation 1 to destination.")

        print(spark_glue_utils.GLUE_STRINGS.GLUE_SUCCESS_MSG.format(JOB_NAME))
        job.commit()
        os._exit(0)

    # define reconciliation expressions
    expr_pass_recon = f"{admw_glue_common_vars.COLUMN_NAME_FILE_HASH}={admw_glue_common_vars.COLUMN_NAME_PRELIM_HASH}"
    expr_fail_recon = f"{admw_glue_common_vars.COLUMN_NAME_FILE_HASH}!={admw_glue_common_vars.COLUMN_NAME_PRELIM_HASH}"
    expr_null_recon = f"{admw_glue_common_vars.COLUMN_NAME_PRELIM_HASH} IS NULL"

    s3_data_file_path = f"""s3://{args["bucket_name"]}/{args["key_name"]}"""

    print(f"Reading data file to DynamicFrame.")
    dyf_data_file = glue_context.create_dynamic_frame.from_options(
        format_options={
            "quoteChar": '"',
            "withHeader": True,
            "separator": ",",
            "optimizePerformance": False,
        },
        connection_type="s3",
        format="csv",
        connection_options={
            "paths": [s3_data_file_path],
            "recurse": True,
        },
        transformation_ctx="dyf_data_file",
    )
    print(f"Retrieved data file.")

    # get connection details
    conn_details = boto3_utils.get_mysql_conn_details_password(glue_connector_name=args["glue_connector_name"])
    spark_conn_details = spark_glue_utils.get_spark_connection_details(conn_details)

    # read preliminary table (apply predicate pushdown to filter for only correct filename)
    # important not to have a closing ;
    print(f"Reading preliminary table to DynamicFrame.")
    query_prelim_filter_filename = f"""SELECT * FROM {config_info["prelim_name"]} \
WHERE {admw_glue_common_vars.COLUMN_NAME_PRELIM_ORIGIN_FILE_NAME}='{args["file_name"]}'"""
    dyf_prelim_matched = glue_context.create_dynamic_frame.from_options(
        connection_type="mysql",
        connection_options={
            "url": spark_conn_details["jdbc_url"],
            "user": spark_conn_details["connection_properties"]["user"],
            "password": spark_conn_details["connection_properties"]["password"],
            "dbtable": config_info["prelim_name"],
            "sampleQuery": query_prelim_filter_filename
        },
        transformation_ctx="dyf_prelim_matched"
    )
    print("Preliminary table retrieved & filtered by file name.")

    df_data_file = dyf_data_file.toDF()
    df_prelim_matched = dyf_prelim_matched.toDF()

    df_prelim_matched_w_hash = admw_glue_prelim_hashing.populate_hash_column(
        glue_context, spark, args["table_name"], df_prelim_matched)

    # initialise temporary hash column name
    global COLUMN_NAME_TEMP_HASH_RESULT
    COLUMN_NAME_TEMP_HASH_RESULT = f"""admw_temp_{datetime.now().strftime("%H%M%S%f")}"""

    # produces true, false, or null (when either doesnt exist)
    df_prelim_matched_w_temp = df_prelim_matched_w_hash.withColumn(COLUMN_NAME_TEMP_HASH_RESULT, expr(expr_pass_recon))

    # [RECON1-1] count occurrences of passes and failures
    df_recon_1 = df_prelim_matched_w_temp.groupBy(COLUMN_NAME_TEMP_HASH_RESULT).count().na.fill(0)
    print("[RECON1-1] Progressed.")

    # [RECON1-2] map true to passed, false/null to failed
    df_recon_1 = df_recon_1.withColumn(
        admw_glue_common_vars.ReconDefs.COLUMN_NAME_RECON_OUTCOME,
        when(
            col(COLUMN_NAME_TEMP_HASH_RESULT) == True,
            admw_glue_common_vars.ReconDefs.RECON_STATUS_PASSED).otherwise(admw_glue_common_vars.ReconDefs.RECON_STATUS_FAILED)
    )
    print("[RECON1-2] Progressed.")

    # [RECON1-3] select only the outcome and count columns
    df_recon_1 = df_recon_1.select(admw_glue_common_vars.ReconDefs.COLUMN_NAME_RECON_OUTCOME, "count")
    print("[RECON1-3] Progressed.")

    # [RECON1-4] if no hash failures, generate a default row that reflects hash failures as 0
    if df_recon_1.filter(
            df_recon_1[admw_glue_common_vars.ReconDefs.COLUMN_NAME_RECON_OUTCOME] == admw_glue_common_vars.ReconDefs.RECON_STATUS_FAILED
    ).count() == 0:
        df_failed_zero = spark.createDataFrame(
            [(admw_glue_common_vars.ReconDefs.RECON_STATUS_FAILED, 0)],
            schema=StructType([
                StructField(admw_glue_common_vars.ReconDefs.COLUMN_NAME_RECON_OUTCOME, StringType(), True),
                StructField("count", IntegerType(), True)
            ])
        )

        df_recon_1 = df_recon_1.union(df_failed_zero)
    print("[RECON1-4] Progressed.")
    print("Reconciliation 1 sparkframe generated.")

    pdf_recon_1 = df_recon_1.toPandas()
    print("Reconciliation 1 pandas dataframe generated.")

    # join on pk(s)
    join_string = " AND ".join(f"a.{column}=b.pre_{column}" for column in config_info["primary_keys"])

    df_data_file.createOrReplaceTempView("tbl_data_file")
    df_prelim_matched_w_hash.createOrReplaceTempView("tbl_prelim_w_hash")
    print("SQL Context tables created.")

    # master join (need select all for upcoming recon 3)
    df_joined_tables = spark.sql(f"SELECT * FROM tbl_data_file AS a LEFT JOIN tbl_prelim_w_hash AS b ON {join_string}")
    print("Tables joined.")

    # [RECON2-1] split pass and fails
    df_recon_2_passed = df_joined_tables.filter(expr(expr_pass_recon))
    df_recon_2_failed = df_joined_tables.filter(expr(expr_fail_recon) | expr(expr_null_recon))
    print("[RECON2-1] Progressed.")

    # [RECON2-2] get only relevant columns and limit row count for recon 2
    cols_data_file = df_data_file.schema.names
    cols_prelim_table = df_prelim_matched.schema.names + [admw_glue_common_vars.COLUMN_NAME_PRELIM_HASH]
    pdf_recon_2_data_file_passed, pdf_recon_2_prelim_table_passed = modify_recon_2_frames(
        df_recon_2_passed, cols_data_file, cols_prelim_table, admw_glue_common_vars.ReconDefs.RECON_STATUS_PASSED)
    pdf_recon_2_data_file_failed, pdf_recon_2_prelim_table_failed = modify_recon_2_frames(
        df_recon_2_failed, cols_data_file, cols_prelim_table, admw_glue_common_vars.ReconDefs.RECON_STATUS_FAILED)
    print("[RECON2-2] Progressed.")

    # [RECON2-3] convert to csv string
    csv_lines_passes = generate_recon_2_csv_str(
        column_mappings=config_info["column_mappings"],
        pdf_data_file=pdf_recon_2_data_file_passed,
        pdf_prelim_table=pdf_recon_2_prelim_table_passed,
        status=admw_glue_common_vars.ReconDefs.RECON_STATUS_PASSED,
        print_header=False
    )
    csv_lines_fails = generate_recon_2_csv_str(
        column_mappings=config_info["column_mappings"],
        pdf_data_file=pdf_recon_2_data_file_failed,
        pdf_prelim_table=pdf_recon_2_prelim_table_failed,
        status=admw_glue_common_vars.ReconDefs.RECON_STATUS_FAILED,
        print_header=True
    )
    csv_lines_all = csv_lines_fails + csv_lines_passes
    print("[RECON2-3] Progressed.")
    print("Reconciliation 2 CSV generated.")

    # write recon 1 (csv, summary)
    s3_path_recon_1 = admw_glue_common_vars.RECON_S3_PATH.format(Path(args["file_name"]).stem, args["pipeline_stage"], "RECON1", "csv")
    pdf_recon_1.to_csv(f"""s3://{args["bucket_name_recon"]}/{args["table_name"]}/{s3_path_recon_1}""", index=False)
    print("Wrote reconciliation file 1 to destination.")

    # write recon 2 (csv, detailed)
    s3_path_recon_2 = admw_glue_common_vars.RECON_S3_PATH.format(Path(args["file_name"]).stem, args["pipeline_stage"], "RECON2", "csv")
    s3 = boto3.client("s3")
    s3.put_object(
        Body=csv_lines_all.encode("utf-8"),
        Bucket=args["bucket_name_recon"],
        Key=f"""{args["table_name"]}/{s3_path_recon_2}"""
    )
    print("Wrote reconciliation file 2 to destination.")

    # Write hashes to hash bucket
    df_pks_and_hash = df_prelim_matched_w_hash.select(
        [f"pre_{primary_key}" for primary_key in config_info["primary_keys"]] +
        [admw_glue_common_vars.COLUMN_NAME_FILE_HASH, admw_glue_common_vars.COLUMN_NAME_PRELIM_HASH]
    )

    df_pks_and_hash.write.parquet(
        f"""s3://{args["bucket_name_hashes"]}/{args["table_name"]}/STAGE1/{args["file_datetime"]}""",
        mode="overwrite"
    )
    print("Wrote all hashes to hash bucket.")

    print(spark_glue_utils.GLUE_STRINGS.GLUE_SUCCESS_MSG.format(JOB_NAME))
    job.commit()


def modify_recon_2_frames(df_joined, col_names_data_file, col_names_prelim_table, recon_outcome):
    """
    | This function splits the joined table back into the preliminary and final table's contents.
    | Also limits number of returned rows.

    :param DataFrame df_joined: joined data file and preliminary table DataFrame.
    :param list col_names_data_file: list of column names to select from the data file.
    :param list col_names_prelim_table: list of column names to select from the preliminary table.
    :param str recon_outcome: outcome of the reconciliation, either "PASSED" or "FAILED".

    :returns: pandas.DataFrame, pandas.DataFrame
    """

    if recon_outcome == admw_glue_common_vars.ReconDefs.RECON_STATUS_PASSED:
        df_joined = df_joined.limit(admw_glue_common_vars.ReconDefs.RECON_2_NUM_PASSED)
    elif recon_outcome == admw_glue_common_vars.ReconDefs.RECON_STATUS_FAILED:
        df_joined = df_joined.limit(admw_glue_common_vars.ReconDefs.RECON_2_NUM_FAILED)

    # limit retrieves arbitrary rows across partitions, causing non-deterministic results in subsequent operations
    # this causes selecting two sets of columns to retrieve different rows, we don't want that
    # to solve this, we convert to pandas which 'collapses' the limited rows, making them stable

    # pandas formatting is particular about formats for certain datatypes, casting to string ensures all sparkframes can be converted without exceptions
    df_joined = df_joined.select([col(column).cast("string") for column in df_joined.columns])

    pdf_joined = df_joined.toPandas()

    pdf_data_file = pdf_joined[col_names_data_file]
    pdf_prelim_table = pdf_joined[col_names_prelim_table]

    print(f"Generated reconciliation child pandas frames: data file's and prelim table's {recon_outcome} content.")

    return pdf_data_file, pdf_prelim_table


def generate_recon_2_csv_str(column_mappings, pdf_data_file, pdf_prelim_table, status, print_header):
    """
    | Generates a large string of CSV rows for reconciliation 2.
    | Column headers include:
    - preliminary table column names <> final table column names (for each column mapping)
    - exclusive columns in data file
    - exclusive columns in preliminary table
    - data_source
    - reconciliation outcome
    - hash comparison result (preliminary hash <> final hash)

    :param list column_mappings: list of column mappings.
    :param pandas.DataFrame pdf_data_file: data file DataFrame.
    :param pandas.DataFrame pdf_prelim_table: preliminary table DataFrame.
    :param str status: reconciliation outcome, either "PASSED" or "FAILED".
    :param bool print_header: whether to print the header row.

    :returns: str
    """

    def list_to_csv_row(data):
        """
        Converts a list of strings to a single, CSV row string.
        
        :param list data: list of data to convert to CSV row.
        
        :returns: str
        """
        output = io.StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)

        writer.writerow(data)

        return output.getvalue().strip()

    columns_informational = [
        "data_source",
        admw_glue_common_vars.ReconDefs.COLUMN_NAME_RECON_OUTCOME,
        f"{admw_glue_common_vars.COLUMN_NAME_FILE_HASH} <> {admw_glue_common_vars.COLUMN_NAME_PRELIM_HASH}"
    ]

    exclusive_cols_data_file = [
        col for col in pdf_data_file.columns
        if col not in [column_mapping[0] for column_mapping in column_mappings]
    ]
    exclusive_cols_prelim_table = [
        col for col in pdf_prelim_table.columns
        if col not in [column_mapping[2] for column_mapping in column_mappings]
    ]

    row_string_list = []

    if print_header:
        column_header_list = []

        for column_mapping in column_mappings:
            column_header_list.append(f"""{column_mapping[0]} <> {column_mapping[2]}""")

        column_header_list = column_header_list + exclusive_cols_data_file + exclusive_cols_prelim_table + columns_informational
        row_string_list.append(list_to_csv_row(column_header_list))

    iter_prelim = pdf_prelim_table.iterrows()
    for (row_index_data_file, row_data_file) in pdf_data_file.iterrows():
        row_index_prelim_table, row_prelim_table = next(iter_prelim)

        row_data_file_list = []
        row_prelim_table_list = []

        # column mapping values
        for column_mapping in column_mappings:
            row_data_file_list.append(row_data_file[column_mapping[0]])
            row_prelim_table_list.append(row_prelim_table[column_mapping[2]])

        # only data file column values
        for column in exclusive_cols_data_file:
            row_data_file_list.append(row_data_file[column])
            row_prelim_table_list.append("-NA-")

        # only preliminary table column values
        for column in exclusive_cols_prelim_table:
            row_data_file_list.append("-NA-")
            row_prelim_table_list.append(row_prelim_table[column])

        # data source column value
        row_data_file_list.append("data_file")
        row_prelim_table_list.append("preliminary_table")

        # status column value
        row_data_file_list.append(status)
        row_prelim_table_list.append(status)

        # variable hash column value
        row_data_file_list.append(row_data_file[admw_glue_common_vars.COLUMN_NAME_OG_FILE_HASH])
        row_prelim_table_list.append(row_prelim_table[admw_glue_common_vars.COLUMN_NAME_PRELIM_HASH])

        row_string_list.append(list_to_csv_row(row_data_file_list))
        row_string_list.append(list_to_csv_row(row_prelim_table_list))

    return "\n".join(row_string_list) + "\n" if row_string_list else ""


if __name__ == "__main__":
    main()
