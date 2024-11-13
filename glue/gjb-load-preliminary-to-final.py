from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import lit
from dmlib import spark_glue_utils, boto3_utils
import admw_glue_common_vars
import sys
import s3fs
import json

JOB_NAME = "JOB_NAME"


def main():
    args = getResolvedOptions(sys.argv,
                              ["JOB_NAME", "bucket_name", "key_name", "file_name", "table_name", "file_datetime", "pipeline_stage",
                               "bucket_name_config", "bucket_name_recon", "bucket_name_processing", "bucket_name_hashes",
                               "object_path_cfg", "glue_connector_name"])
    global JOB_NAME
    JOB_NAME = args["JOB_NAME"]

    print(spark_glue_utils.GLUE_STRINGS.GLUE_START_MSG.format(JOB_NAME))

    spark_context = SparkContext.getOrCreate()
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
        "final_name": dict_cfg["tables"][args["table_name"]]["final_name"],
        "column_mappings": dict_cfg["tables"][args["table_name"]]["mappings_final"]
    }

    # extract only preliminary to final mappings
    mappings_prelim_to_final = []
    for column_mapping in config_info["column_mappings"]:
        mappings_prelim_to_final.append(tuple(column_mapping[0:4]))

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

    # apply mapping
    dyf_prelim_matched_new_columns = ApplyMapping.apply(
        frame=dyf_prelim_matched,
        mappings=mappings_prelim_to_final,
        transformation_ctx="dyf_final_new_rows",
    )
    print("Assembled new columns to be inserted.")

    # add filename column
    df_prelim_matched_new_columns = dyf_prelim_matched_new_columns.toDF()
    df_prelim_matched_new_columns = df_prelim_matched_new_columns.withColumn(
        admw_glue_common_vars.COLUMN_NAME_FINAL_ORIGIN_FILE_NAME, lit(args["file_name"])
    )

    dyf_prelim_matched_new_columns = DynamicFrame.fromDF(
        dataframe=df_prelim_matched_new_columns,
        glue_ctx=glue_context,
        name="dyf_prelim_matched_new_columns"
    )

    glue_context.write_dynamic_frame.from_options(
        frame=dyf_prelim_matched_new_columns,
        connection_type="mysql",
        connection_options={
            "url": spark_conn_details["jdbc_url"],
            "user": spark_conn_details["connection_properties"]["user"],
            "password": spark_conn_details["connection_properties"]["password"],
            "dbtable": config_info["final_name"]
        }
    )
    print(f"Inserts completed.")

    print(spark_glue_utils.GLUE_STRINGS.GLUE_SUCCESS_MSG.format(JOB_NAME))
    job.commit()


if __name__ == "__main__":
    main()
