from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from dmlib import spark_glue_utils, boto3_utils
import admw_glue_transforms
import sys
import s3fs
import json

JOB_NAME = "JOB_NAME"


def main():
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "bucket_name", "key_name", "file_name", "table_name", "file_datetime", "pipeline_stage",
         "bucket_name_config", "bucket_name_recon", "bucket_name_processing", "bucket_name_hashes", "object_path_cfg", "glue_connector_name"]
    )
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
        "column_mappings": dict_cfg["tables"][args["table_name"]]["mappings_prelim"]
    }

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

    dyf_transformed = admw_glue_transforms.transform_table(
        glue_context=glue_context,
        spark=spark,
        table_name=args["table_name"],
        file_name=args["file_name"],
        dyf_table=dyf_data_file,
        list_mappings=config_info["column_mappings"],
        other_info=None
    )
    print(f"Performed transformations on dataframe.")

    # get connection details
    conn_details = boto3_utils.get_mysql_conn_details_password(glue_connector_name=args["glue_connector_name"])
    spark_conn_details = spark_glue_utils.get_spark_connection_details(conn_details)

    glue_context.write_dynamic_frame.from_options(
        frame=dyf_transformed,
        connection_type="mysql",
        connection_options={
            "url": spark_conn_details["jdbc_url"],
            "user": spark_conn_details["connection_properties"]["user"],
            "password": spark_conn_details["connection_properties"]["password"],
            "dbtable": config_info["prelim_name"]
        }
    )
    print(f"Inserts completed.")

    print(spark_glue_utils.GLUE_STRINGS.GLUE_SUCCESS_MSG.format(JOB_NAME))
    job.commit()


if __name__ == "__main__":
    main()
