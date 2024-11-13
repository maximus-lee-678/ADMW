from dmlib import general_utils
import custom_sql_parser
import determine_deps
import generate_transform_template, generate_hash_prelim_template
import os
import boto3
import s3fs
import sqlparse
import json
import jsbeautifier

LAMBDA_NAME = "LAMBDA_NAME"

KEY_INPUT_DDL_FILE = "input_ddl_file_name"
KEY_PRELIM_TABLE_PREFIX = "prelim_table_prefix"
KEY_PRELIM_COLUMN_PREFIX = "prelim_column_prefix"
KEY_SCHEMA = "schema_name"
KEY_PRELIM_DDL_FILE = "to_create_prelim_ddl_file_name"
KEY_FINAL_DDL_FILE = "to_create_final_ddl_file_name"
KEY_OUTPUT_METADATA_FILE = "to_create_metadata_file_name"
KEY_EXTRACTINATOR_MAPPINGS_FILE = "to_create_extractinator_mappings_file_name"
KEY_TRANSFORM_TEMPLATE_FILE = "to_create_transform_template_file_name"
KEY_PRELIM_HASH_TEMPLATE_FILE = "to_create_prelim_hash_template_file_name"
KEY_FINAL_HASH_TEMPLATE_FILE = "to_create_final_hash_template_file_name"

TRACKER_TABLE_DDL = "CREATE TABLE `{0}`.`ADMW_TRACKER_TABLE` (`id` INT NOT NULL AUTO_INCREMENT, \
`file_name` VARCHAR(256) NOT NULL, `executing_for_table` VARCHAR(128) NOT NULL, `mig_sts` VARCHAR(32), \
`error_msg` VARCHAR(1024), `recon_summary` VARCHAR(128), crt_upd_dt_time DATETIME NOT NULL, \
PRIMARY KEY (`file_name`, `executing_for_table`), KEY (`id`));"


def lambda_handler(event, context):
    global LAMBDA_NAME
    LAMBDA_NAME = os.environ["LAMBDA_NAME"]

    print(general_utils.LAMBDA_STRINGS.LAMBDA_START_MSG.format(LAMBDA_NAME))

    # env init
    bucket_name_config = os.environ["bucket_name_config"]

    # event parameters init
    pre_migration_s3_path = event["pre_migration_file_path"]

    parse_order = []
    preliminary_statements = []
    final_statements = []
    pk_lists = []
    col_lists = []
    table_dependencies = []
    datatype_prelim_mappings = []
    datatype_final_mappings = []

    pre_data = {}
    with s3fs.S3FileSystem().open(f"s3://{bucket_name_config}/{pre_migration_s3_path}", "r") as file:
        pre_data = json.load(file)

    sql = ""
    with s3fs.S3FileSystem().open(f"s3://{bucket_name_config}/{pre_data[KEY_INPUT_DDL_FILE]}", "r") as file:
        sql = file.read()

    statements = sqlparse.format(sql, reindent_aligned=True, keyword_case="upper", strip_comments=True)
    statements = sqlparse.split(statements)

    for statement in statements:
        sql_parsed_obj = custom_sql_parser.SQL_PARSER(
            create_statement=statement,
            schema_name=pre_data[KEY_SCHEMA],
            prelim_column_prefix=pre_data[KEY_PRELIM_COLUMN_PREFIX],
            prelim_table_prefix=pre_data[KEY_PRELIM_TABLE_PREFIX]
        )

        parse_order.append(sql_parsed_obj.get_table_name())
        preliminary_statements.append(sql_parsed_obj.get_preliminary_create_statement(has_constraints=False))
        final_statements.append(sql_parsed_obj.get_final_create_statement())
        table_dependencies.append(sql_parsed_obj.get_table_dependencies())
        datatype_prelim_mappings.append(sql_parsed_obj.get_datatype_mappings_prelim())
        datatype_final_mappings.append(sql_parsed_obj.get_datatype_mappings_final())
        pk_lists.append(sql_parsed_obj.get_primary_keys())
        col_lists.append(sql_parsed_obj.get_column_names())

    constrained_order = determine_deps.generate_insert_orders(table_dependencies)
    index_list = [parse_order.index(table) for table in constrained_order]

    preliminary_statements = [preliminary_statements[i] for i in index_list]
    final_statements = [final_statements[i] for i in index_list]
    pk_lists = [pk_lists[i] for i in index_list]
    col_lists = [col_lists[i] for i in index_list]
    datatype_prelim_mappings = [datatype_prelim_mappings[i] for i in index_list]
    datatype_final_mappings = [datatype_final_mappings[i] for i in index_list]
    table_columns = {table: col_lists[i] for i, table in enumerate(constrained_order)}

    s3 = boto3.client("s3")

    preliminary_statements_output = "\n".join(preliminary_statements + [TRACKER_TABLE_DDL.format(pre_data[KEY_SCHEMA])])
    final_statements_output = "\n".join(final_statements)
    transform_template_output = generate_transform_template.generate_template(constrained_order)
    hash_prelim_template_output = generate_hash_prelim_template.generate_template(table_columns)

    # write prelim ddl
    s3.put_object(
        Body=preliminary_statements_output.encode("utf-8"),
        Bucket=bucket_name_config,
        Key=pre_data[KEY_PRELIM_DDL_FILE]
    )
    # write final ddl
    s3.put_object(
        Body=final_statements_output.encode("utf-8"),
        Bucket=bucket_name_config,
        Key=pre_data[KEY_FINAL_DDL_FILE]
    )
    # write extractinator utility file
    s3.put_object(
        Body=json.dumps(table_columns).encode("utf-8"),
        Bucket=bucket_name_config,
        Key=pre_data[KEY_EXTRACTINATOR_MAPPINGS_FILE]
    )
    # write transform template
    s3.put_object(
        Body=transform_template_output.encode("utf-8"),
        Bucket=bucket_name_config,
        Key=pre_data[KEY_TRANSFORM_TEMPLATE_FILE]
    )
    # write hash prelim template
    s3.put_object(
        Body=hash_prelim_template_output.encode("utf-8"),
        Bucket=bucket_name_config,
        Key=pre_data[KEY_PRELIM_HASH_TEMPLATE_FILE]
    )

    migration_metadata = {
        "schema_name": pre_data[KEY_SCHEMA],
        "prelim_table_prefix": pre_data[KEY_PRELIM_TABLE_PREFIX],
        "prelim_column_prefix": pre_data[KEY_PRELIM_COLUMN_PREFIX],
        "load_order": constrained_order,
        "table_executes_which_flow": {f"{table_name}": [f"{table_name}"] for table_name in constrained_order},
        "tables": {}
    }
    for i, table_name in enumerate(constrained_order):
        migration_metadata["tables"][table_name] = {
            "prelim_name": f"{pre_data[KEY_PRELIM_TABLE_PREFIX]}{table_name}",
            "final_name": table_name,
            "mappings_prelim": [mapping for mapping in datatype_prelim_mappings[i]],
            "mappings_final": [mapping for mapping in datatype_final_mappings[i]],
            "primary_keys": pk_lists[i],
            "pre_exclude_from_hash": False
        }

    opts = jsbeautifier.default_options()
    opts.indent_size = 4
    formatted_output = jsbeautifier.beautify(json.dumps(migration_metadata), opts)

    # write config file
    s3.put_object(
        Body=formatted_output.encode("utf-8"),
        Bucket=bucket_name_config,
        Key=pre_data[KEY_OUTPUT_METADATA_FILE]
    )

    print(general_utils.LAMBDA_STRINGS.LAMBDA_SUCCESS_MSG.format(LAMBDA_NAME))
    return general_utils.LAMBDA_STRINGS.LAMBDA_SUCCESS_MSG.format(LAMBDA_NAME)
