from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, when, lit, trim, split, explode, expr, sum, row_number
from awsglue.transforms import Filter, ApplyMapping
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
import admw_glue_transforms
import admw_glue_common_vars
import gs_derived


def get_boolean_select_expr(column_name):
    return f"CASE WHEN {column_name} IS TRUE THEN '1' ELSE '0' END"


def get_decimal_select_expr(column_name):
    return f"format_number({column_name}, '0.##')"


def hash_customer_details(glue_context, spark, df_table):
    """
    | Adds hash column for for table CUSTOMER_DETAILS.
    | Performs:
    - SHA256 checksum computation of concatenation of all hashable columns.
    - pre_points trimmed trailing 0s where possible.

    :param GlueContext glue_context: glue context object.
    :param SparkSession spark: spark session object.
    :param Spark DataFrame df_table: DataFrame containing the table CUSTOMER_DETAILS.

    :returns: Spark DataFrame
    """

    hashable_columns = ["pre_id", "pre_first_name", "pre_last_name", "pre_email", "pre_password",
                        "pre_dob", "pre_telephone", "pre_member_card_sn", "pre_points", "pre_created_at", "pre_modified_at"]
    decimal_columns = ["pre_points"]

    formatted_hashable_columns = []
    for hash_column in hashable_columns:
        if hash_column in decimal_columns:
            formatted_hashable_columns.append(get_decimal_select_expr(hash_column))
            print(f"[HASH_CUSTOMER_DETAILS] {hash_column} is treated as a decimal.")
        else:
            formatted_hashable_columns.append(hash_column)

    final_hash_expr = admw_glue_common_vars.ReconDefs.HASH_EXPR.format(",".join(formatted_hashable_columns))

    df_transformed = df_table.withColumn(admw_glue_common_vars.COLUMN_NAME_PRELIM_HASH, expr(final_hash_expr))
    print(f"[HASH_CUSTOMER_DETAILS] Hash column added to table.")

    return df_transformed


def hash_customer_purchase_instance(glue_context, spark, df_table):
    """
    | Adds hash column for for table CUSTOMER_PURCHASE_INSTANCE.
    | Performs:
    - SHA256 checksum computation of concatenation of all hashable columns.
    - pre_points_gained, pre_points_used trimmed trailing 0s where possible.
    
    :param GlueContext glue_context: glue context object.
    :param SparkSession spark: spark session object.
    :param Spark DataFrame df_table: DataFrame containing the table CUSTOMER_DETAILS.

    :returns: Spark DataFrame
    """

    hashable_columns = ["pre_id", "pre_id_cust", "pre_points_gained", "pre_points_used", "pre_created_at"]
    decimal_columns = ["pre_points_gained", "pre_points_used"]

    formatted_hashable_columns = []
    for hash_column in hashable_columns:
        if hash_column in decimal_columns:
            formatted_hashable_columns.append(get_decimal_select_expr(hash_column))
            print(f"[HASH_CUSTOMER_PURCHASE_INSTANCE] {hash_column} is treated as a decimal.")
        else:
            formatted_hashable_columns.append(hash_column)

    final_hash_expr = admw_glue_common_vars.ReconDefs.HASH_EXPR.format(",".join(formatted_hashable_columns))

    df_transformed = df_table.withColumn(admw_glue_common_vars.COLUMN_NAME_PRELIM_HASH, expr(final_hash_expr))
    print(f"[HASH_CUSTOMER_PURCHASE_INSTANCE] Hash column added to table.")

    return df_transformed


def hash_staff_details(glue_context, spark, df_table):
    """
    | Adds hash column for for table STAFF_DETAILS.
    | Performs:
    - SHA256 checksum computation of concatenation of all hashable columns.
    
    :param GlueContext glue_context: glue context object.
    :param SparkSession spark: spark session object.
    :param Spark DataFrame df_table: DataFrame containing the table CUSTOMER_DETAILS.

    :returns: Spark DataFrame
    """

    hashable_columns = ["pre_id", "pre_first_name", "pre_last_name", "pre_email",
                        "pre_password", "pre_telephone", "pre_created_at", "pre_modified_at"]

    formatted_hashable_columns = []
    for hash_column in hashable_columns:
        formatted_hashable_columns.append(hash_column)

    final_hash_expr = admw_glue_common_vars.ReconDefs.HASH_EXPR.format(",".join(formatted_hashable_columns))

    df_transformed = df_table.withColumn(admw_glue_common_vars.COLUMN_NAME_PRELIM_HASH, expr(final_hash_expr))
    print(f"[HASH_STAFF_DETAILS] Hash column added to table.")

    return df_transformed


def hash_product_brand(glue_context, spark, df_table):
    """
    | Adds hash column for for table PRODUCT_BRAND.
    | Performs:
    - SHA256 checksum computation of concatenation of all hashable columns.
    - pre_active converted from a boolean to 0 or 1.
    
    :param GlueContext glue_context: glue context object.
    :param SparkSession spark: spark session object.
    :param Spark DataFrame df_table: DataFrame containing the table CUSTOMER_DETAILS.

    :returns: Spark DataFrame
    """

    hashable_columns = ["pre_id", "pre_name", "pre_active", "pre_created_at", "pre_modified_at", "pre_modified_by"]
    boolean_columns = ["pre_active"]

    formatted_hashable_columns = []
    for hash_column in hashable_columns:
        if hash_column in boolean_columns:
            formatted_hashable_columns.append(get_boolean_select_expr(hash_column))
            print(f"[HASH_PRODUCT_BRAND] {hash_column} is treated as a boolean.")
        else:
            formatted_hashable_columns.append(hash_column)

    final_hash_expr = admw_glue_common_vars.ReconDefs.HASH_EXPR.format(",".join(formatted_hashable_columns))

    df_transformed = df_table.withColumn(admw_glue_common_vars.COLUMN_NAME_PRELIM_HASH, expr(final_hash_expr))
    print(f"[HASH_PRODUCT_BRAND] Hash column added to table.")

    return df_transformed


def hash_product_category(glue_context, spark, df_table):
    """
    | Adds hash column for for table PRODUCT_CATEGORY.
    | Performs:
    - SHA256 checksum computation of concatenation of all hashable columns.
    - pre_created_at, pre_modified_at, pre_modified_by omitted from hashable columns (transformed drastically).
    - pre_active converted from a boolean to 0 or 1.
    
    :param GlueContext glue_context: glue context object.
    :param SparkSession spark: spark session object.
    :param Spark DataFrame df_table: DataFrame containing the table CUSTOMER_DETAILS.

    :returns: Spark DataFrame
    """

    hashable_columns = ["pre_id", "pre_name", "pre_image_url", "pre_active"]
    boolean_columns = ["pre_active"]

    formatted_hashable_columns = []
    for hash_column in hashable_columns:
        if hash_column in boolean_columns:
            formatted_hashable_columns.append(get_boolean_select_expr(hash_column))
            print(f"[HASH_PRODUCT_CATEGORY] {hash_column} is treated as a boolean.")
        else:
            formatted_hashable_columns.append(hash_column)

    final_hash_expr = admw_glue_common_vars.ReconDefs.HASH_EXPR.format(",".join(formatted_hashable_columns))

    df_transformed = df_table.withColumn(admw_glue_common_vars.COLUMN_NAME_PRELIM_HASH, expr(final_hash_expr))
    print(f"[HASH_PRODUCT_CATEGORY] Hash column added to table.")

    return df_transformed


def hash_product_country(glue_context, spark, df_table):
    """
    | Adds hash column for for table PRODUCT_COUNTRY.
    | Performs:
    - SHA256 checksum computation of concatenation of all hashable columns.
    
    :param GlueContext glue_context: glue context object.
    :param SparkSession spark: spark session object.
    :param Spark DataFrame df_table: DataFrame containing the table CUSTOMER_DETAILS.

    :returns: Spark DataFrame
    """

    hashable_columns = ["pre_id", "pre_name"]

    formatted_hashable_columns = []
    for hash_column in hashable_columns:
        formatted_hashable_columns.append(hash_column)

    final_hash_expr = admw_glue_common_vars.ReconDefs.HASH_EXPR.format(",".join(formatted_hashable_columns))

    df_transformed = df_table.withColumn(admw_glue_common_vars.COLUMN_NAME_PRELIM_HASH, expr(final_hash_expr))
    print(f"[HASH_PRODUCT_COUNTRY] Hash column added to table.")

    return df_transformed


def hash_product_details(glue_context, spark, df_table):
    """
    | Adds hash column for for table PRODUCT_DETAILS.
    | Performs:
    - SHA256 checksum computation of concatenation of all hashable columns.
    - pre_price omitted from hashable columns (many false positives caused by comparisons like 9.900000000000002 vs 9.90).
    - pre_id_category_parent_minor omitted from hashable columns (empty rows have values changed to '1').
    - pre_active converted from a boolean to 0 or 1.
    
    :param GlueContext glue_context: glue context object.
    :param SparkSession spark: spark session object.
    :param Spark DataFrame df_table: DataFrame containing the table CUSTOMER_DETAILS.

    :returns: Spark DataFrame
    """

    hashable_columns = [
        "pre_id", "pre_id_brand", "pre_id_country", "pre_id_category_primary", "pre_id_category_parent_major", "pre_name",
        "pre_display_unit", "pre_description", "pre_key_information", "pre_rating_1", "pre_rating_2", "pre_rating_3", "pre_rating_4",
        "pre_rating_5", "pre_image_url", "pre_active", "pre_created_at", "pre_modified_at", "pre_modified_by"
    ]
    boolean_columns = ["pre_active"]

    formatted_hashable_columns = []
    for hash_column in hashable_columns:
        if hash_column in boolean_columns:
            formatted_hashable_columns.append(get_boolean_select_expr(hash_column))
            print(f"[HASH_PRODUCT_DETAILS] {hash_column} is treated as a boolean.")
        else:
            formatted_hashable_columns.append(hash_column)

    final_hash_expr = admw_glue_common_vars.ReconDefs.HASH_EXPR.format(",".join(formatted_hashable_columns))

    df_transformed = df_table.withColumn(admw_glue_common_vars.COLUMN_NAME_PRELIM_HASH, expr(final_hash_expr))
    print(f"[HASH_PRODUCT_DETAILS] Hash column added to table.")

    return df_transformed


def hash_product_secondary_category(glue_context, spark, df_table):
    """
    | Adds hash column for for table PRODUCT_SECONDARY_CATEGORY.
    | Performs:
    - pre_id, pre_id_prod, pre_id_cat omitted from hashable columns.
    - passes, zero hashable columns for this table.
    
    :param GlueContext glue_context: glue context object.
    :param SparkSession spark: spark session object.
    :param Spark DataFrame df_table: DataFrame containing the table CUSTOMER_DETAILS.

    :returns: Spark DataFrame
    """

    pass


def hash_customer_purchase_instance_items(glue_context, spark, df_table):
    """
    | Adds hash column for for table CUSTOMER_PURCHASE_INSTANCE_ITEMS.
    | Performs:
    - SHA256 checksum computation of concatenation of all hashable columns.
    - pre_price omitted from hashable columns (many false positives caused by comparisons like 9.900000000000002 vs 9.90).
    
    :param GlueContext glue_context: glue context object.
    :param SparkSession spark: spark session object.
    :param Spark DataFrame df_table: DataFrame containing the table CUSTOMER_DETAILS.

    :returns: Spark DataFrame
    """

    hashable_columns = ["pre_id", "pre_id_inst", "pre_id_prod", "pre_quantity"]

    formatted_hashable_columns = []
    for hash_column in hashable_columns:
        formatted_hashable_columns.append(hash_column)

    final_hash_expr = admw_glue_common_vars.ReconDefs.HASH_EXPR.format(",".join(formatted_hashable_columns))

    df_transformed = df_table.withColumn(admw_glue_common_vars.COLUMN_NAME_PRELIM_HASH, expr(final_hash_expr))
    print(f"[HASH_CUSTOMER_PURCHASE_INSTANCE_ITEMS] Hash column added to table.")

    return df_transformed


def hash_customer_cart_items(glue_context, spark, df_table):
    """
    | Adds hash column for for table CUSTOMER_CART_ITEMS.
    | Performs:
    - SHA256 checksum computation of concatenation of all hashable columns.
    
    :param GlueContext glue_context: glue context object.
    :param SparkSession spark: spark session object.
    :param Spark DataFrame df_table: DataFrame containing the table CUSTOMER_DETAILS.

    :returns: Spark DataFrame
    """

    hashable_columns = ["pre_id", "pre_id_cust", "pre_id_prod", "pre_quantity"]

    formatted_hashable_columns = []
    for hash_column in hashable_columns:
        formatted_hashable_columns.append(hash_column)

    final_hash_expr = admw_glue_common_vars.ReconDefs.HASH_EXPR.format(",".join(formatted_hashable_columns))

    df_transformed = df_table.withColumn(admw_glue_common_vars.COLUMN_NAME_PRELIM_HASH, expr(final_hash_expr))
    print(f"[HASH_CUSTOMER_CART_ITEMS] Hash column added to table.")

    return df_transformed


def populate_hash_column(glue_context, spark, table_name, df_table):
    """
    | Adds a hash column to a table based on its name.

    :param GlueContext glue_context: glue context object.
    :param SparkSession spark: spark session object.
    :param string table_name: name of the table to hash.
    :param Spark DataFrame df_table: DataFrame containing the table to hash.

    :returns: Spark DataFrame
    """

    if table_name not in admw_glue_transforms.VALID_TABLE_NAMES:
        raise ValueError(f"Table {table_name} not recognised!")

    print(f"Adding hash column to table {table_name}.")

    match(table_name):
        case "CUSTOMER_DETAILS":
            df_table_w_prelim_hash = hash_customer_details(glue_context, spark, df_table)
        case "CUSTOMER_PURCHASE_INSTANCE":
            df_table_w_prelim_hash = hash_customer_purchase_instance(glue_context, spark, df_table)
        case "STAFF_DETAILS":
            df_table_w_prelim_hash = hash_staff_details(glue_context, spark, df_table)
        case "PRODUCT_BRAND":
            df_table_w_prelim_hash = hash_product_brand(glue_context, spark, df_table)
        case "PRODUCT_CATEGORY":
            df_table_w_prelim_hash = hash_product_category(glue_context, spark, df_table)
        case "PRODUCT_COUNTRY":
            df_table_w_prelim_hash = hash_product_country(glue_context, spark, df_table)
        case "PRODUCT_DETAILS":
            df_table_w_prelim_hash = hash_product_details(glue_context, spark, df_table)
        case "PRODUCT_SECONDARY_CATEGORY":
            df_table_w_prelim_hash = hash_product_secondary_category(glue_context, spark, df_table)
        case "CUSTOMER_PURCHASE_INSTANCE_ITEMS":
            df_table_w_prelim_hash = hash_customer_purchase_instance_items(glue_context, spark, df_table)
        case "CUSTOMER_CART_ITEMS":
            df_table_w_prelim_hash = hash_customer_cart_items(glue_context, spark, df_table)

    return df_table_w_prelim_hash
