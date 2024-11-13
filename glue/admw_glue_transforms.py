from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, when, lit, trim, split, explode, expr, to_timestamp
from awsglue.transforms import Filter, ApplyMapping
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
import gs_derived
import admw_glue_common_vars


VALID_TABLE_NAMES = [
    "CUSTOMER_DETAILS", "CUSTOMER_PURCHASE_INSTANCE", "STAFF_DETAILS", "PRODUCT_BRAND", "PRODUCT_CATEGORY", "PRODUCT_COUNTRY",
    "PRODUCT_DETAILS", "PRODUCT_SECONDARY_CATEGORY", "CUSTOMER_PURCHASE_INSTANCE_ITEMS", "CUSTOMER_CART_ITEMS"
]

PRELIM_HASH_TUPLE_MAPPING = (
    admw_glue_common_vars.COLUMN_NAME_OG_FILE_HASH, "string", admw_glue_common_vars.COLUMN_NAME_FILE_HASH, "string"
)


def generic_map_columns(dyf_table, list_mappings):
    """
    Apply preliminary table column name mappings to a DynamicFrame.

    :param DynamicFrame dyf_table: Input DynamicFrame.
    :param tuple list_mappings: List of lists containing column mappings.

    :returns: DynamicFrame
    """

    list_mappings.append(PRELIM_HASH_TUPLE_MAPPING)

    prelim_mappings = []
    for mapping in list_mappings:
        prelim_mappings.append(tuple(mapping[0:4]))

    if not prelim_mappings:
        return dyf_table

    return ApplyMapping.apply(
        frame=dyf_table,
        mappings=prelim_mappings,
        transformation_ctx="dyf_updated_cols"
    )


def generic_add_prelim_columns(df_table, file_name):
    """
    Add common preliminary columns present in every preliminary table to a SparkFrame.

    :param SparkFrame df_table: Input SparkFrame.
    :param str file_name: Name of data file being processed in this Glue Job.

    :returns: SparkFrame
    """

    df_table = df_table.withColumn(admw_glue_common_vars.COLUMN_NAME_PRELIM_ORIGIN_FILE_NAME, lit(str(file_name)))

    return df_table


def generic_cast_voids(df_table):
    """
    | Convert void columns to StringType to prevent "Can't get JDBC type for void" exceptions.
    | Occurs when all rows of a column are null, causing .dtypes for it to be ('column_name', 'void')

    :param SparkFrame df_table: Input Sparkframe.

    :returns: SparkFrame
    """

    void_cols = [col_name for col_name, col_type in df_table.dtypes if col_type == "void"]
    if not void_cols:
        return df_table

    for col_name in void_cols:
        df_cast_table = df_table.withColumn(col_name, df_table[col_name].cast(StringType()))

    return df_cast_table


def transform_customer_details(glue_context, spark, df_table):
    """
    | Transformation for table CUSTOMER_DETAILS.
    | Performs:
    - No transformations.

    :param GlueContext glue_context: GlueContext object.
    :param SparkContext spark: SparkContext object.
    :param SparkFrame df_table: CUSTOMER_DETAILS SparkFrame.

    :returns: SparkFrame
    """

    return df_table


def transform_customer_purchase_instance(glue_context, spark, df_table):
    """
    | Transformation for table CUSTOMER_PURCHASE_INSTANCE.
    | Performs:
    - No transformations.

    :param GlueContext glue_context: GlueContext object.
    :param SparkContext spark: SparkContext object.
    :param SparkFrame df_table: CUSTOMER_PURCHASE_INSTANCE SparkFrame.

    :returns: SparkFrame
    """

    return df_table


def transform_staff_details(glue_context, spark, df_table):
    """
    | Transformation for table STAFF_DETAILS.
    | Performs:
    - No transformations.

    :param GlueContext glue_context: GlueContext object.
    :param SparkContext spark: SparkContext object.
    :param SparkFrame df_table: STAFF_DETAILS SparkFrame.

    :returns: SparkFrame
    """

    return df_table


def transform_product_brand(glue_context, spark, df_table):
    """
    | Transformation for table PRODUCT_BRAND.
    | Performs:
    - No transformations.

    :param GlueContext glue_context: GlueContext object.
    :param SparkContext spark: SparkContext object.
    :param SparkFrame df_table: PRODUCT_BRAND SparkFrame.

    :returns: SparkFrame
    """

    return df_table


def transform_product_category(glue_context, spark, df_table):
    """
    | Transformation for table PRODUCT_CATEGORY.
    | Performs:
    - PRODUCT_CATEGORY_1: CONVERT ISO8601 to local timestamps for timestamp columns.
    - PRODUCT_CATEGORY_2: UPDATE nulls to 1900-01-01 00:00:00 for timestamp columns.
    - PRODUCT_CATEGORY_3: UPDATE blanks to '1' in pre_modified_by column.

    :param GlueContext glue_context: GlueContext object.
    :param SparkContext spark: SparkContext object.
    :param SparkFrame df_table: PRODUCT_CATEGORY SparkFrame.

    :returns: SparkFrame
    """

    # 1. update timestamps
    df_table = df_table.withColumn("pre_created_at", to_timestamp("pre_created_at") + expr("INTERVAL 8 HOURS"))
    df_table = df_table.withColumn("pre_modified_at", to_timestamp("pre_modified_at") + expr("INTERVAL 8 HOURS"))
    print("[PRODUCT_CATEGORY_1] Converted timestamps.")

    # 2. make sure timestamps are not null
    df_table = df_table.withColumn("pre_created_at", when(col("pre_created_at").isNull(),
                                   lit("1900-01-01 00:00:00")).otherwise(col("pre_created_at")))
    df_table = df_table.withColumn("pre_modified_at", when(col("pre_modified_at").isNull(),
                                   lit("1900-01-01 00:00:00")).otherwise(col("pre_modified_at")))
    print("[PRODUCT_CATEGORY_2] Populated blank timestamps.")

    # 3. make sure modified_by is not empty
    df_table = df_table.withColumn("pre_modified_by", when(col("pre_modified_by") == "", lit("1")).otherwise(col("pre_modified_by")))
    print("[PRODUCT_CATEGORY_3] Populated blank modified by fields.")

    return df_table


def transform_product_country(glue_context, spark, df_table):
    """
    | Transformation for table PRODUCT_COUNTRY.
    | Performs:
    - No transformations.

    :param GlueContext glue_context: GlueContext object.
    :param SparkContext spark: SparkContext object.
    :param SparkFrame df_table: PRODUCT_COUNTRY SparkFrame.

    :returns: SparkFrame
    """

    return df_table


def transform_product_details(glue_context, spark, df_table):
    """
    | Transformation for table PRODUCT_DETAILS.
    | Performs:
    - PRODUCT_DETAILS_1: DROP secondary categories column (moved to PRODUCT_SECONDARY_CATEGORY table).
    - PRODUCT_DETAILS_2: UPDATE blanks in pre_id_category_parent_minor column to '0'.

    :param GlueContext glue_context: GlueContext object.
    :param SparkContext spark: SparkContext object.
    :param SparkFrame df_table: PRODUCT_DETAILS SparkFrame.

    :returns: SparkFrame
    """

    # 1. drop
    df_table = df_table.drop("id_category_secondary")
    print("[PRODUCT_DETAILS_1] Dropped secondary categories.")

    # 2. fill blanks
    df_table = df_table.withColumn("pre_id_category_parent_minor",
                                   when(col("pre_id_category_parent_minor") == "", "0").otherwise(col("pre_id_category_parent_minor")))
    print("[PRODUCT_DETAILS_2] Replaced blanks in pre_id_category_parent_minor with \"0\".")

    return df_table


def transform_product_secondary_category(glue_context, spark, df_table):
    """
    | Transformation for table PRODUCT_SECONDARY_CATEGORY.
    | Performs:
    - PRODUCT_SECONDARY_CATEGORY_1: DROP everything except product id and secondary categories (goodbye hash)
    - PRODUCT_SECONDARY_CATEGORY_2: SPLIT secondary categories into separate rows.
    - PRODUCT_SECONDARY_CATEGORY_3: ADD id column.

    :param GlueContext glue_context: GlueContext object.
    :param SparkContext spark: SparkContext object.
    :param SparkFrame df_table: PRODUCT_SECONDARY_CATEGORY SparkFrame.

    :returns: SparkFrame
    """

    # 1. select columns
    df_table = df_table.select(col("pre_id"), col("pre_id_category_secondary"))
    print("[PRODUCT_SECONDARY_CATEGORY_1] Removed hash column.")

    # 2.1. create new column with list of categories
    df_table = df_table.withColumn("split_categories", expr("transform(split(pre_id_category_secondary, ','), x -> trim(x))"))

    # 2.2. https://www.youtube.com/watch?v=jEexefuB62c&pp=ygUOZXhwbG9zaW9uIG1lbWU%3D
    df_table_exploded = df_table.withColumn("pre_id_cat", explode(col("split_categories")))

    # 2.3. rename columns
    df_table_exploded = df_table_exploded.select(col("pre_id").alias("pre_id_prod"), "pre_id_cat")

    # 2.4. make sure categories are not null
    df_table_exploded = df_table_exploded.withColumn("pre_id_cat", when(
        col("pre_id_cat") == "", lit(None)).otherwise(col("pre_id_cat")))
    print("[PRODUCT_SECONDARY_CATEGORY_2] Split secondary categories into separate rows.")

    # 3. add new id column
    df_table_exploded_with_id = df_table_exploded.rdd.zipWithIndex().toDF()
    df_table_exploded_with_id = df_table_exploded_with_id.select(col("_1.*"), col("_2").alias("pre_id"))
    print("[PRODUCT_SECONDARY_CATEGORY_3] Added id column.")

    return df_table_exploded_with_id


def transform_customer_purchase_instance_items(glue_context, spark, df_table):
    """
    | Transformation for table CUSTOMER_PURCHASE_INSTANCE_ITEMS.
    | Performs:
    - No transformations.

    :param GlueContext glue_context: GlueContext object.
    :param SparkContext spark: SparkContext object.
    :param SparkFrame df_table: CUSTOMER_PURCHASE_INSTANCE_ITEMS SparkFrame.

    :returns: SparkFrame
    """

    return df_table


def transform_customer_cart_items(glue_context, spark, df_table):
    """
    | Transformation for table CUSTOMER_CART_ITEMS.
    | Performs:
    - No transformations.

    :param GlueContext glue_context: GlueContext object.
    :param SparkContext spark: SparkContext object.
    :param SparkFrame df_table: CUSTOMER_CART_ITEMS SparkFrame.

    :returns: SparkFrame
    """

    return df_table


def transform_table(glue_context, spark, table_name, file_name, dyf_table, list_mappings, other_info):
    """
    | Performs transformation on a table based on its name.
    | After delegating to the appropriate transformation function, the following generic transformations are applied:
    - GENERIC_PRE_1: Convert column names to specified prelimary names.
    - GENERIC_POST_1: Add common preliminary columns
    - GENERIC_POST_2: Convert null value to string to prevent "Can't get JDBC type for void" exceptions

    :param GlueContext glue_context: GlueContext object.
    :param SparkContext spark: SparkContext object.
    :param str table_name: Table name to transform.
    :param DynamicFrame dyf_table: Input DynamicFrame.
    :param dict list_mappings: list of lists containing column mappings.
    :param dict other_info: Other information needed for transformation.

    :returns: DynamicFrame
    """

    if table_name not in VALID_TABLE_NAMES:
        raise ValueError(f"Table {table_name} not recognised!")

    print(f"Performing transformation(s) on table {table_name}.")

    # GENERIC_PRE_1: Apply datatype mappings
    dyf_preliminary = generic_map_columns(dyf_table, list_mappings)
    print(f"[{table_name}_GENERIC_PRE_1] Applied datatype mappings according to specification in configuration file.")

    df_preliminary = dyf_preliminary.toDF()

    match(table_name):
        case "CUSTOMER_DETAILS":
            df_transformed = transform_customer_details(glue_context, spark, df_preliminary)
        case "CUSTOMER_PURCHASE_INSTANCE":
            df_transformed = transform_customer_purchase_instance(glue_context, spark, df_preliminary)
        case "STAFF_DETAILS":
            df_transformed = transform_staff_details(glue_context, spark, df_preliminary)
        case "PRODUCT_BRAND":
            df_transformed = transform_product_brand(glue_context, spark, df_preliminary)
        case "PRODUCT_CATEGORY":
            df_transformed = transform_product_category(glue_context, spark, df_preliminary)
        case "PRODUCT_COUNTRY":
            df_transformed = transform_product_country(glue_context, spark, df_preliminary)
        case "PRODUCT_DETAILS":
            df_transformed = transform_product_details(glue_context, spark, df_preliminary)
        case "PRODUCT_SECONDARY_CATEGORY":
            df_transformed = transform_product_secondary_category(glue_context, spark, df_preliminary)
        case "CUSTOMER_PURCHASE_INSTANCE_ITEMS":
            df_transformed = transform_customer_purchase_instance_items(glue_context, spark, df_preliminary)
        case "CUSTOMER_CART_ITEMS":
            df_transformed = transform_customer_cart_items(glue_context, spark, df_preliminary)

    # GENERIC_POST_1: Add common preliminary columns
    df_final = generic_add_prelim_columns(df_transformed, file_name)
    print(f"[{table_name}_GENERIC_POST_1] Added common preliminary columns.")

    # GENERIC_POST_2: Convert null value to string to prevent "Can't get JDBC type for void" exceptions
    df_final = generic_cast_voids(df_final)
    print(f"[{table_name}_GENERIC_POST_2] Converted void columns to StringType.")

    dyf_final = DynamicFrame.fromDF(
        dataframe=df_final,
        glue_ctx=glue_context,
        name="dyf_final"
    )

    return dyf_final
