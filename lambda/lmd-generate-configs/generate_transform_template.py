BASE_TEMPLATE = """from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, when, lit, trim, split, explode, sum, row_number
from awsglue.transforms import Filter, ApplyMapping
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
import gs_derived

VALID_TABLE_NAMES = [{_TABLE_NAMES_STRINGS}]

PRELIM_HASH_TUPLE_MAPPING = (
    admw_glue_common_vars.COLUMN_NAME_OG_FILE_HASH, "string", admw_glue_common_vars.COLUMN_NAME_FILE_HASH, "string"
)

def generic_map_columns(dyf_table, list_mappings):
    \"\"\"
    Apply preliminary table column name mappings to a DynamicFrame.

    :param DynamicFrame dyf_table: Input DynamicFrame.
    :param tuple list_mappings: List of lists containing column mappings.

    :returns: DynamicFrame
    \"\"\"

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
    \"\"\"
    Add common preliminary columns present in every preliminary table to a SparkFrame.

    :param SparkFrame df_table: Input SparkFrame.
    :param str file_name: Name of data file being processed in this Glue Job.

    :returns: SparkFrame
    \"\"\"

    df_table = df_table.withColumn("pre_admw_origin_file_name", lit(str(file_name)))

    return df_table


def generic_cast_voids(df_table):
    \"\"\"
    | Convert void columns to StringType to prevent "Can't get JDBC type for void" exceptions.
    | Occurs when all rows of a column are null, causing .dtypes for it to be ('column_name', 'void')

    :param SparkFrame df_table: Input Sparkframe.

    :returns: SparkFrame
    \"\"\"
    

    void_cols = [col_name for col_name, col_type in df_table.dtypes if col_type == "void"]
    if not void_cols:
        return df_table

    for col_name in void_cols:
        df_cast_table = df_table.withColumn(col_name, df_table[col_name].cast(StringType()))

    return df_cast_table

    
{_TRANSFORMS}def transform_table(glue_context, spark, table_name, file_name, dyf_table, list_mappings, other_info):
    \"\"\"
    | Performs transformation on a table based on its name.
    | After delegating to the appropriate transformation function, the following generic transformations are applied:
    - GENERIC_PRE_1: Convert column names to specified prelimary names.
    - GENERIC_POST_1: Add common preliminary columns
    - GENERIC_POST_2: Convert void columns to StringType to prevent "Can't get JDBC type for void" exceptions.

    :param GlueContext glue_context: GlueContext object.
    :param SparkContext spark: SparkContext object.
    :param str table_name: Table name to transform.
    :param DynamicFrame dyf_table: Input DynamicFrame.
    :param dict list_mappings: list of lists containing column mappings. 
    :param dict other_info: Other information needed for transformation.

    :returns: DynamicFrame
    \"\"\"

    if table_name not in VALID_TABLE_NAMES:
        raise ValueError(f"Table {{table_name}} not recognised!")

    print(f"Performing transformation(s) on table {{table_name}}.")

    # GENERIC_PRE_1: Apply datatype mappings
    dyf_preliminary = generic_map_columns(dyf_table, list_mappings)
    print(f"[{{table_name}}_GENERIC_PRE_1] Applied datatype mappings according to specification in configuration file.")

    df_preliminary = dyf_preliminary.toDF()

    match(table_name):
{_SWITCH_CASES}
    # GENERIC_POST_1: Add common preliminary columns
    df_final = generic_add_prelim_columns(df_transformed, file_name)
    print(f"[{{table_name}}_GENERIC_POST_1] Added common preliminary columns.")

    # GENERIC_POST_2: Convert null value to string to prevent "Can't get JDBC type for void" exceptions
    df_final = generic_cast_voids(df_final)
    print(f"[{{table_name}}_GENERIC_POST_2] Converted void columns to StringType.")

    dyf_final = DynamicFrame.fromDF(
        dataframe=df_final,
        glue_ctx=glue_context,
        name="dyf_final"
    )

    return dyf_final
"""

TRANSFORM_ONE_TEMPLATE = f"""def transform_{{_TABLE_NAME_LOWER}}(glue_context, spark, df_table):
    \"\"\"
    | Transformation for table {{_TABLE_NAME_UPPER}}.
    | Performs:
    - (TODO add transformations, if any)

    :param GlueContext glue_context: GlueContext object.
    :param SparkContext spark: SparkContext object.
    :param SparkFrame df_table: {{_TABLE_NAME_UPPER}} SparkFrame.

    :returns: SparkFrame
    \"\"\"

    return df_table


"""

SWITCH_TEMPLATE = f"""\
        case "{{_TABLE_NAME_UPPER}}":
            df_transformed = transform_{{_TABLE_NAME_LOWER}}(glue_context, spark, df_preliminary)
"""


def generate_template(table_names: list) -> str:
    """
    | Initialises the admw_glue_transforms.py template.

    :param list table_names: List of table names.

    :returns: string
    """

    table_names_string = ", ".join([f"\"{table_name}\"" for table_name in table_names])

    individual_transforms = []
    for table_name in table_names:
        individual_transforms.append(TRANSFORM_ONE_TEMPLATE.format(_TABLE_NAME_LOWER=table_name.lower(), _TABLE_NAME_UPPER=table_name))

    switch_cases = []
    for table_name in table_names:
        switch_cases.append(SWITCH_TEMPLATE.format(_TABLE_NAME_LOWER=table_name.lower(), _TABLE_NAME_UPPER=table_name))

    return BASE_TEMPLATE.format(
        _TABLE_NAMES_STRINGS=table_names_string,
        _TRANSFORMS="".join(individual_transforms),
        _SWITCH_CASES="".join(switch_cases),
        table_name="table_name"
    )
