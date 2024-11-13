BASE_TEMPLATE = """from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, when, lit, trim, split, explode, expr, sum, row_number
from awsglue.transforms import Filter, ApplyMapping
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
import admw_glue_transforms
import admw_glue_common_vars
import gs_derived

HASH_EXPR = "UPPER(SHA2(CONCAT({{0}}), 256))"
    
{_HASHES}def populate_hash_column(glue_context, spark, table_name, df_table):
    \"\"\"
    | Adds a hash column to a table based on its name.

    :param GlueContext glue_context: glue context object.
    :param SparkSession spark: spark session object.
    :param string table_name: name of the table to hash.
    :param Spark DataFrame df_table: DataFrame containing the table to hash.

    :returns: Spark DataFrame
    \"\"\"

    if table_name not in admw_glue_transforms.VALID_TABLE_NAMES:
        raise ValueError(f"Table {{table_name}} not recognised!")

    print(f"Adding hash column to table {{table_name}}.")

    match(table_name):
{_SWITCH_CASES}

    return df_table_w_prelim_hash
"""

HASH_ONE_TEMPLATE = """def hash_{_TABLE_NAME_LOWER}(glue_context, spark, df_table):
    \"\"\"
    | Adds hash column for for table {_TABLE_NAME_UPPER}.
    | Performs:
    - SHA256 checksum computation of concatenation of all hashable columns.
    - (TODO add any additional required formatting)

    :param GlueContext glue_context: glue context object.
    :param SparkSession spark: spark session object.
    :param DataFrame df_table: DataFrame containing the table {_TABLE_NAME_UPPER}.

    :returns: Spark DataFrame
    \"\"\"

    hashable_columns = [{_HASHABLE_COLUMNS}]

    formatted_hashable_columns = []
    for hash_column in hashable_columns:
        formatted_hashable_columns.append(hash_column)

    final_hash_expr = admw_glue_common_vars.ReconDefs.HASH_EXPR.format(",".join(formatted_hashable_columns))

    df_transformed = df_table.withColumn(admw_glue_common_vars.COLUMN_NAME_PRELIM_HASH, expr(final_hash_expr))
    print(f"[HASH_{_TABLE_NAME_UPPER}] Hash column added to table.")

    return df_transformed


"""

SWITCH_TEMPLATE = f"""\
        case "{{_TABLE_NAME_UPPER}}":
            df_table_w_prelim_hash = hash_{{_TABLE_NAME_LOWER}}(glue_context, spark, df_table)
"""


def generate_template(table_to_hash_cols_dict: dict) -> str:
    """
    | Initialises the admw_glue_prelim_hashing.py template.

    :param dict table_to_hash_cols_dict: Dictionary of table names mapped to list of final columns.

    :returns: string
    """

    individual_hash_func_strings = []
    for table_name, hash_cols in table_to_hash_cols_dict.items():
        hash_cols_string = ", ".join([f"\"pre_{hash_col}\"" for hash_col in hash_cols])

        individual_hash_func_strings.append(
            HASH_ONE_TEMPLATE.format(
                _TABLE_NAME_LOWER=table_name.lower(),
                _TABLE_NAME_UPPER=table_name,
                _HASHABLE_COLUMNS=hash_cols_string,
                hash_col="hash_col")
        )

    switch_cases = []
    for table_name in table_to_hash_cols_dict.keys():
        switch_cases.append(SWITCH_TEMPLATE.format(_TABLE_NAME_LOWER=table_name.lower(), _TABLE_NAME_UPPER=table_name))

    return BASE_TEMPLATE.format(
        _HASHES="".join(individual_hash_func_strings),
        _SWITCH_CASES="".join(switch_cases),
        table_name="table_name"
    )
