# Copyright 2016-2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# Licensed under the Amazon Software License (the "License"). You may not use
# this file except in compliance with the License. A copy of the License is
# located at
#
#  http://aws.amazon.com/asl/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express
# or implied. See the License for the specific language governing
# permissions and limitations under the License.

import re
from string import Template

from awsglue import DynamicFrame
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from typing import Union


class DateTimeTemplate(Template):
    delimiter = '%'


# Define mappings between Python dateformat and Java formats for each component, doesn't cover the locale dependent problematic ones
py2j_date_format = {'Y': 'yyyy', 'm': 'MM', 'd': 'dd', 'H': 'HH', 'M': 'mm',
                    'S': 'ss', 'a': 'E', 'A': 'EEEE', 'b': 'MMM', 'B': 'MMMM', 'y': 'yy',
                    'I': 'hh', 'p': 'a', 'f': 'SSSSSS', 'z': 'Z', 'Z': 'z', 'j': 'DDD',
                    'c': 'EEE MMM d HH:mm:ss yyyy', 'X': 'HH:MM:ss', 'G': 'yyyy'}


def split_string_list(slist):
    """
    Split a string with list of elements and return a list with those elements
    :param slist: string with tokens separated by commas
    :return: list of strings after the split, stripped of extra whitespace around it
    """
    return [x.strip() for x in slist.split(',')]


def get_logger():
    return pyspark.SparkContext.getOrCreate()._jvm.com.amazonaws.services.glue.log.GlueLogger()


def get_col_type(df, colName):
    if type(df) == DynamicFrame:
        df = df.toDF()
    return df.select(colName).dtypes[0][1]


def has_choices(dynf):
    def inner(fields):
        from awsglue.gluetypes import ChoiceType, StructType
        for field in fields:
            field_type = field.dataType.__class__.__name__
            if field_type == ChoiceType.__name__:
                return True
            elif field_type == StructType.__name__:
                if inner(field.dataType.fields):
                    return True
        return False

    return inner(dynf.schema())


def is_blank_df(df):
    """
    Indicates if the DataFrame has no schema and no rows
    """
    return not df.schema.fieldNames() and not df.take(1)


def get_main_script_globals():
    import inspect
    stack = inspect.stack()
    return dict(inspect.getmembers(stack[len(stack) - 1][0]))["f_globals"]


def get_glue_ctx():
    """
    This is used when we need the GlueContext but don't have it.
    The GlueContext is neither a singleton not meant to run multiple instances
    We will take it from the main script or request to be added there.
    :return: the glueContext used on the main script
    """
    from awsglue.context import GlueContext
    main_globals = get_main_script_globals()
    for value in main_globals.values():
        if type(value) == GlueContext:
            return value
    raise RuntimeError("This transform needs to be invoked after a GlueContext has been defined")


def enrich_df(name, function, useDynf=False):
    """
    Register the function on DataFrame and DynamicFrame under the name specified,
     converting between DataFrame and DynamicFrame as needed so the same function is shared
    :param name: Name of the method that will invoke the function
    :param function: function with the transformation logic to receive a return a DataFrame
    :param useDynf: if this flag is set, the function specified expects and returns a DynamicFrame
    """
    def transform_dynf(self, **kwargs):
        if has_choices(self):
            # Converting to DataFrame and back with choices is not symmetric, choices become structs, prevent this
            raise ValueError("Please resolve the pending DynamicFrame choices before using this transformation")
        df = self.toDF()
        if is_blank_df(df):
            return self  # No data to transform, return as is
        if useDynf:
            return function(self, **kwargs)
        # Shim for PySpark < 3.3
        if 'sparkSession' not in dir(df):
            df.sparkSession = self.glue_ctx.spark_session
        # Convert, apply and convert back
        return DynamicFrame.fromDF(function(df, **kwargs), self.glue_ctx, self.name)

    def transform_df(self, **kwargs):
        if is_blank_df(self):
            return self  # No data to transform, return as is
        if useDynf:
            dynf = DynamicFrame.fromDF(self, get_glue_ctx(), "")
            return function(dynf, **kwargs).toDF()
        return function(self, **kwargs)

    setattr(DataFrame, name, transform_df)
    setattr(DynamicFrame, name, transform_dynf)


def get_spark_session(dataframe):
    if 'sparkSession' in dir(dataframe):
        return dataframe.sparkSession
    else:
        return SparkSession.builder.getOrCreate()


def escape_name(name):
    """
    Escape a name so it can be used in SparkSQL
    :param name: name of database, table or columns
    :return: escaped name
    """
    return f"`{name.replace('`', '``')}`" if name else name


def resolve_col(df: DataFrame, name: str) -> str:
    return df.select(name).columns[0]


def resolve_cols(df: DataFrame, names: Union[list, tuple]) -> list:
    return df.select(*names).columns
