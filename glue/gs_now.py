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

from gs_common import enrich_df, DateTimeTemplate, py2j_date_format
import pyspark.sql.functions as F


def now(self, colName="timestamp", dateFormat=None):
    new_col = F.current_timestamp()
    if dateFormat:
        new_col = F.date_format(new_col, DateTimeTemplate(dateFormat).substitute(**py2j_date_format))
    return self.withColumn(colName, new_col)


enrich_df('gs_now', now)
