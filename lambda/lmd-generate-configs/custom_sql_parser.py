import sqlparse
import re


class SQL_PARSER:
    """
    Class to parse a CREATE TABLE statement. Produces a few outputs for use:
    Produces a preliminary CREATE TABLE statement with table names prefixed with **PRE_** and columns prefixed with **pre_**.
    Produces a dictionary of table dependencies for use in ordering tables for insertion.


    Exposed methods:
    - **get_table_name():** returns final table name.
    - **get_preliminary_create_statement(has_constraints=True):** returns preliminary CREATE TABLE statement.
    - **get_final_create_statement():** returns final CREATE TABLE statement.
    - **get_table_dependencies():** returns a dictionary of table dependencies.
    - **get_datatype_mappings():** returns a list of tuples usable by AWS Glue to map datatypes.
    - **get_primary_keys():** returns a list of primary key column names.
    - **get_column_names():** returns a list of column names.
    """

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    class COLUMN_DECLARATION:
        """
        Simple class that stores column name and column type of a mysql column declaration.

        Accessible attributes:
        - **column_name:** name of the column.
        - **column_type:** datatype of the column. includes not null if applicable.
        """

        def __init__(self, column_name, column_type):
            self.column_name = column_name
            self.column_type = column_type

        def __str__(self):
            return f"column_name: {self.column_name}\ncolumn_type: {self.column_type}"

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    class CONSTRAINT_DECLARATION:
        """
        Simple class that stores information of a mysql constraint. (primary key or foreign key)

        Accessible attributes:
        - **constraint_type** (str): type of constraint. (CONSTRAINT_DECLARATION.TYPE_PK or CONSTRAINT_DECLARATION.TYPE_FK)
        - **column_names** (list[str]): list of column(s) of the constraint. contents are values between the first set of brackets. \
            list length is greater than 1 only if it is a composite key.
        - **referenced_table** (str): name of the table that the foreign key references. only populated if it is a foreign key.
        - **referenced_column_names** (list[str]): list of column name(s) in the referenced table. contents are values between the second set of brackets. \
            list length is greater than 1 only if it is a composite key. only populated if it is a foreign key.
        - **on_delete** (str): on delete action of the foreign key. defaults to "NO ACTION" if not specified. only populated if it is a foreign key.
        """

        # c enums shall never die
        TYPE_PK = 1
        TYPE_FK = 2

        def __init__(self, constraint_type, column_names, referenced_table, referenced_column_names, on_delete):
            self.constraint_type = constraint_type
            self.column_names = column_names
            self.referenced_table = referenced_table
            self.referenced_column_names = referenced_column_names
            self.on_delete = on_delete

        def __str__(self):
            return f"constraint_type: {self.constraint_type} \ncolumn_names: {self.column_names} \n\
referenced_table: {self.referenced_table} \nreferenced_column_names: {self.referenced_column_names}\non_delete: {self.on_delete}"
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def __init__(self, create_statement, schema_name, prelim_table_prefix, prelim_column_prefix):
        self._final_create_statement_original = create_statement
        self.schema_name = schema_name
        self.prelim_table_prefix = prelim_table_prefix
        self.prelim_column_prefix = prelim_column_prefix

        self._preliminary_table_name = None
        self._final_table_name = None
        self._preliminary_create_statement = None
        self._final_create_statement = None

        self._columns = []
        self._constraints = []
        self._preliminary_columns = []
        self._preliminary_constraints = []
        self._constraint_dict = {}
        self._datatype_mappings_prelim = []
        self._datatype_mappings_final = []

        self._generate_final_metadata()
        self._generate_preliminary_metadata()
        self._generate_preliminary_create_statement(has_constraints=True)
        self._generate_final_create_statement()
        self._generate_glue_apply_mapping_datatypes()

    def _generate_final_metadata(self):
        """
        Internal method to parse the provided final CREATE TABLE statement.
        Stores table name, columns, and constraint declarations.
        """

        tokenised_create_statement = [token for token in sqlparse.parse(self._final_create_statement_original)[0].tokens]

        # check if token count matches a standard create table statement's count
        if len(tokenised_create_statement) != 8:
            raise ValueError("Not a valid CREATE TABLE statement!")
        if "".join([str(token) for token in tokenised_create_statement[0:3]]) != "CREATE TABLE":
            raise ValueError("Not a CREATE TABLE statement!")

        self._final_table_name = str(tokenised_create_statement[4]).strip("`")
        self._constraint_dict = {"name": self._final_table_name, "references": []}

        pattern_split_on_outer_comma = re.compile(r",\s*(?![^()]*\))")
        # [6] is the meat of the create statement, everything between the first and last brackets inclusive
        table_definition_list = pattern_split_on_outer_comma.split(
            str(tokenised_create_statement[6])[1:-1])  # parse string with outer-most brackets removed
        for line in table_definition_list:
            line = line.strip()

            # normal column declaration
            if all(keyword not in line for keyword in ["PRIMARY KEY", "FOREIGN KEY"]):
                column_name = line.split("`")[1].split("`")[0]
                column_type = line.split("`", 2)[2].strip()

                self._columns.append(SQL_PARSER.COLUMN_DECLARATION(column_name, column_type))

            # pk
            elif "PRIMARY KEY" in line:
                primary_key_columns = [pk_column.strip(" `") for pk_column in line.split("(")[1].split(")")[0].split(",")]

                self._constraints.append(SQL_PARSER.CONSTRAINT_DECLARATION(
                    constraint_type=SQL_PARSER.CONSTRAINT_DECLARATION.TYPE_PK,
                    column_names=primary_key_columns,
                    referenced_table=None,
                    referenced_column_names=None,
                    on_delete=None
                ))

            # fk
            elif "FOREIGN KEY" in line:
                # between first set of ()
                foreign_key_columns = [fk_column.strip(" `") for fk_column in line.split("(")[1].split(")")[0].split(",")]
                # between second set of ()
                referenced_columns = [refed_column.strip(" `") for refed_column in line.split("(")[2].split(")")[0].split(",")]
                # between )``(
                referenced_table = line.split(")")[1].split("(")[0].split("`")[1].split("`")[0]
                # the last word is the on delete action, if it exists
                delete_pattern = re.compile(r"\bON DELETE (\w+(?: \w+)*)\b")
                match = delete_pattern.search(line)
                if match:
                    on_delete = match.group(1)
                else:
                    on_delete = "NO ACTION"

                self._constraints.append(SQL_PARSER.CONSTRAINT_DECLARATION(
                    constraint_type=SQL_PARSER.CONSTRAINT_DECLARATION.TYPE_FK,
                    column_names=foreign_key_columns,
                    referenced_table=referenced_table,
                    referenced_column_names=referenced_columns,
                    on_delete=on_delete
                ))

                self._constraint_dict["references"].append(referenced_table)

    def _generate_preliminary_metadata(self):
        """
        Internal method to parse the final CREATE TABLE statement to generate preliminary table metadata.
        Stores (preliminary) table name, columns, and constraint declarations.
        """

        self._preliminary_table_name = f"{self.prelim_table_prefix}{self._final_table_name}"

        for column in self._columns:
            self._preliminary_columns.append(SQL_PARSER.COLUMN_DECLARATION(
                f"{self.prelim_column_prefix}{column.column_name}", column.column_type))

        for column in self._constraints:
            if column.constraint_type == SQL_PARSER.CONSTRAINT_DECLARATION.TYPE_PK:
                self._preliminary_constraints.append(SQL_PARSER.CONSTRAINT_DECLARATION(
                    constraint_type=column.constraint_type,
                    column_names=[f"{self.prelim_column_prefix}{x}" for x in column.column_names],
                    referenced_table=None,
                    referenced_column_names=None,
                    on_delete=None
                ))

            elif column.constraint_type == SQL_PARSER.CONSTRAINT_DECLARATION.TYPE_FK:
                self._preliminary_constraints.append(SQL_PARSER.CONSTRAINT_DECLARATION(
                    constraint_type=column.constraint_type,
                    column_names=[f"{self.prelim_column_prefix}{x}" for x in column.column_names],
                    referenced_table=f"{self.prelim_table_prefix}{column.referenced_table}",
                    referenced_column_names=[f"{self.prelim_column_prefix}{x}" for x in column.referenced_column_names],
                    on_delete=column.on_delete
                ))

    def _generate_preliminary_create_statement(self, has_constraints):
        """
        Internal method to create a new preliminary CREATE TABLE statement.
        Must be called after _generate_preliminary_metadata().

        :param bool: has_constraints: Whether to include constraints in the statement.
        """

        prelim_columns_with_metadata_columns = self._preliminary_columns.copy()
        prelim_columns_with_metadata_columns.append(SQL_PARSER.COLUMN_DECLARATION(
            f"{self.prelim_column_prefix}admw_file_hash", "CHAR(64)"))
        prelim_columns_with_metadata_columns.append(SQL_PARSER.COLUMN_DECLARATION(
            f"{self.prelim_column_prefix}admw_origin_file_name", "VARCHAR(128) NOT NULL"))

        columns = [f"`{column.column_name}` {column.column_type}" for column in prelim_columns_with_metadata_columns]

        primary_keys = [", ".join([f"`{column_names}`"for column_names in column.column_names])
                        for column in self._preliminary_constraints
                        if column.constraint_type == SQL_PARSER.CONSTRAINT_DECLARATION.TYPE_PK]
        primary_keys.append(f"`{self.prelim_column_prefix}admw_origin_file_name`")

        foreign_keys = [f"""FOREIGN KEY ({", ".join([f"`{column_names}`" for column_names in column.column_names])}) \
REFERENCES `{column.referenced_table}` \
({", ".join([f"`{ref_column_names}`" for ref_column_names in column.referenced_column_names])}) ON DELETE {column.on_delete}"""
            for column in self._preliminary_constraints
            if column.constraint_type == SQL_PARSER.CONSTRAINT_DECLARATION.TYPE_FK]

        self._preliminary_create_statement = f"""CREATE TABLE `{self.schema_name}`.`{self._preliminary_table_name}` (\
{", ".join(columns)}\
{f", PRIMARY KEY ({', '.join(primary_keys)})" if primary_keys else ""}\
{f", {', '.join(foreign_keys)}" if foreign_keys and has_constraints else ""}\
);"""

    def _generate_final_create_statement(self):
        """
        Internal method to create a new final CREATE TABLE statement.
        Must be called after _generate_final_metadata().
        """

        final_columns_with_metadata_columns = self._columns.copy()
        final_columns_with_metadata_columns.append(SQL_PARSER.COLUMN_DECLARATION(f"admw_origin_file_name", "VARCHAR(128) NOT NULL"))

        columns = [f"`{column.column_name}` {column.column_type}" for column in final_columns_with_metadata_columns]

        primary_keys = [", ".join([f"`{column_names}`"for column_names in column.column_names])
                        for column in self._constraints
                        if column.constraint_type == SQL_PARSER.CONSTRAINT_DECLARATION.TYPE_PK]

        foreign_keys = [f"""FOREIGN KEY ({", ".join([f"`{column_names}`" for column_names in column.column_names])}) \
REFERENCES `{column.referenced_table}` \
({", ".join([f"`{ref_column_names}`" for ref_column_names in column.referenced_column_names])}) ON DELETE {column.on_delete}"""
            for column in self._constraints
            if column.constraint_type == SQL_PARSER.CONSTRAINT_DECLARATION.TYPE_FK]

        self._final_create_statement = f"""CREATE TABLE `{self.schema_name}`.`{self._final_table_name}` (\
{", ".join(columns)}\
{f", PRIMARY KEY ({', '.join(primary_keys)})" if primary_keys else ""}\
{f", {', '.join(foreign_keys)}" if foreign_keys else ""}\
);"""

    def _generate_glue_apply_mapping_datatypes(self):
        """
        Generates a list of tuples that can be used by AWS Glue to map datatypes.
        Tuples are 6 values long, and contain data (og name, "string", prelim name, "string", final name, derived datatype).

        Implemented datatype mappings:
        - boolean: BOOL
        - string: VARCHAR(?), CHAR
        - int: INT, SMALLINT
        - decimal: DECIMAL(?,?)
        - timestamp: DATETIME(?)
        - date: DATE
        """

        mappings = {
            "BOOL": "boolean",
            "VARCHAR": "string",
            "CHAR": "string",
            "INT": "int",
            "SMALLINT": "int",
            "DECIMAL": "decimal",
            "DATETIME": "timestamp",
            "DATE": "date"
        }

        # locates first occurrence of a datatype in a string when used with re.search, returns None if not found
        pattern = re.compile(fr"\b(?:{"|".join(mappings.keys())})\b")

        for column in self._columns:
            match = pattern.findall(column.column_type)
            if not match:
                raise ValueError(f"No valid datatype found in '{column.column_type}'!")
            if len(match) > 1:
                raise ValueError(f"Multiple datatypes found in '{column.column_type}'!")

            # og name, string, prelim name, string, final name, real datatype
            self._datatype_mappings_prelim.append(
                (column.column_name, "string", f"{self.prelim_column_prefix}{column.column_name}", "string")
            )

            # prelim name, real datatype, final name, real datatype
            self._datatype_mappings_final.append(
                (f"{self.prelim_column_prefix}{column.column_name}", mappings[match[0]], column.column_name, mappings[match[0]])
            )

    def get_table_name(self):
        """
        Returns final table name.

        :returns: string
        """

        return self._final_table_name

    def get_preliminary_create_statement(self, has_constraints=True):
        """
        Returns preliminary CREATE TABLE statement.

        :param has_constraints: boolean specifying whether to include constraints in the statement. defaults to True.

        :returns: string
        """

        self._generate_preliminary_create_statement(has_constraints)
        return self._preliminary_create_statement

    def get_final_create_statement(self):
        """
        Returns final CREATE TABLE statement.

        :returns: string
        """
        return self._final_create_statement

    def get_table_dependencies(self):
        """
        Returns a dictionary of table dependencies.

        :returns: dictionary - {
            "name": string,
            "references": list[str]
        }
        """

        return self._constraint_dict

    def get_datatype_mappings_prelim(self):
        """
        Returns a list of tuples usable by AWS Glue to map datatypes from datafile to table.

        :returns: list[tuple] - [
            (og name, "string", prelim name, "string", final name, derived datatype), ...
        ]
        """

        return self._datatype_mappings_prelim

    def get_datatype_mappings_final(self):
        """
        Returns a list of tuples usable by AWS Glue to map datatypes from table to table.

        :returns: list[tuple] - [
            (og name, "string", prelim name, "string", final name, derived datatype), ...
        ]
        """

        return self._datatype_mappings_final

    def get_primary_keys(self):
        """
        Returns a list of primary key column names.
        List can have a length greater than 1 if it is a composite key.

        :returns: list[str]
        """

        for constraint in self._constraints:
            if constraint.constraint_type == SQL_PARSER.CONSTRAINT_DECLARATION.TYPE_PK:
                return constraint.column_names

    def get_column_names(self):
        """
        Returns a list of column names. 

        :returns: list[str]
        """

        return [column.column_name for column in self._columns]
