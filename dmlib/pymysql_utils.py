import dmlib.general_utils as general_utils
import pymysql
from typing import Union

BOTO3_UTILS_GET_MYSQL_CONN_DETAILS_PASSWORD_ARGS = ["glue_connector_name"] # "schema_name", "ssl" is optional


def get_sql_time_now() -> str:
    """
    | Function that returns SQL command syntax that provides Singapore adjusted GMT time.

    :returns: string
    """
    
    return "CONVERT_TZ(NOW(),'SYSTEM','Asia/Singapore')"
    # return "NOW()"  # if servers are local


def get_conn_object(conn_details: dict) -> pymysql.connections.Connection:
    """
    | Returns a pymysql connection object based on the conn_details dictionary. IAM token and password compatible.

    :param dict conn_details: dictionary produced by boto3_utils.get_mysql_conn_details_password().

    :returns: pymysql connection object
    """

    conn = pymysql.connect(
        host=conn_details["hostname"],
        user=conn_details["username"],
        passwd=conn_details["password"],
        database=conn_details["schema"],
        connect_timeout=general_utils.DEFAULTS.DB_CONNECT_TIMEOUT,
        port=int(conn_details["port"])
    )

    return conn


def safe_select(conn_details: dict, query: str, task: Union[tuple, None], get_type: str) -> dict:
    """
    | SELECT MYSQL Wrapper.

    :param dict conn_details: dictionary produced by boto3_utils.get_mysql_conn_details_password().
    :param string query: query string
    :param tuple/None task: tuple containing values for substitution into prepared statement, or None
    :param string get_type: 'one' or 'all'

    :returns: dictionary - {
        'select_successful': boolean,
        'num_rows' - iff 'select_successful' == True: int - number of rows returned.
        'content' - iff 'select_successful' == True: list - select output. may be empty.
        'exception' - iff 'select_successful' == False: string - sql exception in string format.
    }
    """

    try:
        conn = get_conn_object(conn_details)
        cursor = conn.cursor()

        if task:
            cursor.execute(query, task)
        else:
            cursor.execute(query)

        if get_type == "one":
            content = cursor.fetchone()
            return_dict = {
                "select_successful": True,
                "num_rows": 0 if content is None else 1,
                "content": content
            }

        elif get_type == "all":
            content = cursor.fetchall()
            return_dict = {
                "select_successful": True,
                "num_rows": len(content),
                "content": content
            }

        else:
            return_dict = {"select_successful": False, "exception": "Wrongly specified get_type."}

    except pymysql.Error as e:
        exception_str = "Caught MySQL Error %d: %s" % (e.args[0], e.args[1])

        return_dict = {"select_successful": False, "exception": exception_str}

    except Exception as e:
        exception_str = f"Caught Exception: {e}"

        return_dict = {"select_successful": False, "exception": exception_str}

    finally:
        if "cursor" in locals() and cursor:
            cursor.close()
        if "conn" in locals() and conn:
            conn.close()

        return return_dict


def safe_transaction(conn_details: dict, query: str, task: Union[tuple, None]) -> dict:
    """
    | CREATE, UPDATE, DELETE MySQL Wrapper.

    :param dict conn_details: dictionary produced by boto3_utils.get_mysql_conn_details_password().
    :param string query: query string
    :param tuple/bool task: tuple containing fields for prepared statement, or None

    :returns: dictionary - {
        'transaction_successful': boolean,
        'rows_affected' - iff 'transaction_successful' == True: int - number of rows affected.
        'exception' - iff 'transaction_successful' == False: string - sql exception in string format.
    }
    """

    try:
        conn = get_conn_object(conn_details)
        cursor = conn.cursor()

        if task:
            cursor.execute(query, task)
        else:
            cursor.execute(query)

        conn.commit()

        return_dict = {
            "transaction_successful": True,
            "rows_affected": cursor.rowcount
        }

    except pymysql.Error as e:
        exception_str = "Caught MySQL Error %d: %s" % (e.args[0], e.args[1])
        print(exception_str)

        return_dict = {"transaction_successful": False, "exception": exception_str}

    except Exception as e:
        exception_str = f"Caught Exception: {e}"
        print(exception_str)

        return_dict = {"select_successful": False, "exception": exception_str}

    finally:
        if "cursor" in locals() and cursor:
            cursor.close()
        if "conn" in locals() and conn:
            conn.close()

        return return_dict
