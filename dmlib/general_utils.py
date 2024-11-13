import zipfile
import re
import json
from pathlib import Path


class LAMBDA_STRINGS:
    LAMBDA_START_MSG = "Lambda Function {0} started."
    LAMBDA_SUCCESS_MSG = "Lambda Function {0} completed."
    LAMBDA_FAILED_MSG = "Lambda {0} execution failed, check the logs for more details."

    LAMBDA_BOTO_ERROR = "BotoError"
    LAMBDA_SQL_ERROR = "MySQLError"


def general_decompressor(file_path: str, extract_to: str = None) -> bool:
    """
    | Extracts contents of a compressed binary to a specified location.
    | Returns False if file extension is not supported, otherwise True.
    | Currently supports: .zip.

    :param string file_path: Compressed file name.
    :param string extract_to: Where file is extracted to, defaults to working directory.

    :returns: boolean
    """

    file_ext = Path(file_path).suffix

    match file_ext.lower():
        case ".zip":
            with zipfile.ZipFile(Path(file_path), "r") as zip_ref:
                zip_ref.extractall() if not extract_to else zip_ref.extractall(Path(extract_to))
        case _:
            return False

    return True


def find_first_file_with_ext(extension: str, file_path: str = "./") -> str:
    """
    | Looks in file_path for FIRST occurence of a file with a specified extension.
    | Defaults to working directory if no path is specified.

    :param string extension: Extension string with period included, for example '.py'.
    :param string file_path: Path to start searching in.

    :returns: string
    """

    p = Path(file_path).glob("**/*")
    files = [x for x in p if x.is_file()]

    for file in files:
        if file.suffix == extension:
            return file


def format_lambda_exception(lambda_name: str, error_type: str, exception_cause: str) -> str:
    """
    | For use in lambda functions when an exception has to be thrown. 
    | Formats for use by catch lambda function in a step function, which requires specific perimeters to ensure a graceful pipeline abortion.

    :param string lambda_name: Name of this lambda function (can't access name, it's always lambda_handler)
    :param string error_type: Main exception cause.
    :param string exception_cause: Reason for exception.

    :returns: string, dictionary json.dump-ed - {
        'error_type': string,
        'exception_cause': string,
        'lambda_name': string
    }
    """

    return_dict = {
        "error_type": error_type,
        "exception_cause": exception_cause,
        "lambda_name": lambda_name
    }

    return json.dumps(return_dict)


def decompose_file_name(file_name: str) -> dict:
    """
    | Given a data file name which follows defined naming conventions, returns a dictionary of metadata extracted from the file name.
    | If the file name does not follow the naming convention, an empty dictionary is returned.

    :param string file_name: file name to decompose.

    :returns: dictionary - {
        'file_name': string,
        'table_name': string,
        'file_datetime': string
    } 
    or 
    dictionary - {
    }
    """

    date_pattern = r"^\d+\.\d+$"

    file_metadata = {}
    table_name_and_others_split = Path(file_name).stem.split("~")
    if len(table_name_and_others_split) != 2:
        return {}

    if not re.match(date_pattern, table_name_and_others_split[1]):
        return {}

    file_metadata["file_name"] = file_name
    file_metadata["table_name"] = table_name_and_others_split[0]
    file_metadata["file_datetime"] = table_name_and_others_split[1]

    return file_metadata
