import boto3
import re
import os
import datetime
import math
import lzma
from pathlib import Path

_S3_GET_CHUNK_SIZE = 8388608  # 8MB
_MULTIPART_MIN_CHUNK_SIZE = 67108864  # 64MB


class S3_UPLOAD:
    """
    | Helper function which supports uploading of objects. Also allows for displaying of upload progress in 10% increments.
    | Functionality is exposed through the 'upload()' method.

    :param bool: show_progress: Whether progress should be printed.
    """

    def __init__(self, show_progress: bool):
        self.show_progress = show_progress
        self.total = 0
        self.uploaded = 0
        self.displayed_progress_percent = -1
        self.s3 = boto3.client("s3")

    def _upload_callback(self, size: int):
        """
        | Callback function for upload progress. 
        | Prints progress in 10% increments.
        | As per boto3 documentation, the callback function takes a number of bytes transferred to be periodically called during the upload.

        :param int size: Number of bytes transferred.
        """

        if self.total == 0:
            return
        self.uploaded += size
        current_percent = int(self.uploaded / self.total * 100)
        if current_percent % 10 == 0 and self.displayed_progress_percent != current_percent:
            print(f"Uploading: {current_percent}% ({(self.uploaded / 1048576):.2f}MB / {(self.total / 1048576):.2f}MB)")
            self.displayed_progress_percent = current_percent

    def upload(self, source_file_path: str, dest_bucket_name: str, dest_key_name: str):
        """
        | Uploads a file to an S3 bucket.

        :param string source_file_path: Local path to the file to be uploaded.
        :param string dest_bucket_name: S3 destination bucket name, without s3:// prefix.
        :param string dest_key_name: S3 destination key.
        """

        self.total = os.stat(Path(source_file_path)).st_size

        self.s3.upload_file(
            Filename=source_file_path,
            Bucket=dest_bucket_name,
            Key=dest_key_name,
            Callback=self._upload_callback if self.show_progress else None
        )


class S3_COPY:
    """
    | Helper function which supports copying of S3 objects. Also allows for displaying of copy progress in 10% increments.
    | Supports copying of objects larger than 5GB.
    | Functionality is exposed through the 'copy()' method.

    :param bool: show_progress: Whether progress should be printed.
    """

    def __init__(self, show_progress: bool):
        self.show_progress = show_progress
        self.total = 0
        self.copied = 0
        self.displayed_progress_percent = -1
        self.s3_client = boto3.client("s3")
        self.s3_resource = boto3.resource("s3")

    def _copy_callback(self, size: int):
        """
        | Callback function for copy progress. 
        | Prints progress in 10% increments.
        | As per boto3 documentation, the callback function takes a number of bytes transferred to be periodically called during the copy.

        :param int size: Number of bytes transferred.
        """

        if self.total == 0:
            return
        self.copied += size
        current_percent = int(self.copied / self.total * 100)
        if current_percent % 10 == 0 and self.displayed_progress_percent != current_percent:
            print(f"Copying: {current_percent}% ({(self.copied / 1048576):.2f}MB / {(self.total / 1048576):.2f}MB)")
            self.displayed_progress_percent = current_percent

    def copy(self, source_bucket_name: str, source_key_name: str, dest_bucket_name: str, dest_key_name: str):
        """
        | Copies an object in S3 from one position to another.

        :param string source_bucket_name: S3 source bucket name, without s3:// prefix.
        :param string source_key_name: S3 source key.
        :param string dest_bucket_name: S3 destination bucket name, without s3:// prefix.
        :param string dest_key_name: S3 destination key.
        """

        self.total = self.s3_client.head_object(
            Bucket=source_bucket_name,
            Key=source_key_name
        )["ContentLength"]

        self.s3_resource.meta.client.copy(
            CopySource={
                "Bucket": source_bucket_name,
                "Key": source_key_name
            },
            Bucket=dest_bucket_name,
            Key=dest_key_name,
            Callback=self._copy_callback if self.show_progress else None
        )


class S3_ZIP:
    """
    | Helper function which allows for streaming zipping of S3 bucket objects to a target S3 destination. Also allows for displaying of zip progress in 2^xMB increments.
    | Functionality is exposed through the 'zip()' and 'zip_xz()' methods.
    |
    | Requires stream_zip and to_file_like_obj libraries, which are not part of the standard Glue environment.
    | Add these as additional Python library paths if calling from Glue.

    :param bool: show_progress: Whether progress should be printed.
    """

    def __init__(self, show_progress: bool):
        self.show_progress = show_progress
        self.uploaded_bytes = 0
        self.displayed_uploaded_megabytes = -1
        self.s3 = boto3.client("s3")

    def _zip_callback(self, size: int):
        """
        | Callback function for zip upload progress. 
        | Prints progress of zip in 2^xMB increments using bitwise AND.
        | e.g. [4=0b100, 3=0b011, 100 & 011 == 000 == 0, therefore can check x & (x-1) == 0].
        | As per boto3 documentation, the callback function takes a number of bytes transferred to be periodically called during the upload.

        :param int size: Number of bytes transferred.
        """

        self.uploaded_bytes += size
        uploaded_megabytes_floor = math.floor(self.uploaded_bytes / 1048576)

        if uploaded_megabytes_floor == 0:
            return
        if uploaded_megabytes_floor & (
                uploaded_megabytes_floor - 1) == 0 and self.displayed_uploaded_megabytes != uploaded_megabytes_floor:
            print(f"Uploaded: {uploaded_megabytes_floor}MB")
            self.displayed_uploaded_megabytes = uploaded_megabytes_floor

    def zip(self, source_bucket_name: str, source_key_name: str, dest_bucket_name: str, dest_key_name: str):
        """
        | Zips an object in S3 and writes it to the specified destination in ZIP format.
        | Zipping is done in 8MB chunks in-memory and not written to disk.

        :param string source_bucket_name: S3 source bucket name, without s3:// prefix.
        :param string source_key_name: S3 source key.
        :param string dest_bucket_name: S3 destination bucket name, without s3:// prefix.
        :param string dest_key_name: S3 destination key.
        """

        from stream_zip import stream_zip, ZIP_64
        from to_file_like_obj import to_file_like_obj

        # Stream the file from S3
        def file_stream():
            response = self.s3.get_object(Bucket=source_bucket_name, Key=source_key_name)
            for chunk in response["Body"].iter_chunks(chunk_size=_S3_GET_CHUNK_SIZE):
                yield chunk

        # Compress the file data
        compressed_stream = stream_zip([(
            Path(source_key_name).name,
            datetime.datetime.now(),
            0o600,
            ZIP_64,
            file_stream()
        )])

        # Convert the compressed data into a file-like object
        file_like_obj = to_file_like_obj(compressed_stream)

        # Upload the file-like object to S3
        self.s3.upload_fileobj(
            Fileobj=file_like_obj,
            Bucket=dest_bucket_name,
            Key=dest_key_name,
            Callback=self._zip_callback if self.show_progress else None
        )

        # Final size
        print(f"Final Size: {(self.uploaded_bytes / 1048576):.2f}MB")

    def zip_xz(self, source_bucket_name: str, source_key_name: str, dest_bucket_name: str, dest_key_name: str):
        """
        | Zips an object in S3 and writes it to the specified destination in XZ format.
        | Zipping is done in 8MB chunks in-memory and not written to disk.

        :param string source_bucket_name: S3 source bucket name, without s3:// prefix.
        :param string source_key_name: S3 source key.
        :param string dest_bucket_name: S3 destination bucket name, without s3:// prefix.
        :param string dest_key_name: S3 destination key.
        """

        from to_file_like_obj import to_file_like_obj

        # Compress the file data
        def compressed_stream():
            response = self.s3.get_object(Bucket=source_bucket_name, Key=source_key_name)
            for chunk in response["Body"].iter_chunks(chunk_size=_S3_GET_CHUNK_SIZE):
                yield lzma.compress(chunk)

        # Convert the compressed data into a file-like object
        file_like_obj = to_file_like_obj(compressed_stream())

        # Upload the file-like object to S3
        self.s3.upload_fileobj(
            Fileobj=file_like_obj,
            Bucket=dest_bucket_name,
            Key=dest_key_name,
            Callback=self._zip_callback if self.show_progress else None
        )

        # Final size
        print(f"Final Size: {(self.uploaded_bytes / 1048576):.2f}MB")


class S3_UNZIP:
    """
    | Helper function which allows for streaming unzipping of S3 bucket objects to a target S3 destination. Also allows for displaying of unzip progress in 2^xMB increments.
    | Functionality is exposed through the 'unzip()' method.
    |
    | Requires stream_unzip and to_file_like_obj libraries, which are not part of the standard Glue environment.
    | Add these as additional Python library paths if calling from Glue.

    :param bool: show_progress: Whether progress should be printed.
    """

    def __init__(self, show_progress: bool):
        self.show_progress = show_progress
        self.uploaded_bytes = 0
        self.displayed_uploaded_megabytes = -1
        self.s3 = boto3.client("s3")

    def _unzip_callback(self, size: int):
        """
        | Callback function for unzip upload progress. 
        | Prints progress of zip in 2^xMB increments using bitwise AND.
        | e.g. [4=0b100, 3=0b011, 100 & 011 == 000 == 0, therefore can check x & (x-1) == 0].
        | As per boto3 documentation, the callback function takes a number of bytes transferred to be periodically called during the upload.

        :param int size: Number of bytes transferred.
        """

        self.uploaded_bytes += size
        uploaded_megabytes_floor = math.floor(self.uploaded_bytes / 1048576)

        if uploaded_megabytes_floor == 0:
            return
        # by the power of two! e.g. [4=0b100, 3=0b011, 100 & 011 == 000 == 0]
        if uploaded_megabytes_floor & (
                uploaded_megabytes_floor - 1) == 0 and self.displayed_uploaded_megabytes != uploaded_megabytes_floor:
            print(f"Uploaded: {uploaded_megabytes_floor}MB")
            self.displayed_uploaded_megabytes = uploaded_megabytes_floor

    def unzip(self, source_bucket_name: str, source_key_name: str, dest_bucket_name: str, dest_key_name: str):
        """
        | Unzips an object in S3 and writes it to the specified destination in ZIP format.
        | Unzipping is done in 8MB chunks in-memory and not written to disk.

        :param string source_bucket_name: S3 source bucket name, without s3:// prefix.
        :param string source_key_name: S3 source key.
        :param string dest_bucket_name: S3 destination bucket name, without s3:// prefix.
        :param string dest_key_name: S3 destination key.
        """

        from stream_unzip import stream_unzip
        from to_file_like_obj import to_file_like_obj

        # Stream the file from S3
        def file_stream():
            response = self.s3.get_object(Bucket=source_bucket_name, Key=source_key_name)
            for chunk in response["Body"].iter_chunks():
                yield chunk

        # Unzip the file data
        unzipped_stream = (
            chunk
            for _, _, chunks in stream_unzip(file_stream())
            for chunk in chunks
        )

        # Convert the compressed data into a file-like object
        file_like_obj = to_file_like_obj(unzipped_stream)

        # Upload the file-like object to S3
        self.s3.upload_fileobj(
            Fileobj=file_like_obj,
            Bucket=dest_bucket_name,
            Key=dest_key_name,
            Callback=self._unzip_callback if self.show_progress else None
        )

        # Final size
        print(f"Final Size: {(self.uploaded_bytes / 1048576):.2f}MB")


class S3_DELETE_DIR:
    """
    | Helper function which allows for deleting all objects in an S3 directory. Also allows for displaying of deletion count.
    | By right, 'directories' do not exist in S3, but a directory is created when the 'Create Folder' button is explicitly used in the S3 console.
    | This method will also delete that created directory if needed.
    | Functionality is exposed through the 'delete()' method.

    :param bool: show_progress: Whether progress should be printed.
    """

    def __init__(self, show_progress: bool):
        self.show_progress = show_progress
        self.s3 = boto3.client("s3")
        self.s3_resource = boto3.Session().resource("s3")

    def delete(self, target_bucket_name: str, target_directory_name: str):
        """
        | Deletes all objects in an S3 directory and the directory itself.

        :param string target_bucket_name: S3 target bucket name, without s3:// prefix.
        :param string target_directory_name: S3 target directory name.
        """

        # if the "Create Folder" button was used, the folder itself also gets listed and will persist after everything is deleted
        # mark for deletion later
        folder_is_real = False

        bucket_obj = self.s3_resource.Bucket(target_bucket_name)
        file_names = [object.key for object in bucket_obj.objects.filter(Prefix=target_directory_name)]
        if target_directory_name in file_names:
            file_names.remove(target_directory_name)   # remove listing of directory itself, delete last
            folder_is_real = True

        if self.show_progress:
            print(f"{len(file_names)} files detected. Deleting.")

        for file_name in file_names:
            self.s3.delete_object(Bucket=target_bucket_name, Key=file_name)

        if folder_is_real:
            self.s3.delete_object(Bucket=target_bucket_name, Key=target_directory_name)

        if self.show_progress:
            print(f"Delete completed: {len(file_names)} files removed.")


class S3_MERGE_DIR:
    """
    | Helper function which allows for concatenating all objects in a directory to another. Also allows for displaying of files merged and multipart upload call counts.
    | Concatenation is done in the order of the objects listed in the directory. (important)
    | Safely supports at most ~625GB combined data size (64MB * 10000 parts in worst case scenario).
    | Functionality is exposed through the 'merge()' method.
    |
    | Requires to_file_like_obj library, which is not part of the standard Glue environment.
    | Add this as an additional Python library path if calling from Glue.

    :param bool: show_progress: Whether progress should be printed.
    """

    def __init__(self, show_progress: bool):
        self.show_progress = show_progress
        self.s3 = boto3.client("s3")
        self.s3_resource = boto3.Session().resource("s3")

    def merge(self, source_bucket_name: str, source_dir_name: str, dest_bucket_name: str, dest_key_name: str):
        """
        | Merges all objects in a directory to another. Objects are merged in the order they are listed in the directory.
        | Maximum total allowable size of all objects is 625GB.

        :param string source_bucket_name: S3 source bucket name, without s3:// prefix.
        :param string source_dir_name: S3 source directory name.
        :param string dest_bucket_name: S3 destination bucket name, without s3:// prefix.
        :param string dest_key_name: S3 destination key.
        """

        from to_file_like_obj import to_file_like_obj

        def file_stream(source_bucket_name, source_key_name):
            response = self.s3.get_object(Bucket=source_bucket_name, Key=source_key_name)
            for chunk in response["Body"].iter_chunks(chunk_size=_S3_GET_CHUNK_SIZE):
                yield chunk

        bucket_obj = self.s3_resource.Bucket(source_bucket_name)
        file_names = [object.key for object in bucket_obj.objects.filter(Prefix=source_dir_name)]
        # remove listing of directory itself
        if source_dir_name in file_names:
            file_names.remove(source_dir_name)
        if not source_dir_name.endswith("/") and f"{source_dir_name}/" in file_names:
            file_names.remove(f"{source_dir_name}/")

        if self.show_progress:
            print(f"{len(file_names)} files detected. Beginning merge.")

        part_number = 1
        upload_id = None
        try:
            response = self.s3.create_multipart_upload(Bucket=dest_bucket_name, Key=dest_key_name)
            upload_id = response["UploadId"]
            parts = []
            buffer = b""

            for file_name in file_names:
                file_like_obj = to_file_like_obj(file_stream(source_bucket_name, file_name))

                while True:
                    data = file_like_obj.read(size=_MULTIPART_MIN_CHUNK_SIZE)
                    if not data:
                        break

                    buffer += data
                    if len(buffer) < _MULTIPART_MIN_CHUNK_SIZE:
                        continue

                    response = self.s3.upload_part(
                        Bucket=dest_bucket_name,
                        Key=dest_key_name,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=buffer
                    )
                    parts.append({"PartNumber": part_number, "ETag": response["ETag"]})
                    part_number += 1

                    buffer = b""

            # upload leftovers
            if buffer:
                response = self.s3.upload_part(
                    Bucket=dest_bucket_name,
                    Key=dest_key_name,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=buffer
                )
                parts.append({"PartNumber": part_number, "ETag": response["ETag"]})
                part_number += 1
        except Exception as e:
            if upload_id:
                self.s3.abort_multipart_upload(Bucket=dest_bucket_name, Key=dest_key_name, UploadId=upload_id)
            raise e

        self.s3.complete_multipart_upload(Bucket=dest_bucket_name, Key=dest_key_name,
                                          UploadId=upload_id, MultipartUpload={"Parts": parts})

        if self.show_progress:
            print(f"Multipart upload completed: {part_number - 1} part(s), {len(file_names)} files merged.")


class S3_MERGE_CSVS:
    """
    | Helper function which allows for concatenating all CSV objects in a directory to another. Also allows for displaying of files merged and multipart upload call counts.
    | Assumes there is a newline at the end of each CSV file and that the first line is the header.
    | There is no checking for header content, so ensure all CSVs have the same content.
    | Safely supports at most ~625GB combined data size (64MB * 10000 parts in worst case scenario).
    | Functionality is exposed through the 'merge()' method.
    | 
    | Requires to_file_like_obj library, which is not part of the standard Glue environment.
    | Add this as an additional Python library path if calling from Glue.

    :param bool: show_progress: Whether progress should be printed.
    """

    def __init__(self, show_progress: bool):
        self.show_progress = show_progress
        self.s3 = boto3.client("s3")
        self.s3_resource = boto3.Session().resource("s3")

    def merge(self, file_names: list[str], dest_bucket_name: str, dest_key_name: str):
        """
        | Merges all listed objects. Objects are merged in the order they are listed in.
        | Assumes all objects are CSVs, and will truncate the header of all but the first file.
        | Newline at EOF is expected.
        | Maximum total allowable size of all objects is 625GB.

        :param list file_names: List of S3 file names to merge.
        :param string dest_bucket_name: S3 destination bucket name, without s3:// prefix.
        :param string dest_key_name: S3 destination key.
        """

        from to_file_like_obj import to_file_like_obj

        def file_stream(source_bucket_name, source_key_name):
            response = self.s3.get_object(Bucket=source_bucket_name, Key=source_key_name)
            for chunk in response["Body"].iter_chunks(chunk_size=_S3_GET_CHUNK_SIZE):
                yield chunk

        if self.show_progress:
            print(f"{len(file_names)} files specified. Beginning CSV merge.")

        part_number = 1
        upload_id = None
        try:
            response = self.s3.create_multipart_upload(Bucket=dest_bucket_name, Key=dest_key_name)
            upload_id = response["UploadId"]
            parts = []
            csv_header_written = False
            buffer = b""

            for file_name in file_names:
                fresh_read = True
                bucket_name = "/".join(file_name.split("/")[2:3])
                key_name = "/".join(file_name.split("/")[3:])

                file_like_obj = to_file_like_obj(file_stream(bucket_name, key_name))

                while True:
                    data = file_like_obj.read(size=_MULTIPART_MIN_CHUNK_SIZE)
                    if not data:
                        break

                    # when dealing with a file's first block of data, if header has been already written, skip writing first line
                    # subsequent blocks will always fully written
                    if fresh_read and csv_header_written:
                        data = data[data.index(b"\n")+1:]   # get index of first newline byte, then restart data from one byte after it
                    else:
                        csv_header_written = True

                    fresh_read = False

                    buffer += data
                    if len(buffer) < _MULTIPART_MIN_CHUNK_SIZE:
                        continue

                    response = self.s3.upload_part(
                        Bucket=dest_bucket_name,
                        Key=dest_key_name,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=buffer
                    )
                    parts.append({"PartNumber": part_number, "ETag": response["ETag"]})
                    part_number += 1

                    buffer = b""

            # upload leftovers
            if buffer:
                response = self.s3.upload_part(
                    Bucket=dest_bucket_name,
                    Key=dest_key_name,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=buffer
                )
                parts.append({"PartNumber": part_number, "ETag": response["ETag"]})
                part_number += 1
        except Exception as e:
            if upload_id:
                self.s3.abort_multipart_upload(Bucket=dest_bucket_name, Key=dest_key_name, UploadId=upload_id)
            raise e

        self.s3.complete_multipart_upload(Bucket=dest_bucket_name, Key=dest_key_name,
                                          UploadId=upload_id, MultipartUpload={"Parts": parts})

        if self.show_progress:
            print(f"Multipart upload completed: {part_number - 1} part(s), {len(file_names)} files merged.")


def get_resource_arn(resource_type: str, resource_name: str) -> str:
    """
    | Gets ARN value of a resource based on the resource name. 
    | Works on the basis that ARNs have the resource name at the back of it.

    :param string resource_type: Resource type. Currently implemented: 'state_machine', 'sns'.
    :param string resource_name: Resource name.

    :returns: string
    """

    arn_list = []

    match resource_type:
        case "state_machine":
            stepfunction_client = boto3.client("stepfunctions")
            arn_list = [sfn["stateMachineArn"] for sfn in stepfunction_client.list_state_machines()["stateMachines"]]
        case "sns":
            sns_client = boto3.client("sns")
            arn_list = [tp["TopicArn"] for tp in sns_client.list_topics()["Topics"]]
        case _:
            pass

    if len(arn_list) == 0:
        return None

    # ends with resource_name
    pattern = f"{resource_name}$"
    for arn in arn_list:
        if bool(re.search(pattern, arn)):
            return arn

    return None


def get_mysql_conn_details_password(glue_connector_name: str, override_schema_name: str = "") -> dict:
    """
    | Retrieves MySQL database login details from the specified glue connector.
    | Schema name is also defined in the Glue Connector itself, but can be overridden by specifying override_schema_name.

    :param string glue_connector_name: Name of AWS Glue Connector containing login details.
    :param string override_schema_name: Specify a different schema name to use from the one defined in the Glue connection.

    :returns: dictionary - {
        'hostname': string,
        'username': string,
        'password': string,
        'port': int,
        'schema': string
    }
    """

    # retrieve Glue credentials
    print("Retrieving login credentials from Glue connection.")
    glue_client = boto3.client("glue")
    connection = glue_client.get_connection(Name=glue_connector_name)
    print("Successfully retrieved Glue connection detail.")

    # process Glue credentials
    jdbc_connection_url = connection["Connection"]["ConnectionProperties"]["JDBC_CONNECTION_URL"]
    jdbc_connection_url_split = jdbc_connection_url.split("/")

    hostname = jdbc_connection_url_split[2].split(":")[0]
    port = jdbc_connection_url_split[2].split(":")[1]
    default_schema = jdbc_connection_url_split[3]

    username = connection["Connection"]["ConnectionProperties"]["USERNAME"]
    password = connection["Connection"]["ConnectionProperties"]["PASSWORD"]

    print("Successfully read login credentials.")

    return {
        "hostname": hostname,
        "username": username,
        "password": password,
        "port": int(port),
        "schema": default_schema if not override_schema_name else override_schema_name
    }


def find_objects(bucket_name: str, prefix: str) -> list[str]:
    """
    Get all object names in a bucket with a specified prefix.

    :param string bucket_name: The name of the bucket to search in.
    :param string prefix: The prefix to search for.

    :returns: list(string)
    """

    s3_resource = boto3.Session().resource("s3")

    bucket_obj = s3_resource.Bucket(bucket_name)
    file_names = [object.key for object in bucket_obj.objects.filter(Prefix=prefix)]

    return file_names
