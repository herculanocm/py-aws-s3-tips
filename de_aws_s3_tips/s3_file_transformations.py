from typing import IO, List
from boto3 import client as aws_client, resource as aws_resource
from botocore.exceptions import ClientError
from sys import exit, stdout
from de_aws_s3_tips.custom_formatter_logger import CustomFormatter
from de_aws_s3_tips.progress_percentage_upload import ProgressPercentage
from zipfile import ZipFile, ZipInfo
from io import BytesIO, StringIO
from datetime import datetime
import logging
import csv


def get_logger(logger: logging.Logger = None) -> logging.Logger:
    if logger is None:

        handler = logging.StreamHandler(stdout)
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(CustomFormatter())

        logger = logging.getLogger(__name__)

        if logger.hasHandlers():
            logger.removeHandler(handler)

        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

    return logger

def get_bytes_by_s3_key_object(s3_client: aws_client, bucket_name: str, key: str, logger=None) -> bytes:
    logger = logging.getLogger()
    logger.debug(f'Getting object key: {key}')
    result = None

    try:
        result = s3_client.get_object(Bucket=bucket_name, Key=key)
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            logger.error('The specified key does not exist')
        else:
            logger.critical("Unexpected error: %s" % e)

        exit(3)

    logger.debug(f'Reading object bytes: {key}')
    byt = result["Body"].read()
    logger.info(f'Object bytes getted succefull: {key}')
    return byt


def get_s3_objects_matching_prefix(s3_client: aws_client, bucket: str, prefix: str = "", logger=None) -> List[dict]:
    logger = logging.getLogger()
    logger.debug(f'Getting paginator')
    paginator = s3_client.get_paginator("list_objects_v2")

    kwargs = {'Bucket': bucket}
    lst_return = []

    # We can pass the prefix directly to the S3 API.  If the user has passed
    # a tuple or list of prefixes, we go through them one by one.
    if isinstance(prefix, str):
        prefixes = (prefix, )
    else:
        prefixes = prefix

    for key_prefix in prefixes:
        kwargs["Prefix"] = key_prefix

        for page in paginator.paginate(**kwargs):
            try:
                contents = page["Contents"]
            except KeyError:
                break

            for obj in contents:
                lst_return.append(obj)

    logger.debug(f'Return paginator size is {len(lst_return)}')
    return lst_return

def rename_s3_file(s3_client, bucket_name, old_key, new_key, logger=None):
    logger = logging.getLogger()
    # Copy the file to the new key
    copy_source = {'Bucket': bucket_name, 'Key': old_key}
    logger.info('Copy the file to the new key')
    s3_client.copy(copy_source, bucket_name, new_key)

    # Delete the old file
    logger.info('Delete the old file')
    s3_client.delete_object(Bucket=bucket_name, Key=old_key)

def get_s3_file_last_modified(s3_client, bucket_name, file_key) -> datetime:
    # Get the object metadata
    response = s3_client.head_object(Bucket=bucket_name, Key=file_key)

    # Extract and return the last modified date
    last_modified = response['LastModified']
    return last_modified

def delete_s3_objects_by_prefix(resource: aws_resource, bucket_name: str, prefix: str, logger=None):
    logger = logging.getLogger()
    bucket = resource.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=prefix):
        logger.debug(f'Deleting object: {obj.key}')
        resource.Object(bucket.name, obj.key).delete()
        logger.debug(f'Object: {obj.key} - Deleted')


def get_infolist_from_bytes_zip(bytes_object: bytes, logger=None) -> List[ZipInfo]:
    logger = logging.getLogger()
    buffer = BytesIO(bytes_object)
    logger.debug('Open buffer, getting list')
    zipFile = ZipFile(buffer, 'r')
    infolist = zipFile.infolist()
    logger.debug(f'infolist: {infolist}')
    return infolist


def get_file_from_bytes_zip(bytes_zip: bytes, file_name: str, logger=None) -> IO[bytes]:
    log = logging.getLogger()
    buffer = BytesIO(bytes_zip)
    zipFile = ZipFile(buffer, 'r')
    log.debug('Open buffer, getting file')
    file_bytes = zipFile.open(file_name)
    log.debug('File getted successful')
    return file_bytes


def upload_fileobj(s3_client: aws_client, bytes_obj: bytes, bucket: str, key: str, file_name: str, file_size: float, logger=None):
    logger = logging.getLogger()
    logger.debug(f'Uploading file {file_name}')
    s3_client.upload_fileobj(
        bytes_obj,
        Bucket=bucket,
        Key=key,
        Callback=ProgressPercentage(file_name, file_size, logger)
    )
    logger.debug(f'File upload successful {file_name}')


def list_dict_to_csv_bytes(lista: List[dict], delimiter: str = '|', quotechar: str = '"', quoting: int = csv.QUOTE_NONNUMERIC, lineterminator: str = '\r\n', list_keys: list = None) -> BytesIO:
    
    is_list_keys = False

    if list_keys is None:
        is_list_keys = False
    else:
        is_list_keys = True
    
    writer_file =  StringIO()
    dict_writer = csv.DictWriter(writer_file, lista[0].keys(), delimiter=delimiter, quotechar=quotechar, quoting=quoting, lineterminator=lineterminator)
    
    if is_list_keys == False:
        dict_writer.writeheader()

    dict_writer.writerows(lista)

    content = ''
    if is_list_keys == True:
        if quoting == 0:
            list_keys = [(col) for col in list_keys]
        else:
            list_keys = [('"'+col+'"') for col in list_keys]

        content = (delimiter.join(list_keys) + '' + lineterminator)


    content += writer_file.getvalue()

    return BytesIO(content.encode('utf-8'))


def unzip_bytes_upload_s3_objects(s3_client: aws_client, bytes_zip: bytes, target_bucket: str, target_key_prefix: str, logger=None):
    logger = logging.getLogger()
    for zipInfo in get_infolist_from_bytes_zip(bytes_zip, logger):

        file_name = zipInfo.filename
        file_size = zipInfo.file_size

        final_key = target_key_prefix + file_name
        logger.debug(f'Final key {final_key}')

        bytes_obj = get_file_from_bytes_zip(bytes_zip, file_name, logger)

        upload_fileobj(s3_client, bytes_obj, target_bucket,
                       final_key, file_name, file_size, logger)

    logger.debug(f'Finished unzip')


def order_columns_by_schema(schema: List[dict], name_column_order: str, name_column: str = 'column_name', logger=None) -> List[str]:
    logger = logging.getLogger()
    logger.debug('Ordening columns')
    ordened_list = sorted(schema, key=lambda d: d[name_column_order])
    return [rw[name_column] for rw in ordened_list]


def object_exists(s3_client: aws_client, bucket_name: str, key_name: str, logger=None) -> bool:
    logger = logging.getLogger()
    try:
        logger.debug(f'Checking object exists: {key_name}')
        s3_client.head_object(Bucket=bucket_name, Key=key_name)
        logger.debug(f'Object exists: {key_name}')
        return True
    except ClientError as e:
        logger.debug(f'Error Object not exists: {key_name}')
        logger.error(f'Error Object not exists: {key_name}')
        return False


def add_defaults_columns_cdc_v1(table_name: str, dict_schema: dict, add_sys_file_date: bool = True, logger=None) -> dict:
    logger = logging.getLogger()
    df_cols = [
        {'table_name': table_name, 'column_name': 'sys_operation', 'data_type': 'varchar', 'primary_key': False, 'ordinal_position': 100001, 'default_column': True},
        {'table_name': table_name, 'column_name': 'sys_commit_time', 'data_type': 'timestamp', 'primary_key': False, 'ordinal_position': 100002, 'default_column': True},
        {'table_name': table_name, 'column_name': 'sys_commit_timestamp', 'data_type': 'timestamp', 'primary_key': False, 'ordinal_position': 100003, 'default_column': True},
        {'table_name': table_name, 'column_name': 'sys_transaction_id', 'data_type': 'varchar', 'primary_key': False, 'ordinal_position': 100004, 'default_column': True},
        {'table_name': table_name, 'column_name': 'sys_file_name', 'data_type': 'varchar', 'primary_key': False, 'ordinal_position': 100005, 'default_column': True},
        {'table_name': table_name, 'column_name': 'id_controle_data_lake','data_type': 'int', 'primary_key': False, 'ordinal_position': 100006, 'default_column': True}
    ]
    if add_sys_file_date == True:
        df_cols.append({'table_name': table_name, 'column_name': 'sys_file_date', 'data_type': 'date', 'primary_key': False, 'ordinal_position': 100007, 'default_column': True})

    for dfc in df_cols:
        existe = 0
        for sc in dict_schema['columns']:
            if sc['column_name'].lower() == dfc['column_name'].lower():
                existe = 1
        if existe == 0:
            logger.debug(f"Adding column: {dfc['column_name']}")
            dict_schema['columns'].append(dfc)

    return dict_schema
