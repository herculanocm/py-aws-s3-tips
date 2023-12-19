import sys
sys.path.insert(0, f'../de_aws_s3_tips/')
from de_aws_s3_tips import s3_file_transformations
import pytest
from boto3 import client as aws_client

@pytest.fixture(scope="session")
def s3_client():
    aws_access_key_id=''
    aws_secret_access_key=''
    
    s3_client = aws_client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
        )
    yield s3_client

def test_list_dict_to_csv_bytes(s3_client):
    lista = []
    file_s3_csv = s3_file_transformations.list_dict_to_csv_bytes(lista=lista, list_keys=['id','nome'])
    print(f"file_s3_csv: {file_s3_csv}")
    assert 1 == 1

# def test_get_bytes_by_s3_key_object(s3_client):
#     object_bytes = s3_file_transformations.get_bytes_by_s3_key_object(
#         s3_client,
#         'herculanocm-public-python-tips', 
#         'zip-files/python-tips/file.zip'
#         )
#     assert isinstance(object_bytes, bytes) and len(object_bytes) > 0

# def test_unzip_bytes_upload_s3_objects(s3_client):
#     list_objs = s3_file_transformations.get_s3_objects_matching_prefix(
#         s3_client,
#         'herculanocm-public-python-tips',
#         'unzip-files/export/'
#     )
#     list_assert =[]
#     for lobj in list_objs:
#         list_assert.append((lobj['Key'], lobj['Size']))

#     list_compare = [
#         ('unzip-files/export/file1.csv', 3283974), 
#         ('unzip-files/export/file1.xlsx', 9755), 
#         ('unzip-files/export/file2.csv', 3283974), 
#         ('unzip-files/export/file3.txt', 55)
#         ]

#     assert len(list_assert) > 0 and list_assert == list_compare