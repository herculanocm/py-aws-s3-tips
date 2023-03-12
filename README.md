# Py tips for use
de_aws_s3_tips = Data Engineer AWS S3 Python Tips
## Using:

First generate wheel file for install with
```
python setup.py bdist_wheel
```

Import the module
```
from de_aws_s3_tips import s3_file_transformations as DE

object_bytes = DE.get_bytes_by_s3_key_object(
    s3_client,
    args['par_source_bucket_name'], 
    args['par_unzip_file_key'],
    logger
)
```

## Enviroments

* Python 3.9

## Tests

```
pytest -v -s
```