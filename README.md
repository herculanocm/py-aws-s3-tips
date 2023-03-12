# Pyspark tips for use
de_pyspark_tips = Data Engineer Pyspark Tips
## Using:

First generate wheel file for install with
```
python setup.py bdist_wheel
```

Import the module
```
import de_pyspark_tips.df_transformations as ET

castedDF = ET.cast_columns_types_by_schema(df, list_schema, True)
filterDF = ET.choose_last_row_modify_by_ids(castedDF, ['id'], ['data_transaction'])
```

## Enviroments

* Python 3.9
* JAVA 1.8
* SPARK 3.2
* Pyspark >=3.1,<3.3

## Tests

```
pytest -v -s
```