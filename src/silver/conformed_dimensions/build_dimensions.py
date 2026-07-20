import os
import sys
import yaml
from pyspark.sql.functions import *
from pyspark.sql.window import Window

from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.common.logger import get_logger
from src.common.favicon import favicon
from src.common.spark import SparkManager
from src.common.aws import S3Util
from src.common.database import Database
from src.common.loader import Loader
from repository.metadata_query import QueryStore


logger = get_logger()

spark = SparkManager("build_dimensions").get_spark_session()
s3_util = S3Util()

with open('./src/silver/dimensions.yml', 'r') as file:
    dimensional_config = yaml.safe_load(file)

with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)


if __name__ == '__main__':
    # logger.info(f"{favicon['info']} Building Dimensions....")
    file_name = os.path.abspath(__file__).split('/')[-1].split('.')[0]
    dimensions = dimensional_config['dimensions']
    layer = config['layers']['silver']
    bucket_name = config['storage']['bucket_name']
    loader = Loader()

    for dimension in dimensions:
        name = dimension['name']
        logger.info(f"{favicon['info']} Working on dimension - {name}")
        module = dimension['module']
        module_path = dimension['module_path']
        class_name = dimension['class']
        enabled = dimension['enabled']
        load_method = dimension['load_method']
        format = dimension['format']

        s3_key = rf"s3a://{bucket_name}/{layer}/{module}/"
        
        imported_module = __import__(module_path, fromlist=[module])

        dimension_class = getattr(imported_module, class_name)
        dimension_obj = dimension_class(spark)

        df = dimension_obj.initial_load()
        
        kwargs = {
            'dataframe': df,
            'file_name': file_name,
            'source': 'seed_file',
            'destination': s3_key,
            'mode': load_method,
            'partition_column': None,
            'format': format,
            'layer': layer
        }

        loader.load_dataframe(**kwargs)


        



