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
    dimensions = dimensional_config['dimensions']
    silver = config['layers']['silver']
    bucket_name = config['storage']['bucket_name']
    db_obj = Database()

    for dimension in dimensions:
        name = dimension['name']
        logger.info(f"{favicon['info']} Working on dimension - {name}")
        module = dimension['module']
        module_path = dimension['module_path']
        class_name = dimension['class']
        enabled = dimension['enabled']
        load_method = dimension['load_method']
        format = dimension['format']

        s3_key = rf"s3a://{bucket_name}/{silver}/{module}/"

        query = QueryStore().silver_load_log(module, 'seed_file', s3_key, 'UPLOADING', load_method)
        db_obj.execute(query=query)
        
        imported_module = __import__(module_path, fromlist=[module])

        dimension_class = getattr(imported_module, class_name)
        dimension_obj = dimension_class(spark)

        df = dimension_obj.initial_load()
        
        df.write \
            .format(format) \
            .mode(load_method) \
            .save(s3_key)
        
        logger.info(f"{favicon['right']} Successfully created dimension - {name}")
        
        query = QueryStore().silver_load_log(module, 'seed_file', s3_key, 'SUCCESS', load_method)
        db_obj.execute(query=query)


        



