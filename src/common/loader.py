import sys
import yaml
from pathlib import Path
from pyspark.sql.functions import *
from dotenv import load_dotenv

project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.common.logger import get_logger
from src.common.favicon import favicon
from src.common.spark import SparkManager
from src.common.database import Database
from repository.metadata_query import QueryStore

load_dotenv()

logger = get_logger()
database_obj = Database()

class Loader():


    def load_dataframe(self, dataframe, file_name, source, destination, mode, load_type, partition_column, format, log_table_name):
        try:
            # query = QueryStore().ingestion_log(
            #     file_name=file_name, 
            #     source=source, 
            #     destination=destination, 
            #     status='UPLOADING', 
            #     method=method, 
            #     load_type=load_type,
            #     error=None
            # )
            # database_obj.execute(query=query)
            
            #########################################
            # Writing starts here 
            #########################################
            logger.info(f"{favicon['info']} Started Loading Batch")
            
            dataframe.write \
                .format(format) \
                .mode(mode) \
                .partitionBy(partition_column) \
                .save(destination)
            
            logger.info(f"{favicon['right']} Batch Loaded Successfully")
            #########################################
            # Writing ends here 
            #########################################
            
            # query = QueryStore().silver_load_log(
            #     file_name=file_name, 
            #     source=source, 
            #     destination=destination, 
            #     status='SUCCESS', 
            #     method=method, 
            #     load_type=load_type,
            #     error=None
            # )
            # database_obj.execute(query=query)
            
        except Exception as e:
            logger.info(f"{favicon['error']} Failed Loading the Batch Successfully")
            # query = QueryStore().silver_load_log(
            #     file_name=file_name, 
            #     source=source, 
            #     destination=destination, 
            #     status='FAILED', 
            #     method=method, 
            #     load_type=load_type,
            #     error=str(e)
            # )
            # database_obj.execute(query=query)

    
    def incremental_load(self, source_df, target_df, condition, file_name, source, destination, method, load_type, log_table_name):
        try:
            # query = QueryStore().silver_load_log(
            #     file_name=file_name, 
            #     source=source, 
            #     destination=destination, 
            #     status='UPLOADING', 
            #     method=method, 
            #     load_type=load_type,
            #     error=None
            # )
            # database_obj.execute(query=query)
            
            #########################################
            # Writing starts here 
            #########################################
            logger.info(f"{favicon['info']} Started Loading Batch")
            
            target_df.alias("TARGET").merge(
                source=source_df.alias("SOURCE"),
                condition=condition
            ) \
            .withSchemaEvolution() \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
            
            logger.info(f"{favicon['right']} Batch Loaded Successfully")
            #########################################
            # Writing ends here 
            #########################################
            
            # query = QueryStore().silver_load_log(
            #     file_name=file_name, 
            #     source=source, 
            #     destination=destination, 
            #     status='SUCCESS', 
            #     method=method, 
            #     load_type=load_type,
            #     error=None
            # )
            # database_obj.execute(query=query)
            
        except Exception as e:
            logger.info(f"{favicon['error']} Failed Loading the Batch Successfully")
            # query = QueryStore().silver_load_log(
            #     file_name=file_name, 
            #     source=source, 
            #     destination=destination, 
            #     status='FAILED', 
            #     method=method, 
            #     load_type=load_type,
            #     error=str(e)
            # )
            # database_obj.execute(query=query)
        