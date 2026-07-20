import sys
from pathlib import Path
from pyspark.sql.functions import *
from dotenv import load_dotenv

project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.common.logger import get_logger
from src.common.favicon import favicon
from src.common.database import Database
from repository.metadata_query import QueryStore

load_dotenv()

logger = get_logger()
database_obj = Database()

class Loader():

    def load_dataframe(self, dataframe, file_name, source, destination, mode, partition_column, format, layer):
        """Write a dataframe to storage and log the ingestion status.

        Args:
            dataframe: The Spark DataFrame to write.
            file_name: The name of the file being loaded (for metadata logging).
            source: The source of the data (for metadata logging).
            destination: The target storage path where the dataframe will be written.
            mode: The write mode (e.g., 'overwrite', 'append').
            partition_column: Column name to partition by, or None for no partitioning.
            format: The file format to write (e.g., 'parquet', 'delta').
            layer: The data layer being written to (bronze, silver, or gold).
        """
        try:
            query = QueryStore().run_log_status(layer=layer)
            database_obj.execute(
                    query=query,
                    params={
                        "file_name": file_name,
                        "source": source,
                        "destination": destination,
                        "status": 'UPLOADING',
                        "load_type": mode,
                        "error": str('None')
                    }
                )
            
            #########################################
            # Writing starts here 
            #########################################
            logger.info(f"{favicon['info']} Started Loading Batch")
            
            if partition_column is not None:
                dataframe.write \
                    .format(format) \
                    .mode(mode) \
                    .partitionBy(partition_column) \
                    .save(destination)
            else:
                dataframe.write \
                    .format(format) \
                    .mode(mode) \
                    .save(destination)
            
            logger.info(f"{favicon['right']} Batch Loaded Successfully")
            #########################################
            # Writing ends here 
            #########################################
            
            query = QueryStore().run_log_status(layer=layer)
            database_obj.execute(
                    query=query,
                    params={
                        "file_name": file_name,
                        "source": source,
                        "destination": destination,
                        "status": 'SUCCESS',
                        "load_type": mode,
                        "error": str('None')
                    }
                )
            
        except Exception as e:
            logger.info(f"{favicon['error']} Failed Loading the Batch")
            
            query = QueryStore().run_log_status(layer=layer)
            database_obj.execute(
                    query=query,
                    params={
                        "file_name": file_name,
                        "source": source,
                        "destination": destination,
                        "status": 'FAILED',
                        "load_type": mode,
                        "error": str(e)
                    }
                )

    
    def incremental_load(self, source_df, target_df, condition, file_name, source, destination, mode, layer):
        """Perform an incremental merge between source and target dataframes.

        Uses Delta Lake MERGE operation to update existing records and insert new records
        based on the provided merge condition. Logs the ingestion status throughout the process.

        Args:
            source_df: The source Spark DataFrame with new or updated data.
            target_df: The target Spark DataFrame (Delta table) to merge into.
            condition: The merge condition that determines which rows match between source and target.
            file_name: The name of the file being loaded (for metadata logging).
            source: The source of the data (for metadata logging).
            destination: The target storage path (for metadata logging).
            mode: The write mode (for metadata logging).
            layer: The data layer being written to (bronze, silver, or gold).
        """
        try:
            query = QueryStore().run_log_status(layer=layer)
            database_obj.execute(
                    query=query,
                    params={
                        "file_name": file_name,
                        "source": source,
                        "destination": destination,
                        "status": 'UPLOADING',
                        "load_type": mode,
                        "error": str('None')
                    }
                )
            
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
            
            query = QueryStore().run_log_status(layer=layer)
            database_obj.execute(
                    query=query,
                    params={
                        "file_name": file_name,
                        "source": source,
                        "destination": destination,
                        "status": 'SUCCESS',
                        "load_type": mode,
                        "error": str('None')
                    }
                )
            
        except Exception as e:
            logger.info(f"{favicon['error']} Failed Loading the Batch")
            query = QueryStore().run_log_status(layer=layer)
            database_obj.execute(
                    query=query,
                    params={
                        "file_name": file_name,
                        "source": source,
                        "destination": destination,
                        "status": 'FAILED',
                        "load_type": mode,
                        "error": str(e)
                    }
                )