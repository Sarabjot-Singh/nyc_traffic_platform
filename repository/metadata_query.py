"""CREATE TABLE metadata.gold_ingestion_status_log
(
    id           SERIAL PRIMARY KEY,
    file_name    VARCHAR(255) NOT NULL,
    source       TEXT NOT NULL,
    destination  TEXT NOT NULL,
    status       VARCHAR(20) NOT NULL,
    load_type    VARCHAR(20) NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error TEXT
);"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.common.logger import get_logger
from src.common.favicon import favicon

logger = get_logger()

class QueryStore:

    @staticmethod
    def run_log_status(layer):
        """Generate a SQL INSERT query to log ingestion status for a given layer.

        Args:
            layer: The data layer (bronze, silver, or gold) for which to log status.

        Returns:
            str: A parameterized SQL INSERT statement for the appropriate status log table.
        """
        table_name = ''
        if layer.lower() == 'bronze':
            table_name = 'bronze_ingestion_status_log'
        elif layer.lower() == 'silver':
            table_name = 'silver_ingestion_status_log'
        elif layer.lower() == 'gold':
            table_name = 'gold_ingestion_status_log'
        else:
            table_name = 'faulty_ingestion_status_log'
            logger.error(f"""{favicon['error']} Error please pass a valid layer name.
                         Uploading logs to faulty_ingestion_status_log
                """)
            
        query = f"""
            INSERT INTO metadata.{table_name}
            (
                file_name,
                source,
                destination,
                status,
                load_type,
                error
            )
            VALUES
            (
                :file_name,
                :source,
                :destination,
                :status,
                :load_type,
                :error
            );
        """

        return query
    
    
    @staticmethod
    def get_n_latest_files(n, file_name):
        """Retrieve destinations of the n most recently ingested files matching a pattern.

        Args:
            n: The number of most recent files to retrieve.
            file_name: A filename pattern to filter results (matches partial filenames).

        Returns:
            str: A SQL query that returns distinct destination paths for the top n files.
        """
        query = f"""
            WITH top_n_files AS (
                SELECT 
                    *
                    , ROW_NUMBER() OVER(ORDER BY processed_at DESC) AS rn 
                FROM metadata.bronze_ingestion_status_log
                WHERE status = 'SUCCESS' AND file_name like '%{file_name}%'
            )
            SELECT 
                DISTINCT destination 
            FROM top_n_files
            WHERE 
                rn <= {n}
            ;
        """

        return query
    
    @staticmethod
    def get_successful_bronze_files(file_name):
        """Retrieve all destinations of successfully ingested bronze files matching a pattern.

        Args:
            file_name: A filename pattern to filter results (matches partial filenames).

        Returns:
            str: A SQL query that returns distinct destination paths for successful files.
        """
        query = f"""
            SELECT
                DISTINCT destination
            FROM metadata.bronze_ingestion_status_log
            WHERE 
                status = 'SUCCESS'
                AND file_name like '%{file_name}%'
                ;
        """
        return query
    
    @staticmethod
    def is_file_Uploaded_to_bronze(file_name):
        """Check if a file has been successfully uploaded to the bronze layer.

        Args:
            file_name: The name of the file to check.

        Returns:
            str: A SQL query that returns a count of 1 if the file was successfully ingested, 0 otherwise.
        """
        query = f"""SELECT COUNT(1) FROM metadata.bronze_ingestion_status_log WHERE file_name = '{file_name}' AND status = 'SUCCESS'"""
        return query

    @staticmethod
    def pipeline_run_log():
        """Generate a query to log pipeline execution details (not yet implemented).

        Returns:
            None: This method is not yet implemented.
        """
        pass