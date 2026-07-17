'''CREATE TABLE metadata.bronze_ingestion_status_log
(
    id           SERIAL PRIMARY KEY,
    file_name    VARCHAR(255) NOT NULL,
    source       TEXT NOT NULL,
    destination  TEXT NOT NULL,
    status       VARCHAR(20) NOT NULL,
    load_type    VARCHAR(20) NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error TEXT
);'''

'''
CREATE TABLE metadata.silver_ingestion_log
(
    id           SERIAL PRIMARY KEY,
    file_name    VARCHAR(255) NOT NULL,
    source       TEXT NOT NULL,
    destination  TEXT NOT NULL,
    method       VARCHAR(20) NOT NULL,
    status       VARCHAR(20) NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    load_type TEXT,
    error TEXT
);
'''

class QueryStore:

    @staticmethod
    def run_log_status(file_name, source, destination, status, load_type, error, table_name):
        query = f'''
            INSERT INTO metadata.{table_name}
            (
                file_name,
                source,
                destination,
                status,
                load_type
                error
            )
            VALUES
            (
                '{file_name}',
                '{source}',
                '{destination}',
                '{status}',
                '{load_type}',
                '{error}'
            );
        '''

        return query
    
    
    @staticmethod
    def get_n_latest_files(n, file_name):
        query = f'''
            WITH top_n_files AS (
                SELECT 
                    *
                    , ROW_NUMBER() OVER(ORDER BY processed_at DESC) AS rn 
                FROM metadata.bronze_ingestion_log
                WHERE status = 'SUCCESS' AND file_name like '%{file_name}%'
            )
            SELECT 
                DISTINCT destination 
            FROM top_n_files
            WHERE 
                rn <= {n}
            ;
        '''

        return query
    
    @staticmethod
    def get_successful_bronze_files(file_name):
        query = f'''
            SELECT
                DISTINCT destination
            FROM metadata.bronze_ingestion_log
            WHERE 
                status = 'SUCCESS'
                AND file_name like '%{file_name}%'
                ;
        '''
        return query
    
    @staticmethod
    def is_file_Uploaded_to_bronze(file_name):
        query = f"""SELECT COUNT(1) FROM metadata.bronze_ingestion_log WHERE file_name = '{file_name}' AND status = 'SUCCESS'"""
        return query

    @staticmethod
    def pipeline_run_log():
        pass