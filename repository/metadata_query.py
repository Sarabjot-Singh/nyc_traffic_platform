'''CREATE TABLE metadata.raw_ingestion_log
(
    id           SERIAL PRIMARY KEY,
    file_name    VARCHAR(255) NOT NULL,
    source       TEXT NOT NULL,
    destination  TEXT NOT NULL,
    status       VARCHAR(20) NOT NULL,
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
    def ingestion_log(file_name, source, destination, status, error):
        query = f'''
            INSERT INTO metadata.raw_ingestion_log
            (
                file_name,
                source,
                destination,
                status,
                error
            )
            VALUES
            (
                '{file_name}',
                '{source}',
                '{destination}',
                '{status}',
                '{error}'
            );
        '''

        return query

    @staticmethod
    def silver_load_log(file_name, source, destination, status, error, load_type, method='overwrite'):
        query = f'''
            INSERT INTO metadata.silver_ingestion_log
            (
                file_name,
                source,
                destination,
                method,
                status,
                load_type,
                error
            )
            VALUES
            (
                '{file_name}',
                '{source}',
                '{destination}',
                '{method}',
                '{status}',
                '{load_type}',
                '{error}'
            );
        '''

        return query
    
    @staticmethod
    def get_n_latest_files(n):
        query = f'''
            WITH top_n_files AS (
                SELECT 
                    *
                    , ROW_NUMBER() OVER(ORDER BY processed_at DESC) AS rn 
                FROM metadata.raw_ingestion_log
                WHERE status = 'SUCCESS'
            )
            SELECT 
                DISTINCT destination 
            FROM top_n_files
            WHERE rn <= {n};
        '''

        return query
    
    @staticmethod
    def get_successful_bronze_files():
        query = '''
            SELECT
                DISTINCT destination
            FROM metadata.raw_ingestion_log
            WHERE status = 'SUCCESS';
        '''
        return query
    
    @staticmethod
    def is_file_Uploaded_to_bronze(file_name):
        query = f"""SELECT COUNT(1) FROM metadata.raw_ingestion_log WHERE file_name = '{file_name}' AND status = 'SUCCESS'"""
        return query

    def pipeline_run_log():
        pass