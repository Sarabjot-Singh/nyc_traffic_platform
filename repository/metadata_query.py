'''CREATE TABLE metadata.raw_ingestion_log
(
    id           SERIAL PRIMARY KEY,
    file_name    VARCHAR(255) NOT NULL,
    source       TEXT NOT NULL,
    destination  TEXT NOT NULL,
    status       VARCHAR(20) NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
'''

class QueryStore:

    @staticmethod
    def ingestion_log(file_name, source, destination, status):
        query = f'''
            INSERT INTO metadata.raw_ingestion_log
            (
                file_name,
                source,
                destination,
                status
            )
            VALUES
            (
                '{file_name}',
                '{source}',
                '{destination}',
                '{status}'
            );
        '''

        return query

    @staticmethod
    def silver_load_log(file_name, source, destination, status, method='overwrite'):
        query = f'''
            INSERT INTO metadata.silver_ingestion_log
            (
                file_name,
                source,
                destination,
                method,
                status
            )
            VALUES
            (
                '{file_name}',
                '{source}',
                '{destination}',
                '{method}',
                '{status}'
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

    def pipeline_run_log():
        pass