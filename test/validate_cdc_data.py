import argparse
import logging
import time
import pandas as pd
import boto3
from deltalake import DeltaTable
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CDCSimpleValidator:
    def __init__(self, pg_host, pg_port, pg_user, pg_password, pg_database, pg_table,
                 s3_endpoint, s3_access_key, s3_secret_key, delta_bucket, delta_prefix, key_column="customerId"):
        """Initialize the simple CDC validator."""
        # PostgreSQL connection
        self.pg_port = int(pg_port)
        self.pg_host = pg_host
        self.pg_user = pg_user
        self.pg_password = pg_password
        self.pg_database = pg_database
        self.pg_table = pg_table
        self.pg_conn_string = f'postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{self.pg_port}/{pg_database}'
        
        # S3/Delta settings
        self.s3_endpoint = s3_endpoint
        self.s3_access_key = s3_access_key
        self.s3_secret_key = s3_secret_key
        self.delta_bucket = delta_bucket
        self.delta_prefix = delta_prefix
        self.key_column = key_column
        
        # Initialize connections
        self.pg_engine = None
        self.s3_client = None
        
    def connect_to_postgresql(self, max_retries=3, retry_delay=2):
        """Connect to PostgreSQL source database with retries."""
        retries = 0
        while retries < max_retries:
            try:
                logger.info(f"Attempting PostgreSQL connection to {self.pg_host}:{self.pg_port}")
                self.pg_engine = create_engine(
                    self.pg_conn_string, 
                    connect_args={"connect_timeout": 10}
                )
                
                # Test connection
                with self.pg_engine.connect() as conn:
                    conn.execute(text("SELECT 1")).fetchone()  # Use text() here
                
                logger.info(f"Connected to PostgreSQL database")
                return True
            except Exception as e:
                logger.error(f"PostgreSQL connection failed: {str(e)}")
                retries += 1
                if retries < max_retries:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.error("Could not connect to PostgreSQL.")
                    raise
    
    def connect_to_s3(self):
        """Connect to S3/MinIO."""
        try:
            # Get the endpoint URL without http:// prefix
            endpoint = self.s3_endpoint
            if endpoint.startswith('http://'):
                endpoint = endpoint[7:]
            elif endpoint.startswith('https://'):
                endpoint = endpoint[8:]
                
            self.s3_client = boto3.client(
                's3',
                endpoint_url=self.s3_endpoint,
                aws_access_key_id=self.s3_access_key,
                aws_secret_access_key=self.s3_secret_key,
                use_ssl=self.s3_endpoint.startswith('https'),
                verify=False,
                config=boto3.session.Config(signature_version='s3v4')
            )
            
            # Test connection
            self.s3_client.list_buckets()
            logger.info(f"Connected to S3 at {self.s3_endpoint}")
            return True
        except Exception as e:
            logger.error(f"S3 connection failed: {str(e)}")
            raise
    
    def get_postgresql_data(self):
        """Read data from PostgreSQL into a pandas DataFrame."""
        try:
            with self.pg_engine.connect() as connection:
                query = f'SELECT * FROM "{self.pg_table}"'
                pg_data = pd.read_sql_query(query, connection)
                logger.info(f"Retrieved {len(pg_data)} records from PostgreSQL")
                return pg_data
        except Exception as e:
            logger.error(f"Failed to read PostgreSQL data: {str(e)}")
            raise
    
    def get_delta_data(self):
        """Read data from Delta table using deltalake library."""
        try:
            # Create a storage options dict for S3
            storage_options = {
                "endpoint_url": self.s3_endpoint,
                "access_key_id": self.s3_access_key,
                "secret_access_key": self.s3_secret_key,
                "region": "us-east-1",  # Default region
                "s3_allow_unsafe_rename": "true",
            }
            
            # Construct the S3 path
            table_path = f"s3://{self.delta_bucket}/{self.delta_prefix}"
            
            # Load Delta Table
            delta_table = DeltaTable(table_path, storage_options=storage_options)
            
            # Convert to pandas DataFrame
            delta_df = delta_table.to_pandas()
            logger.info(f"Retrieved {len(delta_df)} records from Delta table")
            return delta_df
        except Exception as e:
            logger.error(f"Failed to read Delta table data: {str(e)}")
            raise
    
    def compare_data(self, pg_data, delta_data):
        """Compare PostgreSQL and Delta table data."""
        # Sort both DataFrames by the key column
        pg_sorted = pg_data.sort_values(by=self.key_column).reset_index(drop=True)
        delta_sorted = delta_data.sort_values(by=self.key_column).reset_index(drop=True)
        
        # Check columns (ignoring timestamp in delta)
        pg_cols = set(pg_sorted.columns)
        delta_cols = set(delta_sorted.columns)
        delta_cols.discard('timestamp')  # Ignore timestamp column
        
        missing_in_delta = pg_cols - delta_cols
        extra_in_delta = delta_cols - pg_cols
        
        if missing_in_delta:
            logger.warning(f"Columns missing in Delta table: {missing_in_delta}")
        if extra_in_delta:
            logger.warning(f"Extra columns in Delta table: {extra_in_delta}")
        
        # Check keys
        pg_keys = set(pg_sorted[self.key_column])
        delta_keys = set(delta_sorted[self.key_column])
        
        missing_keys = pg_keys - delta_keys
        extra_keys = delta_keys - pg_keys
        
        if missing_keys:
            logger.warning(f"Records missing in Delta table: {missing_keys}")
        if extra_keys:
            logger.warning(f"Extra records in Delta table: {extra_keys}")
        
        # Compare matching records
        common_keys = pg_keys.intersection(delta_keys)
        logger.info(f"Found {len(common_keys)} matching keys to compare")
        
        mismatches = []
        for key in common_keys:
            pg_row = pg_sorted[pg_sorted[self.key_column] == key].iloc[0]
            delta_row = delta_sorted[delta_sorted[self.key_column] == key].iloc[0]
            
            for col in pg_cols.intersection(delta_cols):
                if pd.isna(pg_row[col]) and pd.isna(delta_row[col]):
                    continue  # Both are NaN/None
                elif pg_row[col] != delta_row[col]:
                    mismatches.append((key, col, pg_row[col], delta_row[col]))
                    break  # Only report first mismatch per record
        
        result = {
            'postgres_records': len(pg_sorted),
            'delta_records': len(delta_sorted),
            'common_records': len(common_keys),
            'missing_in_delta': len(missing_keys),
            'extra_in_delta': len(extra_keys),
            'value_mismatches': len(mismatches),
            'mismatched_details': mismatches,
            'is_consistent': (len(missing_keys) == 0 and len(extra_keys) == 0 and len(mismatches) == 0)
        }
        
        return result
    
    def validate(self):
        """Run the complete validation process."""
        # Setup connections
        self.connect_to_postgresql()
        self.connect_to_s3()
        
        # Get data
        pg_data = self.get_postgresql_data()
        delta_data = self.get_delta_data()
        
        # Compare data
        result = self.compare_data(pg_data, delta_data)
        
        # Print summary
        logger.info("=" * 60)
        logger.info("CDC Validation Summary")
        logger.info("=" * 60)
        logger.info(f"PostgreSQL records: {result['postgres_records']}")
        logger.info(f"Delta table records: {result['delta_records']}")
        logger.info(f"Common records: {result['common_records']}")
        logger.info(f"Records missing in Delta: {result['missing_in_delta']}")
        logger.info(f"Extra records in Delta: {result['extra_in_delta']}")
        logger.info(f"Records with value mismatches: {result['value_mismatches']}")
        
        # Show mismatch details
        if result['mismatched_details']:
            logger.info("Mismatch details (showing up to 5):")
            for i, (key, col, pg_val, delta_val) in enumerate(result['mismatched_details'][:5]):
                logger.info(f"  Key {key}, Column '{col}': PG='{pg_val}', Delta='{delta_val}'")
                
        logger.info(f"Data consistent: {result['is_consistent']}")
        logger.info("=" * 60)
        
        return result

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Simple CDC Data Validator')
    # PostgreSQL connection
    parser.add_argument('--pg-host', default='localhost', help='PostgreSQL host')
    parser.add_argument('--pg-port', default='5433', help='PostgreSQL port')
    parser.add_argument('--pg-user', default='postgres', help='PostgreSQL user')
    parser.add_argument('--pg-password', default='postgres', help='PostgreSQL password')
    parser.add_argument('--pg-database', default='postgres', help='PostgreSQL database')
    parser.add_argument('--pg-table', default='links', help='PostgreSQL table name')
    
    # S3/MinIO and Delta configuration
    parser.add_argument('--s3-endpoint', default='http://127.0.0.1:9000', help='S3/MinIO endpoint')
    parser.add_argument('--s3-access-key', default='12345678', help='S3/MinIO access key')
    parser.add_argument('--s3-secret-key', default='12345678', help='S3/MinIO secret key')
    parser.add_argument('--delta-bucket', default='change-data-capture', help='S3 bucket containing Delta table')
    parser.add_argument('--delta-prefix', default='customers-delta', help='Prefix/path to Delta table within bucket') 
    parser.add_argument('--key-column', default='customerId', help='Primary key column for comparison')
    
    args = parser.parse_args()
    
    validator = CDCSimpleValidator(
        pg_host=args.pg_host,
        pg_port=args.pg_port,
        pg_user=args.pg_user,
        pg_password=args.pg_password,
        pg_database=args.pg_database,
        pg_table=args.pg_table,
        s3_endpoint=args.s3_endpoint,
        s3_access_key=args.s3_access_key,
        s3_secret_key=args.s3_secret_key,
        delta_bucket=args.delta_bucket,
        delta_prefix=args.delta_prefix,
        key_column=args.key_column
    )
    
    validator.validate()