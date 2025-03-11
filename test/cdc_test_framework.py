import argparse
import time
import random
import logging
from datetime import datetime
from faker import Faker
import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, text
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# python test/cdc_test_framework.py -hst localhost -p 5433 -u postgres -psw postgres -db postgres -t links -tm 1

class CDCTestFramework:
    def __init__(self, host, port, user, password, database, table_name, runtime_minutes=5):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.table_name = table_name
        self.runtime_minutes = runtime_minutes
        self.conn_string = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'
        self.engine = None
        self.faker = Faker()
        
        # Operation counters
        self.insert_count = 0
        self.update_count = 0
        self.delete_count = 0
        self.customer_ids = []
        
        # Connect to database
        self.connect_to_db()
        
    def connect_to_db(self):
        """Establish database connection"""
        try:
            self.engine = create_engine(self.conn_string)
            logger.info(f"Connected to PostgreSQL at {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"Database connection failed: {str(e)}")
            raise
            
    def setup_table(self):
        """Create customer table if it doesn't exist"""
        try:
            metadata = MetaData()
            Table(
                self.table_name, metadata,
                Column('customerId', Integer, primary_key=True),
                Column('customerFName', String),
                Column('customerLName', String),
                Column('customerEmail', String),
                Column('customerPassword', String),
                Column('customerStreet', String),
                Column('customerCity', String),
                Column('customerState', String),
                Column('customerZipcode', Integer)
            )
            
            # Create the table if it doesn't exist
            metadata.create_all(self.engine)
            logger.info(f"Table {self.table_name} setup complete")
        except SQLAlchemyError as e:
            logger.error(f"Table setup failed: {str(e)}")
            raise
            
    def generate_customer_data(self, customer_id=None):
        """Generate fake customer data"""
        if customer_id is None:
            customer_id = random.randint(10000, 99999)
            
        return {
            'customerId': customer_id,
            'customerFName': self.faker.first_name(),
            'customerLName': self.faker.last_name(),
            'customerEmail': self.faker.email(),
            'customerPassword': self.faker.password(),
            'customerStreet': self.faker.street_address(),
            'customerCity': self.faker.city(),
            'customerState': self.faker.state_abbr(),
            'customerZipcode': int(self.faker.zipcode()[:5])
        }
        
    def insert_customer(self):
        """Insert a new customer into the database"""
        customer_data = self.generate_customer_data()
        try:
            df = pd.DataFrame([customer_data])
            df.to_sql(self.table_name, self.engine, if_exists='append', index=False)
            
            # Store customer ID for potential updates/deletes
            self.customer_ids.append(customer_data['customerId'])
            self.insert_count += 1
            logger.info(f"INSERT - Customer {customer_data['customerId']}")
        except Exception as e:
            logger.error(f"Insert operation failed: {str(e)}")
            
    def update_customer(self):
        """Update an existing customer"""
        if not self.customer_ids:
            logger.info("No customers to update, performing insert instead")
            self.insert_customer()
            return
            
        customer_id = random.choice(self.customer_ids)
        updated_data = self.generate_customer_data(customer_id)
        
        try:
            df = pd.DataFrame([updated_data])
            # Use sqlalchemy to update by customer_id
            with self.engine.begin() as conn:
                # Delete the existing row
                conn.execute(
                    text(f"DELETE FROM {self.table_name} WHERE \"customerId\" = :id"),
                    {"id": customer_id}
                )
                # Insert the updated row
                df.to_sql(self.table_name, conn, if_exists='append', index=False)
            
            self.update_count += 1
            logger.info(f"UPDATE - Customer {customer_id}")
        except Exception as e:
            logger.error(f"Update operation failed: {str(e)}")
            
    def delete_customer(self):
        """Delete an existing customer"""
        if not self.customer_ids:
            logger.info("No customers to delete, performing insert instead")
            self.insert_customer()
            return
            
        customer_id = random.choice(self.customer_ids)
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text(f"DELETE FROM {self.table_name} WHERE \"customerId\" = :id"),
                    {"id": customer_id}
                )
            
            self.customer_ids.remove(customer_id)
            self.delete_count += 1
            logger.info(f"DELETE - Customer {customer_id}")
        except Exception as e:
            logger.error(f"Delete operation failed: {str(e)}")
            
    def get_current_count(self):
        """Get current row count in the database"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {self.table_name}"))
                return result.scalar()
        except Exception as e:
            logger.error(f"Count query failed: {str(e)}")
            return -1
            
    def run_test(self):
        """Run the CDC test for the specified duration"""
        logger.info(f"Starting CDC test for {self.runtime_minutes} minutes")
        
        # Setup table
        self.setup_table()
        
        # Initial inserts to have some data
        for _ in range(10):
            self.insert_customer()
        
        operations = ['insert', 'update', 'delete']
        weights = [0.5, 0.3, 0.2]  # 50% inserts, 30% updates, 20% deletes
        
        start_time = time.time()
        end_time = start_time + (self.runtime_minutes * 60)
        
        while time.time() < end_time:
            # Select operation based on weights
            operation = random.choices(operations, weights=weights, k=1)[0]
            
            if operation == 'insert':
                self.insert_customer()
            elif operation == 'update':
                self.update_customer()
            else:  # delete
                self.delete_customer()
                
            # Sleep between operations
            time.sleep(random.uniform(0.5, 2.0))
            
        # Print summary
        final_count = self.get_current_count()
        expected_count = self.insert_count - self.delete_count  # Updates don't change count
        
        logger.info("=" * 50)
        logger.info("CDC Test Summary")
        logger.info("=" * 50)
        logger.info(f"Inserts performed: {self.insert_count}")
        logger.info(f"Updates performed: {self.update_count}")
        logger.info(f"Deletes performed: {self.delete_count}")
        logger.info(f"Total operations: {self.insert_count + self.update_count + self.delete_count}")
        logger.info(f"Expected final count: {expected_count}")
        logger.info(f"Actual database count: {final_count}")
        logger.info(f"Data consistent: {expected_count == final_count}")
        logger.info("=" * 50)
        
        return {
            'inserts': self.insert_count,
            'updates': self.update_count,
            'deletes': self.delete_count,
            'expected_count': expected_count,
            'actual_count': final_count,
            'is_consistent': expected_count == final_count
        }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='CDC Test Framework')
    parser.add_argument('-hst', '--host', default='localhost', help='PostgreSQL host')
    parser.add_argument('-p', '--port', default='5432', help='PostgreSQL port')
    parser.add_argument('-u', '--user', default='postgres', help='PostgreSQL user')
    parser.add_argument('-psw', '--password', default='postgres', help='PostgreSQL password')
    parser.add_argument('-db', '--database', default='postgres', help='PostgreSQL database')
    parser.add_argument('-t', '--table', default='customers', help='Target table name')
    parser.add_argument('-tm', '--time', type=int, default=5, help='Test runtime in minutes')
    
    args = parser.parse_args()
    
    test_framework = CDCTestFramework(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
        table_name=args.table,
        runtime_minutes=args.time
    )
    
    test_framework.run_test()