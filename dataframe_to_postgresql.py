import pandas as pd
import argparse
from sqlalchemy import create_engine
import sys

def load_data_to_postgresql(args):
    try:
        # Read CSV file
        df = pd.read_csv(
            args.input_file,
            sep=args.separator,
            encoding='utf-8'
        )
        
        # Create PostgreSQL connection string
        connection_string = f"postgresql://{args.user}:{args.password}@{args.host}:{args.port}/{args.database}"
        
        # Create SQLAlchemy engine
        engine = create_engine(connection_string)
        
        # Write DataFrame to PostgreSQL
        df.to_sql(
            name=args.table,
            con=engine,
            if_exists='replace' if args.reset else 'append',
            index=False
        )
        
        print(f"Successfully loaded {len(df)} rows into table {args.table}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description='Load CSV data into PostgreSQL')
    
    parser.add_argument('-i', '--input_file', required=True,
                        help='Input CSV file path')
    parser.add_argument('-hst', '--host', required=True,
                        help='PostgreSQL host')
    parser.add_argument('-p', '--port', required=True, type=int,
                        help='PostgreSQL port')
    parser.add_argument('-s', '--separator', default=',',
                        help='CSV separator')
    parser.add_argument('-u', '--user', required=True,
                        help='PostgreSQL username')
    parser.add_argument('-psw', '--password', required=True,
                        help='PostgreSQL password')
    parser.add_argument('-db', '--database', required=True,
                        help='PostgreSQL database name')
    parser.add_argument('-t', '--table', required=True,
                        help='Target table name')
    parser.add_argument('-rst', '--reset', type=int, default=0,
                        help='Reset table if exists (1=True, 0=False)')
    parser.add_argument('-es', '--encoding_scheme', default='csv',
                        help='File encoding scheme')

    args = parser.parse_args()
    
    load_data_to_postgresql(args)

if __name__ == "__main__":
    main()