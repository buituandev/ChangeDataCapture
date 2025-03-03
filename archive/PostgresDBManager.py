from typing import List, Tuple

import psycopg2.extras
import pandas as pd
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

class PostgresDBManager:
    def __init__(self):
        self.hostname = 'localhost'
        self.database = 'practice'
        self.username = 'postgres'
        self.pwd = 'tuansql2003'
        self.port_id = '5432'
        self.conn = None
    
    def connect(self):
        self.conn = psycopg2.connect(
            host=self.hostname,
            database=self.database,
            user=self.username,
            password=self.pwd,
            port=self.port_id
        )

    def create_table(self) -> None:
        create_script = '''
        create table if not exists employees
        (
            employee_id serial primary key,
            first_name varchar(50),
            last_name varchar(50),
            email varchar(100),
            phone_number varchar(20),
            salary numeric(10, 2)
        );
        '''
        with self.conn.cursor() as cur:
            cur.execute('drop table if exists employees;')
            cur.execute(create_script)
            self.conn.commit()

    def insert_employees(self, employees: List[Tuple]) -> None:
        insert_script = '''
        insert into employees (first_name, last_name, email, phone_number, salary) 
        values (%s, %s, %s, %s, %s);
        '''
        with self.conn.cursor() as cur:
            for record in employees:
                cur.execute(insert_script, record)
            self.conn.commit()

    def get_all_employees(self) -> List[dict]:
        with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute('select * from employees;')
            return cur.fetchall()

    def update_employee_salaries(self, salary_updates: List[Tuple[float, int]]) -> None:
        update_script = 'update employees set salary = %s where employee_id = %s;'
        with self.conn.cursor() as cur:
            for record in salary_updates:
                cur.execute(update_script, record)
            self.conn.commit()

    def delete_employee(self, employee_id: int) -> None:
        delete_script = 'delete from employees where employee_id = %s;'
        with self.conn.cursor() as cur:
            cur.execute(delete_script, (employee_id,))
            self.conn.commit()

    def close_connection(self) -> None:
        if self.conn is not None:
            self.conn.close()
            
    def get_dataframe(self, query: str) -> pd.DataFrame:
        return pd.read_sql(query, self.conn)
def main():
    db = PostgresDBManager()
    try:
        db.connect()
        db.create_table()
        sample_employees = [
            ('Cardi', 'B', 'cardib@gmail.com', '123456789', 50000),
            ('Nicki', 'C', 'nickic@a.com', '987654321', 60000)
        ]
        db.insert_employees(sample_employees)
        
        print("Initial employees:")
        print(db.get_dataframe('select * from employees;'))
        
        salary_updates = [(70000, 1), (80000, 2)]
        db.update_employee_salaries(salary_updates)
        db.delete_employee(1)
        
        print("\nFinal employees after updates and deletion:")
        print(db.get_dataframe('select * from employees;'))
    except Exception as e:
        print(f"Error: {e}")
    finally:
        db.close_connection()

if __name__ == "__main__":
    main()