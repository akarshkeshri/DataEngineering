import os
import configparser
import psycopg2
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
class database:
    def __init__(self,data):
        load_dotenv()
        # Create a ConfigParser object
        config = configparser.ConfigParser()

        # Read the INI file
        config.read('api.ini')
        usename = config['postgres']['username']
        password = os.getenv('postgrespassword')
        port = config['postgres']['port']
        database = config['postgres']['dbname']
        
        # make Postgres connection.
        self.data=data
        conn_params={
            'db_name':config['postgres']['dbname'],
            'user':config['postgres']['username'],
            'password':os.getenv('postgrespassword'),
            'host':config['postgres']['host'],
            'port':config['postgres']['port']
            
        }
        try:
            self.conn=psycopg2.connect(**conn_params)
            print('Connection successful')
        except Exception as e:
            print(f"Error connecting to database: {e}")
            
        # Replace with your actual database credentials
        self.engine = create_engine('postgresql+psycopg2://username:password@host:port/database')
            
        #Create a cursor object
        return self.conn,self.engine
    def load_to_postgres(self,df):
        #Use batch insert
        #Optimize database performance with indexing
        try:
            #Use batch insert
            print('Started writting data in database')
            df.to_sql('table_name', self.engine, if_exists='append', index=False, method = 'multi')
        except Exception as e:
            print(e)
            print('Failed to write in Postgres')
        try:
            print('Started indexing of database')
            with self.engine.connect() as conn:
                self.conn.execute(text("CREATE INDEX idx_column1 ON table_name (column1);"))
                self.conn.execute(text("CREATE INDEX idx_column2 ON table_name (column2);"))
        except Exception as e:
            print(e)


        pass
    