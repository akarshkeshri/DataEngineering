import os
import pyodbc
import configparser
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pandas as pd

class database_fnc:
    def __init__(self):
        load_dotenv()
        # Create a ConfigParser object
        config = configparser.ConfigParser()
        config.read(r'DataEngineering/Airflow/config.ini')
        if not config.sections():
            raise FileNotFoundError("config.ini file is missing or empty, or the path is wrong.")
        load_dotenv()
        # Read the INI file
        
        print(config)
        self.username =  config['postgres']['username']
        self.password = os.getenv('postgrespassword')
        self.port =  config['postgres']['port']
        self.database =  config['postgres']['dbname']
        self.DSN= os.getenv('DSN')
        
        
        # make Postgres connection.
        try:
            self.conn = pyodbc.connect(
            f"DSN={self.DSN};UID={self.username};PWD={self.password}"
        )
            print('Connection successful')
            self.cursor = self.conn.cursor()
        except Exception as e:
            print(f"Error connecting to database: {e}")
            
        #Create a cursor object



    def load_to_postgres_NY(self,df,flag):
            #Use batch insert
            #Optimize database performance with indexing
            if flag==0:
                try:
                    # Create table (run once)
                    self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS nyt_BookDetail (
                        published_date DATE,
                        genres TEXT,
                        title TEXT,
                        author TEXT,
                        publisher TEXT,
                        description TEXT,
                        isbn10 TEXT,
                        isbn13 TEXT
                    );
                    """)
                    self.conn.commit()
                except Exception as e:
                    pass
                try:
                    #Use batch insert
                    print('Started writting data in database')

                    # Replace NaN with None to avoid errors
                    df = df.where(pd.notnull(df), None)
                    #  Prepare your insert query
                    insert_query = """
                    INSERT INTO nyt_BookDetail (
                        published_date, genres, title, author, publisher, description, isbn10, isbn13
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """

                    # Convert dataframe rows to list of tuples
                    data_tuples = [tuple(x) for x in df.to_numpy()]
                    self.cursor.fast_executemany = True  # Significantly faster for large batches
                    self.cursor.executemany(insert_query, data_tuples)
                    self.conn.commit()
                    print('Write successful of Ny data')
                    return None


                    #  Insert all rows from DataFrame
                    # for _, row in df.iterrows():
                    #     cursor.execute(insert_query, tuple(row))
                    # #Commit and close
                    # conn.commit()
                    # cursor.close()
                    # conn.close()


                    #-Normal Insert
                    # # Insert into DB
                    # cursor.execute("""
                    #     INSERT INTO nyt_books (published_date, genres, title, author, publisher, description, isbn10, isbn13)
                    #     VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    # """, (published_date, genres, title, author, publisher, description, isbn10, isbn13))

                    #df.to_sql('BookDetail', engine, if_exists='append', index=False)

                    #df.to_sql('BookDetail', engine, if_exists='append', index=False, method = 'multi')
                    
                except Exception as e:
                    print(e)
                    print('Failed to write in Postgres')
                    #return None
                try:
                    print('Started indexing of database')
                    #with engine.connect() as conn:
                        #conn.execute(text("CREATE INDEX idx_column1 ON table_name (column1);"))
                        #conn.execute(text("CREATE INDEX idx_column2 ON table_name (column2);"))
                except Exception as e:
                    print('Error in indexing')
                    print(e)
                    #return None
            # Insert data into Metadata
                
            elif flag==1:
                # Create table if not exists
                self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS Metadata (
                    id SERIAL PRIMARY KEY,
                    number_of_pages INTEGER,
                    language VARCHAR(10)
                )
                """)
                #Loading data from api and ingesting in Postgres
                num_pages = df.get('number_of_pages')
                language = df.get('languages')[0]['key'].split('/')[-1] if df.get('languages') else None
                # Insert extracted values
                self.cursor.execute("INSERT INTO Metadata (number_of_pages, language) VALUES (?, ?)", (num_pages, language))

                # Commit and close
                self.conn.commit()
                print("Inserted metadata successfully.")
            #Insert into SalesRanking table
            elif flag==2:
                self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS SalesRanking (
                    id SERIAL PRIMARY KEY,
                    list_name TEXT,
                    rank TEXT,#change this datatype in future
                    weeks_on_list TEXT#change this datatype in future
                )
                """)
                # Insert values
                
                insert_query2 = """
                                    INSERT INTO SalesRanking (
                                        list_name,rank, weeks_on_list
                                    ) VALUES (?, ?, ?)
                                    """
                df3 = df.applymap(lambda x: len(str(x)))
                print(df3.head)
                df3.head()

                # Convert dataframe rows to list of tuples
                #self.cursor.execute("INSERT INTO SalesRanking (list_name,rank, weeks_on_list) VALUES (?,?, ?)", (list_name, rank,weeks_on_list))

                data_tuples2 = [tuple(x) for x in df.to_numpy()]
                self.cursor.fast_executemany = True  # Significantly faster for large batches
                
                self.cursor.executemany(insert_query2, data_tuples2)
                self.conn.commit()
                print('Created table')
                print('Data written in Salesrankin table')
    
    def load_SalesRanking_table(self,df,flag):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS TData (
            id SERIAL PRIMARY KEY,
            list_name TEXT,
            rank INTEGER,
            weeks_on_list INTEGER
        )
        """)

        # Insert values
        insert_query2 = """(
        INSERT INTO SalesRanking (
            list_name, rank, weeks_on_list
        ) VALUES (?, ?, ?)
        """

        # Convert dataframe rows to list of tuples
        data_tuples = [tuple(x) for x in df.to_numpy()]
        self.cursor.fast_executemany = True  # Significantly faster for large batches
        self.cursor.executemany(insert_query2, data_tuples)
        self.conn.commit()
        print('Data inserted for NewYork in postgres database')


    def read_data_from_postgres(self):
        try:
            return self.conn
        except Exception as e:
            print(e)

