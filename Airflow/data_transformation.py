from datetime import datetime,timedelta
import time
import json
import threading
from database import database_fnc
import pandas as pd
import os

class transformation:
    def __init__(self):
          pass
        
    
    def _NyData(self,data1,flag):
        if flag==0:
            database_fncInstance = database_fnc()
            file_path = '/NyDataSample.txt'
            # try:
            #     if os.path.exists(file_path):

            #         os.chmod(file_path, 0o666)
            #         print("File permissions modified successfully!")
            #     else:
            #         print("File not found:", file_path)
            # except Exception as e:
            #     print(e)
            # with open ('NyDataSample.txt','w') as wr:
            #     wr.write(json.dumps(data1))
            #     print('Write successfull')
            try:
                for indbook,loop2 in enumerate(data1['results']['lists']):
                    records = []
                    record2=[]
                    if  loop2['list_name']=='Hardcover Fiction':
                            for inddes,loop3 in enumerate(loop2['books']):
                                    print(loop2['list_name'])
                                    published_date = data1['results']['published_date']
                                    description =  loop3['description']
                                    #print(inddes,book)
                                    title =loop3['title']
                                    #print(inddes,title)
                                    author = loop3['author']
                                    publisher = loop3['publisher']
                                    isbn10 = loop3["primary_isbn10"]
                                    isbn13 = loop3["primary_isbn13"]
                                    rank=loop3['rank']
                                    weeks_on_list=loop3['weeks_on_list']
                                    list_name = loop2['list_name']
                                    
                                    try:
                                            genres = loop3['list_name']
                                    except Exception as e:
                                            genres =None
                                    print(inddes,published_date, genres, title, author, publisher, description, isbn10, isbn13)

                                    records.append({
                                        "published_date": published_date,
                                        "genres": genres,
                                        "title": title,
                                        "author": author,
                                        "publisher": publisher,
                                        "description": description,
                                        "isbn10": isbn10,
                                        "isbn13": isbn13
                                    })
                                    #print(published_date, genres, title, author, publisher, description, isbn10, isbn13)
                                    #Creating DataFrame of fetched data
                                    df=pd.DataFrame(records)
                                    print(df.columns)

                                    flag=0#To insertin nybookdetail table
                                    #Writting to Postgres
                                    database_fncInstance.load_to_postgres_NY(df,flag)
                                    # Loading data to SalesRanking Table
                                    flag=2
                                    record2.append({"rank": rank,
                                        "list_name": list_name,
                                        "weeks_on_list": weeks_on_list})
                                    print(rank,list_name,weeks_on_list)
                                    df2=pd.DataFrame(record2)
                                    print('DF2 Columns',df2.columns)
                                    database_fncInstance.load_to_postgres_NY(df2,flag)
                                    #database_fncInstance.load_SalesRanking_table(df2,flag)
                                    
                                    print('Data inserted for NewYork in postgres database')
                    else:
                        continue
            except Exception as e:
                print('Error in writting Ny Data',e) 
                print('Error in data_transformation.py function _NYdata')                         
            return True
            
            #parsing
            #Normalization
            #Enrichment
            #Joining

    def _OpenBook(self):
        database_fncInstance = database_fnc()
        #df_ny=Load data of ny from postgres
        #df_openbook=Load data of openbook from postgres
        conn = database_fncInstance.read_data_from_postgres()
        query_10="""  Select isbn10 from nyt_bookdetail """
        query_13 = """  Select isbn13 from nyt_bookdetail """
        df1 = pd.read_sql(query_10, conn)
        df2 = pd.read_sql(query_13, conn)
        isbn10_list = df1['isbn10'].to_list()
        isbn13_list=df2['isbn13'].to_list()
        print(isbn10_list)
        #Perform join between two
        #df1.merge(df2)
        
        
        #parsing
        #Normalization
        #Enrichment
        #Joining
        return isbn10_list,isbn13_list
    def transform_and_enrich_data(self,data1,data2):
        data1 = self._NyData()
        data2 = self._OpenBook()
        #mechanism to check and detect schema change
        #include versioning
        return data1,data2
    def data_quality_check(self,data1,data2):
        #Check record count, data completeness and correctness
        #Enforce data quality rules
        pass
        
