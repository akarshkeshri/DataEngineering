from datetime import datetime,timedelta
import time
import json
import threading
from database import database

class transformation:
    def __init__(self,data1):
        self.data1=data1
        
    def _NyData(self,data1):
        data1=data1
        best_seller_list ={}
        for i in data1['results']:
            isbn10=''
            isbn13=''
            title=''
            if i['list_name']=='Hardcover Fiction':
                for j in i['book_details']:
                    print(j['title'])

                for k in i['isbns']:
                    isbn10 = k['isbn10']
                    isbn13 = k['isbn13']
                    best_seller_list[j['title']]= [isbn10,isbn13]
        return best_seller_list
        
        #parsing
        #Normalization
        #Enrichment
        #Joining
        return data1
    def _OpenBook(self,nydata,openbookdata):
        #df1=Load data of ny from postgres
        #df2=Load data of openbook from postgres
        #Perform join between two
        #df1.merge(df2)
        
        
        #parsing
        #Normalization
        #Enrichment
        #Joining
        pass
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
        
