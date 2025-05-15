
from data_call import *
from data_transformation import *
from logging import *
from database import *
from datetime import datetime,timedelta
import pytz


# if __name__=="__main__":
#     instance1=fetch_api()
#     instance2=transformation()
#     instance3=database()



database_instance = database()

today_date_time = datetime.now(pytz.timezone('Asia/Kolkata'))
# Step1: Calling Api to fetch data
instance1=fetch_api()
data1 = instance1.fetch_nytimes_books()

instance2=transformation(data1)

#Writting to Postgres
database_instance.load_to_postgres(data1)
#Transforming the data
data_Ny = instance2._NyData(data1)
# Bifurcate isbn10 & isbn13 and pass to openBookLibrary
isbn10=[j[0] for i,j in data_Ny.items()]
isbn13=[j[1] for i,j in data_Ny.items()]


# Calling api for each isbn
if len(isbn10)>=len(isbn13):
    length = isbn10
else:
    length=isbn13
for i in range(length-1):
    data2 = instance1.fetch_openlibrary_data(isbn10[i],isbn13[i])
    #Loading data from api and ingesting in Postgres
    database_instance.load_to_postgres(data2)




# Step2: Performing data transformation
'''
parsing
Normalization
Enrichment
Joining

'''

data2 = instance2._OpenBook()


data1,data2 = instance2.transform_and_enrich_data(data1,data2)
# Performing data quality checks
if instance2.data_quality_check(data1,data2):
    pass
else:
    print('Failed data quality check')
    print(f'Starting to log incident---->{today_date_time}')
    
print(data1)


