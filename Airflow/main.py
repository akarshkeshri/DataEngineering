
from data_call import fetch_api
from data_transformation import transformation
from data_logging import implementing_logging
from database import database_fnc 
from datetime import datetime,timedelta
import pytz

today_date_time = datetime.now(pytz.timezone('Asia/Kolkata'))


dataCallInstance = fetch_api()

# Step1: Calling Api to fetch data
data1 = dataCallInstance.fetch_nytimes_books()

transformationInstance=transformation()

database_fncInstance = database_fnc()


#Transforming the data to dataframe
#Flag 0 signify insert in nyt_bookdetail table
transformationInstance._NyData(data1,0)

# Bifurcate isbn10 & isbn13 and pass to openBookLibrary

isbn10_list,isbn13_list = transformationInstance._OpenBook()
# Calling api for each isbn
if len(isbn10_list)>=len(isbn13_list):
    length = len(isbn10_list)
else:
    length=len(isbn13_list)
for i in range(length-1):
    #Flag 1 signify insert in Metadata table
    flag=1
    data,response = dataCallInstance.fetch_openlibrary_data(isbn10_list[i],isbn13_list[i])
    try:
        database_fncInstance.load_to_postgres_NY(data,flag)
    except AttributeError as e:
        if "'NoneType' object has no attribute 'get'" in str(e):
            print('No data for isbn {isbn10_list[i]} {isbn13_list[i]}')
            continue
        

