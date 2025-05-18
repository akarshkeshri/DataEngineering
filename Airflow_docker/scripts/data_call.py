import json
import requests
import os
from dotenv import load_dotenv
from datetime import datetime,timedelta
from database import database_fnc
import time

class fetch_api:
    def __init__(self):
        #self.endpoint=endpoint
        load_dotenv()
        # Access the secrets
        self.api_key = os.getenv('NewYork_API_KEY')
        self.secret_key = os.getenv('NewYork_SECRET_KEY')
        self.initial_delay = 5 # seconds
        self.max_retries = 3
        self.retry_delay = 5 # seconds
        self.rate_limit_delay = 1 # seconds
        print('Loading init in data_call module')

    # Define the task to fetch data from API 1
    def fetch_nytimes_books(self):
        
        requestUrl = f"https://api.nytimes.com/svc/books/v3/lists/overview.json?api-key={self.api_key}"
            #headers={'Content-Type':'Application/json','x-api-key':self.api_key,'Authorization':self.secret_key}
        params = {
            "Accept": "application/json"}
        # for attempt in range(self.max_retries):
        result_return=False
        while result_return==False:
            try:
                response = requests.get(requestUrl, headers=params)
                if response.status_code == 429: # Rate limiting
                    print("Rate limit exceeded, retrying after delay...")
                    time.sleep(self.rate_limit_delay)
                    continue
                response.raise_for_status() # Raise an HTTPError for bad responses
                if response.status_code==200:
                    data = response.json()
                    result_return=True
                    return data
                else:
                    print(f"error {response.status_code}")
            except requests.exceptions.RequestException as e:
                print(f"Request failed: {e}, retrying...")
                time.sleep(self.retry_delay)
            raise Exception(f"Failed to fetch data from {requestUrl} after {self.max_retries} attempts")
        return data

    # Define the task to fetch data from API 2
    def fetch_openlibrary_data(self,isbn10,isbn13):
        #Flag 1 signify insert in Metadata table
        flag=1
        if (isbn10 =='' and isbn13 =='') or (isbn10 ==None and isbn13 == None) or (isbn10 == 'null' and isbn13 =='null'):
            print('All isbn null so skipping book record of this data')
        elif isbn13 =='' or isbn13 == None or isbn13 =='null':
            isbn=isbn10
        else:
            isbn=isbn13
        #OpenBookUrl = "https://openlibrary.org/books/OL7353617M.json"
        OpenBookUrl = f"https://openlibrary.org/isbn/{isbn}/.json"
        # Make the GET request
        #--
        result_return=False
        while result_return==False:
        #for attempt in range(self.max_retries):
            try:
                response = requests.get(OpenBookUrl)
                if response.status_code == 429: # Rate limiting
                    print("Rate limit exceeded, retrying after delay...")
                    time.sleep(self.rate_limit_delay)
                    result_return=False
                    return data,response.status_code
                elif response.status_code==200:
                    data = response.json()
                    print(data)
                    if len (data)>10:
                        result_return=True
                        return data,response.status_code
                    else:
                        return None,response.status_code
                else:
                    return None,response.status_code
            except requests.exceptions.RequestException as e:
                print(f"Request failed: {e}, retrying...")
                time.sleep(self.retry_delay)
        return data,response.status_code
    
