import json
import requests
import os
from dotenv import load_dotenv
from datetime import datetime,timedelta
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
    
    # Define the task to fetch data from API 1
    def fetch_nytimes_books(self):
        requestUrl = f"https://api.nytimes.com/svc/books/v3/lists.json?list=hardcover-fiction&api-key={self.api_key}"
            #headers={'Content-Type':'Application/json','x-api-key':self.api_key,'Authorization':self.secret_key}
        params = {
            "Accept": "application/json"}
        for attempt in range(self.max_retries):
            try:
                response = requests.request('GET',requestUrl,headers=params)
                if response.status_code == 429: # Rate limiting
                    print("Rate limit exceeded, retrying after delay...")
                    time.sleep(self.rate_limit_delay)
                    continue
                response.raise_for_status() # Raise an HTTPError for bad responses
                if response.status_code==200:
                    data = response.json()
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
        #OpenBookUrl = "https://openlibrary.org/books/OL7353617M.json"
        OpenBookUrl = f"https://openlibrary.org/isbn/{isbn13}/.json"
        # Make the GET request
        #--
        for attempt in range(self.max_retries):
            try:
                response = requests.get(OpenBookUrl)
                if response.status_code == 429: # Rate limiting
                    print("Rate limit exceeded, retrying after delay...")
                    time.sleep(self.rate_limit_delay)
                    continue
                response.raise_for_status() # Raise an HTTPError for bad responses
                if response.status_code==200:
                    data = response.json()
                    if len (data)>10:
                        return data
                    else:
                        OpenBookUrl = f"https://openlibrary.org/isbn/{isbn10}/.json"
                        response = requests.get(OpenBookUrl)
                        if response.status_code==200:
                            data = response.json()
                            if len (data)>10:
                                return data
                            else:
                                print("Tried Two isbn 10 & 13, both are not able to fetch data")
                    return data
                else:
                    print(f"error {response.status_code}")
            except requests.exceptions.RequestException as e:
                print(f"Request failed: {e}, retrying...")
                time.sleep(self.retry_delay)
            raise Exception(f"Failed to fetch data from {self.requestUrl} after {self.max_retries} attempts")
        return data
    
