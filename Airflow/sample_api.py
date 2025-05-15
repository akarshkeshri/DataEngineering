import json
import requests
import os
from dotenv import load_dotenv

class sample_api:
    def __init__(self):
        #self.endpoint=endpoint
        load_dotenv()
        # Access the secrets
        self.api_key = os.getenv('NewYork_API_KEY')
        self.secret_key = os.getenv('NewYork_SECRET_KEY')

    def NewYork_api_call(self,api_name):
        requestUrl = f"https://api.nytimes.com/svc/books/v3/lists.json?list=hardcover-fiction&api-key={self.api_key}"
        #headers={'Content-Type':'Application/json','x-api-key':self.api_key,'Authorization':self.secret_key}
        
        params = {
            "Accept": "application/json"}
        try:
            response = requests.request('GET',requestUrl,headers=params)
        except Exception as e:
            print(e)
        if response.status_code==200:
            data=response.json()
            # text =json.dumps(data, indent=4)
            # with open('sampleNewYorkData.txt','w') as wr:
            #     wr.write(text)
            print(data)
            return data
        else:
            print(f"error {response.status_code}")
        
    def NewYork_parse_json(self,data):
        for i in data['results']:
            print(i)
        return i
    def openBookLibrary():
        OpenBookUrl = "https://openlibrary.org/books/OL7353617M.json"
        # Make the GET request
        try:
            response = requests.get(OpenBookUrl)
        except Exception as e:
            print(e)

        # Check if the request was successful
        if response.status_code == 200:
            # Parse the JSON response
            data = response.json()
            print(data)
        else:
            print(f"Error: {response.status_code}")
if __name__=="__main__":
    instance = sample_api()
    api_name = 'NewYorkBook_api'
    data = instance.NewYork_api_call(api_name)
    parsed_data = instance.NewYork_parse_json(data)
        
            
        