import pandas as pd
import fixerio
import requests
from datetime import  timedelta, datetime as dt
from collections import OrderedDict
import json
import pickle
from os import getenv
import numpy as np
from dotenv import load_dotenv
import os
import sqlite3
from sqlalchemy import create_engine



load_dotenv()
#hide the api_key
api_key = os.environ.get('apikey')
os.chdir = "/home/kla/Documents/"
outfile = "/home/kla/Documents/forex_calls.json"
#establish the timeline: the last 180 days from the current date

today = dt.today()
past = today + timedelta(-180)
dates_list = []

def get_dates_list():
    """This function creates a list of the dates between today and the last 180 days and transforms them from datetime objs to str"""
    
    delta = today - past

    for i in range(delta.days + 1):
        day = past + timedelta(days=i)
        day = day.strftime("%Y-%m-%d")
        dates_list.append(day)
    return dates_list
    
api_calls = []

def get_forex_data(dates_list):
    """Retrieves historical data of selected currencies based on the euro from fixer.io, saves it to a json file."""

    for i, each_date in enumerate(dates_list,1):
        url = "http://data.fixer.io/api/" + each_date +"?access_key=" + api_key + "&base=" + "EUR" + "&symbols=" + "USD,GBP,JPY"
        base_rate = requests.get(url)
        base_rate_data = base_rate.json()
        print(i, base_rate_data)
        api_calls.append(base_rate_data)


        with open('forex_calls.json', 'w') as outfile:
            json.dump(api_calls, outfile)
    
    return outfile

get_forex_data(dates_list)


api_data = []

def create_dataframe(outfile):
    """Parses json file and creates a dataframe based on currencies and dates."""

    with open(outfile, 'r') as infile:
        data = infile.read()
        results = json.loads(data)

        for each_call in results:

            row = OrderedDict()
            row['date'] = each_call['date']
            row['USD'] = each_call['rates']['USD']
            row['GBP'] = each_call['rates']['GBP']
            row['JPY'] = each_call['rates']['JPY']

            api_data.append(row)

        api_df = pd.DataFrame(api_data)

        return api_df
        
 def run_query(): 
    """pickles the data and queries it to get the maximum rate per currency"""
    forex_df = create_dataframe(outfile) #adding this here in case one does not want to call the dataframe first
    forex_df.to_pickle('forex_df.pkl')
    api_pickle = pickle.load(open("/home/kla/Documents/forex_df.pkl", "rb"))


    engine = sqlite3.connect(":memory:")
    api_pickle.to_sql(name='forex_rates', con=engine)
    cur = engine.cursor()
    query = engine.execute("SELECT 'USD' AS currency, max(USD) AS max_rate FROM forex_rates UNION SELECT 'GBP', max(GBP) FROM forex_rates UNION SELECT  'JPY', max(JPY) FROM forex_rates").fetchall()
    formatted_result = [f"{currency} {max_rate }" for currency, max_rate in query]
    currency, max_rate = "currency ", " max_rate"
    query_results = print('\n'.join([f"{currency}{max_rate}"] + formatted_result))

    return query_results


if __name__ == '__main__':
    get_dates_list()
    get_forex_data(dates_list)
    create_dataframe(outfile)
    run_query()
