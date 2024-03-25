import pandas as pd
import numpy as np
import sklearn
from datetime import datetime
import os
import glob

import timescaledb_model as tsdb

db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
#db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker

def store_file(name, website):
    if db.is_file_done(name):
        return
    if website.lower() == "boursorama":
        market_id = "0"
        market_alias = name.split()[0]
        if market_alias == "peapme":
            market_id = db.raw_query("SELECT id FROM markets WHERE alias = %s", ("euronx",))[0][0]
            pea = True
        else:
            market_id = db.raw_query("SELECT id FROM markets WHERE alias = %s", (market_alias,))[0][0]
            pea = False

        try:
            df = pd.read_pickle("data/boursorama/" + name)  # is this dir ok for you ?
        except:
            year = name.split()[1].split("-")[0]
            df = pd.read_pickle("data/boursorama/" + year + "/" + name)


        df['last'] = df['last'].str.replace('(c)', '')
        df['last'] = df['last'].str.replace('(s)', '')
        df['last'] = df['last'].str.replace(' ', '').astype(float)

        #DATE
        date_string = name.split(' ')[1] + " " +  name.split(' ')[2]
        date_string = date_string.replace('.bz2', '')

        companies = df['name'].unique()

        #COMPANY
        columns = ['name', 'mid', 'symbol', 'symbol_nf', 'isin', 'reuters', 'boursorama', 'pea', 'sector']
        new_companies = []

        for name in companies:
            company_id = db.search_company_id(name)
            if company_id == 0:
                new_companies.append({'name': name, 'mid': int(market_id), 'pea': pea})

        if new_companies != []:
            new_companies = pd.DataFrame(new_companies, columns=columns)
            db.df_write(new_companies, 'companies', index=False, if_exists='append')
            print("New companies added to the database.")
        else:
            print("No new companies to add to the database.")


        #STOCKS
        stocks_df = pd.DataFrame({'date': [date_string] * len(df),
                                    'cid': df['name'].apply(lambda x: db.search_company_id(x)),
                                    'value': df['last'],
                                    'volume': df['volume']})
        db.df_write(stocks_df, 'stocks', index=False, if_exists='append')


        #DAYSTOCKS
        daystocks_df = pd.DataFrame({'date': [date_string] * len(df),
                                     'cid': df['name'].apply(lambda x: db.search_company_id(x)),
                                     'close': df['last'],
                                     'volume': df['volume']
        })

        daystocks_df['open'] = stocks_df.groupby('cid')['value'].transform('first')
        daystocks_df['high'] = stocks_df.groupby('cid')['value'].transform('max')
        daystocks_df['low'] = stocks_df.groupby('cid')['value'].transform('min')

        db.df_write(daystocks_df, 'daystocks', index=False, if_exists='append')
                                     
        db.df_write(pd.DataFrame({'name': [name]}), 'file_done', index=False, if_exists='append')
        print("File stored in the database.")

        




if __name__ == '__main__':
    TEST = True
    if TEST:
        path = "data/boursorama/2020"

        for root, dirs, files in os.walk(path):
            for file in files:
                if not db.is_file_done(file):
                    print("Storing file " + file)
                    store_file(file, "boursorama")
    else:
        path = "data/boursorama/"

        for years in os.walk(path):
            print(years)
            for file in years:
                if not db.is_file_done(file):
                    print("Storing file " + file)
                    store_file(file, "boursorama")

    print("Done")
