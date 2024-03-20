import pandas as pd
import numpy as np
import sklearn

import timescaledb_model as tsdb

db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
#db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker

def store_file(name, website):
    if db.is_file_done(name):
        return
    if website.lower() == "boursorama":
        try:
            market_alias = name.split()[0]
            market_id = db.raw_query("SELECT id FROM markets WHERE alias = %s", (market_alias,))[0][0]
        except:
            print("Fichier: " + name + " : marché non reconnu")

        try:
            df = pd.read_pickle("data/boursorama/" + name)  # is this dir ok for you ?
        except:
            year = name.split()[1].split("-")[0]
            df = pd.read_pickle("data/boursorama/" + year + "/" + name)
        # to be finished
        df['last'] = df['last'].str.replace('(c)', '')
        df['last'] = df['last'].str.replace('(s)', '')
        df['last'] = df['last'].str.replace(' ', '').astype(float)

        #print 30 first rows
        print(name)
        print(df.head(30))

        columns = ['name', 'mid', 'symbol', 'symbol_nf', 'isin', 'reuters', 'boursorama', 'pea', 'sector']



if __name__ == '__main__':
    store_file("compA 2023-12-28 14:42:02.133818.bz2", "boursorama")
    store_file("peapme 2023-12-29 17:22:01.359979.bz2", "boursorama")
    print("Done")
