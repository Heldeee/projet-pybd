import pandas as pd
import numpy as np
import sklearn
from datetime import datetime
import os
import glob

import timescaledb_model as tsdb

db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
#db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker

def store_files(market, year):
    files = glob.glob("data/boursorama/" + year + "/" + market+ "*")
    files_df = pd.DataFrame({'name': [x.split('/')[-1] for x in files]})
    df = pd.concat([pd.read_pickle(file) for file in files], keys=[pd.to_datetime(file.split(' ')[-2] + ' ' + file.split(' ')[-1].split('.bz2')[0], format='%Y-%m-%d %H:%M:%S.%f') for file in files], names=['date'])

    if market == "peapme":
        market_id = db.raw_query("SELECT id FROM markets WHERE alias = %s", ("euronx",))[0][0]
    else:
        market_id = db.raw_query("SELECT id FROM markets WHERE alias = %s", (market,))[0][0]


    df['last'] = df['last'].str.replace('(c)', '')
    df['last'] = df['last'].str.replace('(s)', '')
    df['last'] = df['last'].str.replace(' ', '').astype(float)

    df.drop(columns=['symbol'], inplace=True)
    df.reset_index(level=1, inplace=True)

    new_companies = []

    # Groupement par le nom de l'entreprise et ajout des symboles à la liste new_companies
    unique_symbols = df['symbol'].unique()

    for symbol in unique_symbols:
        # Check if the symbol already exists in the database
        company_id = db.return_company_id_symbol(symbol)
        if company_id != []:
            print("Symbol already exists in the database.", symbol)
        else:
            # Find the corresponding company names for the symbol
            company_name = df[df['symbol'] == symbol]['name'].iloc[0]
            new_companies.append({
                'name': company_name,
                'mid': int(market_id),
                'symbol': symbol
            })
            #print("New company + symbol added to the database.", company_name, symbol)

    if new_companies:
        new_companies_df = pd.DataFrame(new_companies)
        #print(new_companies_df)
        db.df_write(new_companies_df, 'companies', index=False, if_exists='append')
        #print("New companies added to the database.")

    #get companies table
    df.reset_index(inplace=True)
    res = db.df_query("SELECT * FROM companies")
    companies_df = pd.concat(res)

    merged_df = pd.merge(df, companies_df, how='inner', left_on='symbol', right_on='symbol')

    # Créer le DataFrame 'stocks_df' avec les colonnes 'value', 'volume', 'cid', et 'date'
    """stocks_df = pd.DataFrame({
        'value': merged_df['last'],
        'volume': merged_df['volume'],
        'cid': merged_df['id'],  # Utilisation de l'ID de la société provenant de 'companies_df'
        'date': merged_df['date']
    })"""
    # Ajout des stocks
    #get cid in companies_df from name and symbol
    #db.df_write(stocks_df, 'stocks', index=False, if_exists='append')

    #db.df_write(files_df, 'file_done', index=False, if_exists='append')

    
if __name__ == '__main__':
    store_files("compA", "2020")

    """db.execute(
        INSERT INTO daystocks (date, cid, open, close, high, low, volume)
        SELECT 
            DATE(date) AS date, 
            cid, 
            MIN(value) AS open, 
            MAX(value) AS close, 
            MAX(value) AS high, 
            MIN(value) AS low, 
            SUM(volume) AS volume
        FROM 
            stocks
        GROUP BY 
            DATE(date), cid;
    , commit=True)"""

    """daystocks = db.df_query("SELECT * FROM daystocks")
    for row in daystocks:
        print(row)"""

    print("Done")
