import pandas as pd
import numpy as np
import sklearn
from datetime import datetime
import os
import glob

import timescaledb_model as tsdb

db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
#db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker

def store_file(filename, website):
    if db.is_file_done(filename):
        return
    if website.lower() == "boursorama":
        market_id = "0"
        market_alias = filename.split()[0]
        if market_alias == "peapme":
            market_id = db.raw_query("SELECT id FROM markets WHERE alias = %s", ("euronx",))[0][0]
            pea = True
        else:
            market_id = db.raw_query("SELECT id FROM markets WHERE alias = %s", (market_alias,))[0][0]
            pea = False

        try:
            df = pd.read_pickle("data/boursorama/" + filename)  # is this dir ok for you ?
        except:
            year = filename.split()[1].split("-")[0]
            df = pd.read_pickle("data/boursorama/" + year + "/" + filename)


        df['last'] = df['last'].str.replace('(c)', '')
        df['last'] = df['last'].str.replace('(s)', '')
        df['last'] = df['last'].str.replace(' ', '').astype(float)

        #DATE
        date_string = filename.split(' ')[1] + " " +  filename.split(' ')[2]
        date_string = date_string.replace('.bz2', '')

        companies = df['name'].unique()
        print(df.head())
        #COMPANY
        columns = ['name', 'mid', 'symbol', 'symbol_nf', 'isin', 'reuters', 'boursorama', 'pea', 'sector']
        new_companies = []

        for company_name in companies:
            company_id = db.search_company_id(company_name)
            if company_id == 0:
                new_companies.append({'name': company_name, 'mid': int(market_id), 'pea': pea})

        if new_companies != []:
            new_companies = pd.DataFrame(new_companies, columns=columns)
            #db.df_write(new_companies, 'companies', index=False, if_exists='append')
            print("New companies added to the database.")
        else:
            print("No new companies to add to the database.")


        #STOCKS
        columns = ['date', 'cid', 'value', 'volume']
        stocks_df = pd.DataFrame({'date': [date_string] * len(df),
                                    'cid': df['name'].apply(lambda x: db.search_company_id(x)),
                                    'value': df['last'],
                                    'volume': df['volume']},
                                    columns=columns)
        #db.df_write(stocks_df, 'stocks', index=False, if_exists='append')


        #DAYSTOCKS
        """columns = ['date', 'cid', 'close', 'volume', 'open', 'high', 'low']
        daystocks_df = pd.DataFrame({'date': [date_string] * len(df),
                                     'cid': df['name'].apply(lambda x: db.search_company_id(x)),
                                     'volume': df['volume']},
                                     columns=columns)

        daystocks_df['open'] = stocks_df.groupby('cid')['value'].transform('first')
        daystocks_df['close'] = stocks_df.groupby('cid')['value'].transform('last')
        daystocks_df['high'] = stocks_df.groupby('cid')['value'].transform('max')
        daystocks_df['low'] = stocks_df.groupby('cid')['value'].transform('min')


        db.df_write(daystocks_df, 'daystocks', index=False, if_exists='append')"""
                                     
        db.df_write(pd.DataFrame({'name': [filename]}), 'file_done', index=False, if_exists='append')
        print("File stored in the database.")

        

def store_files(market, year):
    files = glob.glob("data/boursorama/" + year + "/" + market+ "*")
    
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
    for company_name, symbols in df.groupby('name')['symbol']:
        for symbol in symbols.unique():
            company_id = db.return_company_id(company_name, symbol)
            if company_id == []:
                new_companies.append({
                    'name': company_name,
                    'mid': int(market_id),
                    'symbol': symbol
                })
                print("New company + symbol added to the database.", company_name, symbol)
            else:
                print("Company and symbol already in the database.", company_id, company_name, symbol)
    if new_companies:
        new_companies_df = pd.DataFrame(new_companies)
        db.df_write(new_companies_df, 'companies', index=False, if_exists='append')
        print("New companies added to the database.")
    else:
        print("No new companies to add to the database.")

    #get companies table
    df.reset_index(inplace=True)
    res = db.df_query("SELECT * FROM companies")
    companies_df = pd.concat(res)

    merged_df = pd.merge(df, companies_df, how='inner', left_on='symbol', right_on='symbol')

    # Créer le DataFrame 'stocks_df' avec les colonnes 'value', 'volume', 'cid', et 'date'
    stocks_df = pd.DataFrame({
        'value': merged_df['last'],
        'volume': merged_df['volume'],
        'cid': merged_df['id'],  # Utilisation de l'ID de la société provenant de 'companies_df'
        'date': merged_df['date']
    })
    # Ajout des stocks
    #get cid in companies_df from name and symbol
    db.df_write(stocks_df, 'stocks', index=False, if_exists='append')

    
if __name__ == '__main__':
    TEST = True
    #store_files("compA", "2020")
    if TEST:
        """path = "data/boursorama/2020"

        for root, dirs, files in os.walk(path):
            for file in files:
                if not db.is_file_done(file):
                    print("Storing file " + file)
                    store_file(file, "boursorama")"""
    else:
        path = "data/boursorama/"

        for dirs in os.walk(path):
            for files in dirs:
                for file in files:
                    print("Storing file " + file)
                    store_file(file, "boursorama")

    print("Done")
