import pandas as pd
import numpy as np
import sklearn
from datetime import datetime
import os
import glob
import time
import mylogging
import multiprocessing

import timescaledb_model as tsdb

symbol_map = {}
new_companies = []
id_count = 1

logger = mylogging.getLogger(__name__)

db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
#db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker

def read_pickle_file(file):
    return pd.read_pickle(file)

def create_dataframe(files):
    # Read pickle files in parallel
    with multiprocessing.Pool() as pool:
        dfs = pool.map(read_pickle_file, files)
    
    # Concatenate the DataFrames
    df = pd.concat(dfs, keys=[pd.to_datetime(file.split(' ')[-2] + ' ' + file.split(' ')[-1].split('.bz2')[0], format='%Y-%m-%d %H:%M:%S.%f') for file in files], names=['date'])
    
    return df

def process_data(df, companies_df):
    # Now you can access the name associated with a symbol directly from the hashmap
    merged_df = pd.merge(df, companies_df, how='inner', on='symbol')
    merged_df.rename(columns={'last': 'value'}, inplace=True)

    # Create stocks_df with columns 'value', 'volume', 'cid', and 'date'
    stocks_df = merged_df[['value', 'volume', 'cid']].copy()
    stocks_df['date'] = df.index  # Accessing the index to get the date

    # Checkpoint after merging and creating stocks_df
    merge_stocks_time = time.time()

    # Get cid in companies_df from name and symbol
    db.df_write(stocks_df, 'stocks',commit=True, index=False)

    # Checkpoint after writing to database
    write_db_time = time.time()
    logger.info(f"Writing to database completed in {write_db_time - merge_stocks_time} seconds")

    # Assuming new_companies is a list of dictionaries

def store_files(market, year):
    start_time = time.time()  # Start time checkpoint

    # Get files
    files = glob.glob("data/boursorama/" + year + "/" + market+ "*")
    # Create DataFrame in parallel
    df = create_dataframe(files)
    pea = False
    if market == "peapme":
        pea = True
        market_id = int(db.raw_query("SELECT id FROM markets WHERE alias = %s", ("euronx",))[0][0])
    else:
        market_id = int(db.raw_query("SELECT id FROM markets WHERE alias = %s", (market,))[0][0])

    global id_count, symbol_map, new_companies
    df.loc[:, 'last'] = df['last'].str.replace('(c)', '').str.replace('(s)', '')
    df.loc[:, 'last'] = df['last'].str.replace(' ', '').astype(float)
    logger.info(f"Data loaded in {time.time() - start_time} seconds")

    df.drop(columns=['symbol'], inplace=True)
    df.reset_index(level=1, inplace=True)

    # Checkpoint after data preprocessing
    preprocess_time = time.time()
    logger.info(f"Data preprocessed in {preprocess_time - start_time} seconds")

    unique_symbols = df['symbol'].unique()
    unique_names = df.groupby('symbol')['name'].first()

    # Convert unique_symbols to a pandas Series
    unique_symbols_series = pd.Series(unique_symbols)

    # Identify new symbols
    new_symbols = unique_symbols_series[~unique_symbols_series.isin(symbol_map)]

    # Add new symbols to symbol_map and new_companies
    for symbol in new_symbols:
        name = unique_names[symbol]
        symbol_map[symbol] = (name, id_count)
        new_companies.append({
            'name': name,
            'mid': market_id,
            'symbol': symbol,
            'pea': pea,
            'cid': id_count
            # Add more columns here if needed
        })
        id_count += 1
    companies_df = pd.DataFrame(new_companies)

    chunk_size = len(df) // 12
    chunks = [df.iloc[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

    processes = []
    for chunk in chunks:
        process = multiprocessing.Process(target=process_data, args=(chunk, companies_df))
        processes.append(process)
        process.start()
    for process in processes:
        process.join()

    # Checkpoint after loading files
    load_files_time = time.time()
    logger.info(f"Files loaded in {load_files_time - start_time} seconds")


if __name__ == '__main__':
    start_time = time.time()
    #store_files("compA", "2020")
    #store_files("compB", "2020")
    #store_files("amsterdam", "2020")
    #companies_df = pd.DataFrame(new_companies).drop(columns=['cid'])
    #logger.info(companies_df)

    #db.df_write(companies_df, 'companies',commit=True, index=False)

    if False:
        db.execute("""
        INSERT INTO daystocks (date, cid, open, close, high, low, volume)
        SELECT
            date_trunc('day', date) AS date,
            cid,
            first_value(value) OVER (PARTITION BY cid, date_trunc('day', date) ORDER BY date ASC) AS open,
            last_value(value) OVER (PARTITION BY cid, date_trunc('day', date) ORDER BY date ASC) AS close,
            max(value) OVER (PARTITION BY cid, date_trunc('day', date)) AS high,
            min(value) OVER (PARTITION BY cid, date_trunc('day', date)) AS low,
            sum(volume) OVER (PARTITION BY cid, date_trunc('day', date)) AS volume
        FROM
            stocks;"""
        , commit=True)

    end_time = time.time()  # Record the end time
    execution_time = end_time - start_time  # Calculate the execution time
    print(f"Total execution time: {execution_time} seconds")
    print("Done")
