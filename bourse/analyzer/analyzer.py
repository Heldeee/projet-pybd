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
    db_thread = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp', is_thread=True) 
    # Now you can access the name associated with a symbol directly from the hashmap
    merged_df = pd.merge(df, companies_df, how='inner', on='symbol')
    merged_df.rename(columns={'last': 'value'}, inplace=True)

    # Create stocks_df with columns 'value', 'volume', 'cid', and 'date'
    stocks_df = merged_df[['value', 'volume', 'cid']].copy()
    stocks_df['date'] = df.index  # Accessing the index to get the date

    # Checkpoint after merging and creating stocks_df
    merge_stocks_time = time.time()

    # Get cid in companies_df from name and symbol
    db_thread.df_write(stocks_df, 'stocks',commit=True, index=False)

    # Checkpoint after writing to database
    write_db_time = time.time()
    logger.info(f"||||| process_data() - Writing to database completed in {round(write_db_time - merge_stocks_time,2)} seconds")

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
    logger.info(f"||||| store_files({market},{year}) - Data loaded in {round(time.time() - start_time,2)} seconds")

    df.drop(columns=['symbol'], inplace=True)
    df.reset_index(level=1, inplace=True)

    # Checkpoint after data preprocessing
    preprocess_time = time.time()
    logger.info(f"||||| store_files({market},{year}) - Data preprocessed in {round(preprocess_time - start_time,2)} seconds")

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

    # Divide DataFrame into 12 chunks
    chunk_size = len(df) // 12
    chunks = [df.iloc[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
    
    for chunk in chunks:
        # Divide each chunk into sub-chunks based on the number of CPU cores
        num_cores = multiprocessing.cpu_count()
        sub_chunk_size = max(len(chunk) // num_cores, 1)
        sub_chunks = [chunk.iloc[i:i + sub_chunk_size] for i in range(0, len(chunk), sub_chunk_size)]

        processes = []
        for sub_chunk in sub_chunks:
            # Create a process for each sub-chunk
            process = multiprocessing.Process(target=process_data, args=(sub_chunk, companies_df))
            processes.append(process)
            process.start()
        
        # Wait for all processes to finish
        for process in processes:
            process.join()

    # Checkpoint after loading files
    load_files_time = time.time()
    logger.info(f"||||| store_files({market},{year}) - stored in {round(load_files_time - start_time,2)} seconds")

def witchcraft(start_date, end_date):
    begin = time.time()
    logger.info(f"Beggining whitchcraft for period {start_date}/{end_date}...")
    db.execute("""
    INSERT INTO daystocks (date, cid, open, close, high, low, volume)
    SELECT DISTINCT ON (date_trunc('day', s.date), s.cid)
        date_trunc('day', s.date) AS date,
        s.cid,
        first_value(s.value) OVER (PARTITION BY date_trunc('day', s.date), s.cid ORDER BY s.date) AS open,
        last_value(s.value) OVER (PARTITION BY date_trunc('day', s.date), s.cid ORDER BY s.date RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS close,
        max(s.value) OVER (PARTITION BY date_trunc('day', s.date), s.cid) AS high,
        min(s.value) OVER (PARTITION BY date_trunc('day', s.date), s.cid) AS low,
        sum(s.volume) OVER (PARTITION BY date_trunc('day', s.date), s.cid) AS volume
    FROM stocks s
    WHERE s.date BETWEEN '%s' AND '%s'
    ORDER BY date_trunc('day', s.date), s.cid, s.date;
""" % (start_date, end_date), commit=True)
    end = time.time()
    logger.info(f"Whitchcraft done on period {start_date}/{end_date} in {round(end-begin,2)} seconds.")

def load_everything():
    store_files("peapme", "2023")
    store_files("compB", "2023")
    store_files("compA", "2023")
    #store_files("amsterdam", "2023")
    witchcraft('2023-01-01', '2023-12-31')
    store_files("peapme", "2022")
    store_files("compB", "2022")
    store_files("compA", "2022")
    #store_files("amsterdam", "2022")
    witchcraft('2022-01-01', '2022-12-31')
    store_files("peapme", "2021")
    store_files("compB", "2021")
    store_files("compA", "2021")
    #store_files("amsterdam", "2021")
    witchcraft('2021-01-01', '2021-12-31')
    store_files("compB", "2020")
    store_files("compA", "2020")
    #store_files("amsterdam", "2020")
    witchcraft('2020-01-01', '2020-12-31')
    store_files("compB", "2019")
    store_files("compA", "2019")
    #store_files("amsterdam", "2019")
    witchcraft('2019-01-01', '2019-12-31')

    companies_df = pd.DataFrame(new_companies).drop(columns=['cid'])

    db.df_write(companies_df, 'companies',commit=True, index=False)

if __name__ == '__main__':
    start_time = time.time()

#    load_everything()

    end_time = time.time()  # Record the end time
    execution_time = end_time - start_time  # Calculate the execution time
    logger.info(f"Total execution time: {round(execution_time,2)} seconds")
    logger.info("Done")
