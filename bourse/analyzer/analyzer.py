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
    
def process_dataframe(df):
    # Use vectorized string operations to replace characters and convert to float
    df['last'] = df['last'].str.replace(r'\(c\)|\(s\)| ', '', regex=True).astype(float)
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
    #logger.info(f"Writing to database completed in {write_db_time - merge_stocks_time} seconds")

    # Assuming new_companies is a list of dictionaries


def launch_store_file(market, year):
    start_time = time.time()
    files = glob.glob("data/boursorama/" + year + "/" + market+ "*")
    num_groups = 12
    file_groups = [files[i:i + len(files) // num_groups] for i in range(0, len(files), len(files) // num_groups)]
    logger.info("G0\n")
    store_files(market, year, file_groups[0])
    logger.info("G1\n")
    store_files(market, year, file_groups[1])
    logger.info("G2\n")
    store_files(market, year, file_groups[2])
    logger.info("G3\n")
    store_files(market, year, file_groups[3])
    logger.info("G4\n")
    store_files(market, year, file_groups[4])
    logger.info("G5\n")
    store_files(market, year, file_groups[5])
    logger.info("G6\n")
    store_files(market, year, file_groups[6])
    logger.info("G7\n")
    store_files(market, year, file_groups[7])
    logger.info("G8\n")
    store_files(market, year, file_groups[8])
    logger.info("G9\n")
    store_files(market, year, file_groups[9])
    logger.info("G10\n")
    store_files(market, year, file_groups[10])
    logger.info("G11\n")
    store_files(market, year, file_groups[11])
    if len(file_groups) > 12:
        store_files(market, year, file_groups[12])
    logger.info(f"\n\n\n{market} {year} done in {time.time() - start_time} seconds\n\n")


def store_files(market, year, files):
    start_time = time.time()  # Start time checkpoint

    # Create DataFrame in parallel
    df = create_dataframe(files)
    logger.info(f"Files read in {time.time() - start_time} seconds")
    pea = False
    if market == "peapme":
        pea = True
        market_id = int(db.raw_query("SELECT id FROM markets WHERE alias = %s", ("euronx",))[0][0])
    else:
        market_id = int(db.raw_query("SELECT id FROM markets WHERE alias = %s", (market,))[0][0])
    logger.info(f"Check 0: {time.time() - start_time}")

    global id_count, symbol_map, new_companies
    with multiprocessing.Pool() as pool:
        processed_dfs = pool.map(process_dataframe, [df])
    logger.info(f"Check 1: {time.time() - start_time}")
    df = processed_dfs[0]
    #logger.info(f"Data loaded in {time.time() - start_time} seconds")

    df.drop(columns=['symbol'], inplace=True)
    df.reset_index(level=1, inplace=True)

    # Checkpoint after data preprocessing
    preprocess_time = time.time()
    #logger.info(f"Data preprocessed in {preprocess_time - start_time} seconds")

    unique_symbols = df['symbol'].unique()
    unique_names = df.groupby('symbol')['name'].first()

    # Convert unique_symbols to a pandas Series
    unique_symbols_series = pd.Series(unique_symbols)
    #logger.info(f"Check 2: {time.time() - start_time}")

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
    #logger.info(f"Check 3: {time.time() - start_time}")

    # Divide DataFrame into sub-chunks based on the number of CPU cores
    num_cores = multiprocessing.cpu_count()
    sub_chunk_size = len(df) // num_cores
    sub_chunks = [df.iloc[i:i + sub_chunk_size] for i in range(0, len(df), sub_chunk_size)]

    # Create processes for each sub-chunk and start them
    processes = []
    for sub_chunk in sub_chunks:
        process = multiprocessing.Process(target=process_data, args=(sub_chunk, companies_df))
        processes.append(process)
        process.start()

    # Wait for all processes to finish
    for process in processes:
        process.join()

        # Checkpoint after loading files
        load_files_time = time.time()

def load_everything():
    """launch_store_file("peapme", "2023")
    launch_store_file("compB", "2023")
    launch_store_file("compA", "2023")"""
    launch_store_file("amsterdam", "2023")
    """launch_store_file("peapme", "2022")
    launch_store_file("compB", "2022")
    launch_store_file("compA", "2022")
    launch_store_file("amsterdam", "2022")
    launch_store_file("peapme", "2021")
    launch_store_file("compB", "2021")
    launch_store_file("compA", "2021")
    launch_store_file("amsterdam", "2021")
    launch_store_file("compB", "2020")
    launch_store_file("compA", "2020")
    launch_store_file("amsterdam", "2020")
    launch_store_file("compB", "2019")
    launch_store_file("compA", "2019")
    launch_store_file("amsterdam", "2019")"""


if __name__ == '__main__':
    start_time = time.time()
#    load_everything()
    companies_df = pd.DataFrame(new_companies).drop(columns=['cid'])
    #logger.info(companies_df)

    db.df_write(companies_df, 'companies',commit=True, index=False)

    if False:
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
ORDER BY date_trunc('day', s.date), s.cid, s.date;
"""
        , commit=True)

    end_time = time.time()  # Record the end time
    execution_time = end_time - start_time  # Calculate the execution time
    logger.info(f"Total execution time: {execution_time} seconds")
    print("Done")
