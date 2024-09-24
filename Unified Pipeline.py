import pandas as pd
import json
import glob
import time
import sqlite3
import re

#helper function to pull name from imported file to be used in naming new file
def pull_name(string):
    name_list = string.split('\\')
    for x in name_list:
        if ".json" in x:
            name = x.split('.')
            return name[0]
        else:
            pass
#helper function to pull the date from imported files for the sql database name
def pull_date(string):
    match = re.search(r'(\d{4}-\d{2}-\d{2})', string)
    if match:
        date = match.group(1)
        return date
#converter function for mscounts
def mscounts_converter(files):
    for file in files:        
        with open(file) as json_file:
            data = json.load(json_file)
            data_dump = data['master_counts']
            data_dump = data_dump[1:]
            df1 = pd.DataFrame(data_dump, columns=['time_stamp', 'call count', 'put count'])
            df1['time_stamp'] = pd.to_datetime(df1['time_stamp'], format="%H:%M").dt.time
            df1 = df1.iloc[42:]
            mscounts_dfs.append(df1)
#converter function for individual tickers
def ticker_converter(files):
    for file in files:
        with open(file) as json_file:
            data = json.load(json_file)
            data_dump = data['trend']
#This double naming is due to meta data stored in the 0 index of the values for the .json's dict object
            data_dump = data_dump[1:]
            df1 = pd.DataFrame(data_dump, columns=['time_stamp', 'call_count', 'put_count'])
            data_dump2 = data['candles']
            data_dump2 = data_dump2[1:]
            df2 = pd.DataFrame(data_dump2, columns = ['time_stamp', 'low', 'open','close', 'high', 'style', 'c_bid', 'c_ask', 'c_high', 'c_low', 'p_bid', 'p_ask', 'p_high', 'p_low'])
            df2 = df2.drop(columns=['style', 'c_bid', 'c_ask', 'c_high', 'c_low', 'p_bid', 'p_ask', 'p_high', 'p_low'])
            df3 = pd.merge(df1, df2)
            df3['time_stamp'] = pd.to_datetime(df3['time_stamp'], format="%H:%M").dt.time
            df3 = df3.iloc[42:]
#This logic will separate the ES and NQ datasets into separate containers
            if "\\ES-" in file:
                es_ticker_dfs.append(df3)
            else:
                nq_ticker_dfs.append(df3)
#converter function for marketscore 
def marketscore_converter(files):
    for file in files:
        with open(file) as json_file:
            marketscore_candles = json.load(json_file)
#This seperates the JSON dict object out so that we are left with just the data portions without the 'headers'
            marketscore_dataset = marketscore_candles['marketscore_candles'][1:]
            #marker_bin is a list of lists
            marker_bin = []
            #meta_data_bin is a list of dicts
            meta_data_bin = []
            #upside_data_bin is a list of dicts
            upside_data_bin = []
            #downside_data_bin is a list of dicts
            downside_data_bin = []
#iterating through the larger dataset to sort our the markers from the metadata dictionary
            for dataset in marketscore_dataset:
                markers = dataset[:3]
                marker_bin.append(markers)
                meta_data = dataset[3]
                meta_data_bin.append(meta_data)
#iterating through the meta_data_bin list to seperate out all the of the upside/downside data
            for meta_data in meta_data_bin:
                upside_data_bin.append(meta_data['upside_data'])
                meta_data.pop('upside_data')
                downside_data_bin.append(meta_data['downside_data'])
                meta_data.pop('downside_data')
#3 seperate helper functions that each clean the data in the holding lists, divides and simplifies the tasks
#2 helper functions in use the third will be saved for future use.
            #meta_data_df = meta_data_cleaner(meta_data_bin)           
            upside_data_df = upside_data_cleaner(upside_data_bin)
            downside_data_df = downside_data_cleaner(downside_data_bin)
#Creates the main data frame and merges the returned data frames from the helper functions into the main one before exporting to disk
            market_score_df = pd.DataFrame(marker_bin, columns= ['time', 'upside_score', 'downside_score'])
            market_score_df['count'] = [x for x in range(len(market_score_df))]
            combined_marketscore_df = market_score_df.merge(upside_data_df, how='outer', on='count').merge(downside_data_df, how='outer', on='count')
            combined_marketscore_df = combined_marketscore_df.drop(columns='count')
#converts our time dtype values to datetime from string
            combined_marketscore_df['time'] = pd.to_datetime(combined_marketscore_df['time'], format="%H:%M").dt.time
            marketscore_dfs.append(combined_marketscore_df)
#helper function for marketscore converter
def downside_data_cleaner(dict_list):
    holding_bin = []
    columns_to_drop = ['score','last_high_time', 'last_low_time']
    for dict in dict_list:
        for column in columns_to_drop:
            dict.pop(column)
        downside_df = pd.DataFrame.from_dict([dict], orient='columns')
        downside_df.rename(columns=lambda x: 'dwnside_'+ x, inplace=True)
        holding_bin.append(downside_df)     
    combined_downside_df = pd.concat(holding_bin, ignore_index=True)
    combined_downside_df['count'] = [x for x in range(len(combined_downside_df))]
    return combined_downside_df
#helper function for marketscore converter
def upside_data_cleaner(dict_list):
    holding_bin = []
    columns_to_drop = ['score','last_high_time', 'last_low_time']
    for dict in dict_list:
#dropping columns not needed in final data set
        for column in columns_to_drop:
            dict.pop(column)
        upside_df = pd.DataFrame.from_dict([dict], orient='columns')
        upside_df.rename(columns=lambda x: 'upside_'+ x, inplace=True)
        holding_bin.append(upside_df)
#takes all of the cleaned smaller data frames and joins them into 1 large dataframe for export
    combined_upside_df = pd.concat(holding_bin, ignore_index=True)
#this gives a common column in each dataframe to merge on
    combined_upside_df['count'] = [x for x in range(len(combined_upside_df))]
    return combined_upside_df
#converter function for daily trades
def trades_converter(files):
    for file in files:
        with open(file) as info_dump:
            data = json.load(info_dump)
            data_s1 = data["config"]
            data_s2 = data_s1["trades"] # <-- this is a list of dictionaries 
            keys_to_keep = ['trade_count', 'position_type', 'exit_type', 'entry_time', 'total_heat', 'heat_before_peak', 'heat_allowed', 'exit_time', 'total_points']
#data_to_keep set to empty list to hold the dictionary entries that are parsed through using a dictionary comprehension.
#this will allow for easier parsing with the cleaning function to separate out all of the relevant data sets
            data_to_keep = []
            df_holding_list = []
#this dict comprehension is to get the correct data sets from the larger json pulling out the keys in the 'keys_to_keep' list
            for item in data_s2:
                data_to_keep.append({key: item[key] for key in keys_to_keep})
#this dict comprehension is to pull the actual data out of the embedded dictionaries so that we end with a singular {key:value} pairing
            for item in data_to_keep:
                new_dict = {key: item[key]['value'] for key in item.keys()}
                new_df = pd.DataFrame.from_dict([new_dict], orient='columns')
                df_holding_list.append(new_df)
#this concats all of the smaller DF's into a large one for export
            full_df = pd.concat(df_holding_list, ignore_index= True)
#setting our time values as dtype datetime as this will be part of the compound primary key
            full_df['entry_time'] = pd.to_datetime(full_df['entry_time'], format="%H:%M").dt.time
            full_df['exit_time'] = pd.to_datetime(full_df['exit_time'], format="%H:%M").dt.time
#This logic will separate the ES and NQ datasets into separate containers using regex
            if "\\ES-" in file:
                es_trades_dfs.append(full_df)
            else:
                nq_trades_dfs.append(full_df)


#this pulls all data from local directories to import for cleaning
mscounts_data = glob.glob("C:FILE LOCATION\\*json")
ticker_data = glob.glob("C:FILE LOCATION\\*.json")
marketscore_data = glob.glob("C:FILE LOCATION\\*json")
trades_data = glob.glob("C:FILE LOCATION\\*.json")

#containers for the functions to dump completed DFs into to be added to a SQL database
database_names = []

tic = time.perf_counter()
for file in mscounts_data:
    database_names.append(pull_date(file))

mscounts_dfs = []
es_ticker_dfs = []
nq_ticker_dfs = []
marketscore_dfs = []
es_trades_dfs = []
nq_trades_dfs = []

mscounts_converter(mscounts_data)
ticker_converter(ticker_data) 
marketscore_converter(marketscore_data)
trades_converter(trades_data)

#logic used to create the sql databases to be exported.
for name, df1, df2, df3, df4, df5, df6 in zip(database_names, mscounts_dfs, es_ticker_dfs, nq_ticker_dfs, marketscore_dfs, es_trades_dfs, nq_trades_dfs):
#stricly for debugging purposes
    assert len(database_names) == len(mscounts_dfs) == len(es_ticker_dfs) == len(nq_ticker_dfs) == len(marketscore_dfs) == len(es_trades_dfs) == len(nq_trades_dfs), "All containers are not of equal length"
#using local sqlite connection, any sql connection can be used.
    conn = sqlite3.connect(f'C:\\Users\\17197\\OneDrive\\Documents\\Coding Projects\\Market Score Data\\Test Data\\{name}.db')
    df1.to_sql('ms_counts', conn, if_exists='replace', index=False)
    df2.to_sql('es_ticker', conn, if_exists='replace', index=False)
    df3.to_sql('nq_ticker', conn, if_exists='replace', index=False)
    df4.to_sql('marketscore', conn, if_exists='replace', index=False)
    df5.to_sql('es_trades', conn, if_exists='replace', index=False)
    df6.to_sql('nq_trades', conn, if_exists='replace', index=False)
conn.close()

toc = time.perf_counter()
print(f"Completed operation in {toc - tic:0.4f} seconds.")
