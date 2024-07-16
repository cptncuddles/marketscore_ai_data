#these are the libraries we need installed and imported for the pipeline with the time module being optional, it is purely for metric tracking
import pandas as pd
import json
import glob
import time

#this is when we want to view the dataframes in the terminal
#pd.set_option('display.max_columns', None)

json_files = glob.glob("C:\\Users\\17197\\Documents\\Coding Projects\\Market Score Data\\MarketScore Ticker\\*json")
test_file = ["C:\\Users\\17197\\Documents\\Coding Projects\\Market Score Data\\MarketScore Ticker\\MARKETSCORE-2024-05-01-1.json"]

#This function is used to extract the file name for reuse upon export
def pull_name(string):
    name_list = string.split('\\')
    for x in name_list:
        if ".json" in x:
            name = x.split('.')
            return name[0]
        else:
            pass
'''
#largest helper function for cleaning the embedded data values for the meta_data key not in use for first production run, but keeping for possible future use
def meta_data_cleaner(dict_list):
    holding_bin = []
    for dict in dict_list:
        tuple_list = []
#converts all of the data in the 2 columns to floats from strings for better analysis
        for x, y in zip(dict['last_12_upside'], dict['last_12_downside']):
            x = float(x)
            y = float(y)
            combo_score = (x, y)
            tuple_list.append(combo_score)
        dict['last_12_up/down_scores'] = tuple_list
#this is to get rid of the upside/downside data which is going to a sepearate dataframe as well as the duplicated date and time columns
        columns_to_drop = ['last_12_upside', 'last_12_downside', 'date', 'time']
        for column in columns_to_drop:
            dict.pop(column)
        holding_bin.append(pd.json_normalize(dict))
#takes all of the cleaned smaller data frames and joins them into 1 large dataframe for export
    combined_meta_df = pd.concat(holding_bin, ignore_index=True)
    combined_meta_df['count'] = [x for x in range(len(combined_meta_df))]
    return combined_meta_df
'''
#These two helper functions work exactly the same, I'm sure there is a way to optimize the process so I only have to run it once, but that will come in v2
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

#Main function to process the imported files
def json_to_csv_converter(files):
    tic = time.perf_counter()
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
            names = [pull_name(file)]
            for name in names:
                combined_marketscore_df.to_csv(f"C:\\Users\\17197\\Documents\\Coding Projects\\Market Score Data\\MarketScore Ticker\\{name}.csv", index=False)
#current file is csv format with zero index, but can be switched to parquet or even back to json. file type can be determined once a ML structure is decided upon
#a performance timer left in for future optimization testing
    toc = time.perf_counter()
    print(f"Completed function in {toc - tic:0.4f} second")

json_to_csv_converter(json_files)
