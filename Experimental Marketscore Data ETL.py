import pandas as pd
import json
import glob
from IPython.display import display
pd.set_option('display.max_columns', None)

json_files = glob.glob("C:\\Users\\17197\\Documents\\Coding Projects\\Market Score Data\\MarketScore Ticker\\*json")
test_file = ["C:\\Users\\17197\\Documents\\Coding Projects\\Market Score Data\\MarketScore Ticker\\MARKETSCORE-2024-05-01-1.json"]

def pull_name(string):
    name_list = string.split('\\')
    for x in name_list:
        if ".json" in x:
            name = x.split('.')
            return name[0]
        else:
            pass

def meta_data_cleaner(dict_list):
    holding_bin = []
    for dict in dict_list:
        tuple_list = []
        for x, y in zip(dict['last_12_upside'], dict['last_12_downside']):
            x = float(x)
            y = float(y)
            combo_score = (x, y)
            tuple_list.append(combo_score)
        dict['last_12_up/down_scores'] = tuple_list
        columns_to_drop = ['last_12_upside', 'last_12_downside', 'date', 'time']
        for column in columns_to_drop:
            dict.pop(column)
        holding_bin.append(pd.json_normalize(dict))
    combined_meta_df = pd.concat(holding_bin, ignore_index=True)
    combined_meta_df['count'] = [x for x in range(len(combined_meta_df))]
    return combined_meta_df

def upside_data_cleaner(dict_list):
    holding_bin = []
    for dict in dict_list:
        upside_df = pd.DataFrame.from_dict([dict], orient='columns')
        upside_df.rename(columns=lambda x: 'up_'+ x, inplace=True)
        upside_df['count'] = [x for x in range(len(upside_df.index))]
        holding_bin.append(upside_df)
    combined_upside_df = pd.concat(holding_bin, ignore_index=True)
    combined_upside_df['count'] = [x for x in range(len(combined_upside_df))]
    return combined_upside_df

def downside_data_cleaner(dict_list):
    holding_bin = []
    for dict in dict_list:
        downside_df = pd.DataFrame.from_dict([dict], orient='columns')
        downside_df.rename(columns=lambda x: 'dwn_'+ x, inplace=True)
        downside_df['count'] = [x for x in range(len(downside_df.index))]
        holding_bin.append(downside_df)
    combined_downside_df = pd.concat(holding_bin, ignore_index=True)
    combined_downside_df['count'] = [x for x in range(len(combined_downside_df))]
    return combined_downside_df

def json_to_csv_converter(files):
    for file in files:
        with open(file) as json_file:
            marketscore_candles = json.load(json_file)
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
#3 seperate helper functions that each clean the data in the holding lists for a quicker running time, will add concurrent programming to speed up further
            meta_data_df = meta_data_cleaner(meta_data_bin)           
            upside_data_df = upside_data_cleaner(upside_data_bin)
            downside_data_df = downside_data_cleaner(downside_data_bin)
            market_score_df = pd.DataFrame(marker_bin, columns= ['time', 'upside', 'downside'])
            market_score_df['count'] = [x for x in range(len(market_score_df))]
            combined_marketscore_df = market_score_df.merge(upside_data_df, how='outer', on='count').merge(downside_data_df, how='outer', on='count').merge(meta_data_df, how='outer', on='count')
            combined_marketscore_df.drop(columns='count', inplace=True)
            names = [pull_name(file)]
            for name in names:
                combined_marketscore_df.to_parquet(f"C:\\Users\\17197\\Documents\\Coding Projects\\Market Score Data\\MarketScore Ticker\\{name}.parquet", index=False)

json_to_csv_converter(test_file)

'''
tl_range = range(len(tuple_list))
dict1 = {0:0}
for x, y in zip(tl_range, tuple_list):
dict1.update({x:y})
dict.update(dict1)
'''