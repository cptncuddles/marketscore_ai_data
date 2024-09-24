import pandas as pd
import json
import glob
import time


#this is when we want to view the dataframes in the terminal
#pd.set_option('display.max_columns', None)

test_file = "C:\\Users\\17197\\Documents\\Coding Projects\\Market Score Data\\Trades Data\\incoming_trade_files\\ES-2024-05-22-1-results.json"
directory = glob.glob("C:\\Users\\17197\\OneDrive\\Documents\\Coding Projects\\Market Score Data\\Trades Data\\incoming_trade_files\\*.json")

def pull_name(string):
    name_list = string.split('\\')
    for x in name_list:
        if ".json" in x:
            name = x.split('.')
            return name[0]
        else:
            pass

def trades_cleaner(dir):
    tic = time.perf_counter()
    for file in dir:
        with open(file) as info_dump:
            data = json.load(info_dump)
            data_s1 = data["config"]
            data_s2 = data_s1["trades"] #this is a list of dictionaries
            keys_to_keep = ['trade_count', 'position_type', 'exit_type', 'entry_time', 'total_heat', 'heat_before_peak', 'heat_allowed', 'trade_type', 'exit_time', 'total_points']
#data_to_keep set to empty list to hold the dictionary entries that are parsed through using a dictionary comprehension.
#this will allow for easier parsing with the cleaning function to separate out all of the relevant data sets
            data_to_keep = []
            df_holding_list = []
#this dict comprehension is to get the correct data sets from the larger json pulling out the keys in the 'keys_to_keep' list
            for item in data_s2:
                data_to_keep.append({key: item[key] for key in keys_to_keep})
#this dict comprehension is to pull the actual data out of the embedded dictionaries so that we end with a singular key: value pairing
            for item in data_to_keep:
                new_dict = {key: item[key]['value'] for key in item.keys()}
                new_df = pd.DataFrame.from_dict([new_dict], orient='columns')
                df_holding_list.append(new_df)
#this will concat all of the smaller DF's into a large one for export
            full_df = pd.concat(df_holding_list, ignore_index= True)
#setting our time values as dtype datetime as this will be part of the compound primary key
            full_df['entry_time'] = pd.to_datetime(full_df['entry_time'], format="%H:%M").dt.time
            full_df['exit_time'] = pd.to_datetime(full_df['exit_time'], format="%H:%M").dt.time
            names = [pull_name(file)]
#reusing the naming function for export
            for name in names:
                full_df.to_csv(f"C:\\Users\\17197\\OneDrive\\Documents\\Coding Projects\\Market Score Data\\Trades Data\\outgoing_trade_files\\{name}.csv", index=False)
    toc = time.perf_counter()
    print(f"Completed function in {toc - tic:0.4f} second")

trades_cleaner(directory)
