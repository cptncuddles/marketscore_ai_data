import pandas as pd
import json
import glob
from IPython.display import display
#using glob module to allow us to grab multiple files at once. This will change once we switch to an API extraction process

#helper function to pull name from imported file to be used in naming new file
def pull_name(string):
    name_list = string.split('\\')
    for x in name_list:
        if ".json" in x:
            name = x.split('.')
            return name[0]
        else:
            pass

json_files = glob.glob("file_path\\*json")
#main function for cleaning this specific dataset, all 3 programs will have a similar stucture, with small changes to accommodate each dataset
def json_to_csv_converter(files):
    for file in files:        
        with open(file) as json_file:
            data = json.load(json_file)
            df_list = []
            data_dump = data['master_counts']
            data_dump = data_dump[1:]
            df1 = pd.DataFrame(data_dump, columns=['time_stamp', 'call count', 'put count'])
            df1['time_stamp'] = pd.to_datetime(df1['time_stamp'], format="%H:%M").dt.time
            df1 = df1.iloc[42:]
            df_list.append(df1)
            names = [pull_name(file)]
#Need to use zip for this double iterator so that each name get's paired properly with it's corresponding file.   
            for df, name in zip(df_list, names):
                df.to_csv(f"file_path\\{name}.csv", index=False)
#current file is csv format with zero index, but can be switched to parquet or even back to json. file type can be determined once a ML structure is decided upon            
            
json_to_csv_converter(json_files)
