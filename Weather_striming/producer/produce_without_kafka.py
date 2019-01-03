import json
import os
import time
import pandas as pd
try:
    import thread
except ImportError:
    import _thread as thread

if __name__ == "__main__":
    
    for year in range(1996, 2014): 
        loadString = "../../storm_data/Stormdata_" + str(year) + "_red.csv"
        data = pd.read_csv(loadString,low_memory=False)
        for month in range(1, 13):
            for day in range(1,31):
                current_ym = year*100 + month
                begining_df = data.loc[(data['BEGIN_YEARMONTH'] == current_ym) & (data['BEGIN_DAY'] == day)]
                begining_df = begining_df.drop_duplicates(subset='EPISODE_ID')
                ending_df = data.loc[(data['END_YEARMONTH'] == current_ym) & (data['END_DAY'] == day)]
                ending_df = ending_df.drop_duplicates(subset='EPISODE_ID')
                for index, row in begining_df.iterrows():
                    #print row['BEGIN_YEARMONTH'],row['BEGIN_DAY'],row['STATE'], row['EVENT_TYPE'],row['EPISODE_ID']
                    json_row = row.to_json()
                    print json_row, "=================================="
                    
                for index, row in begining_df.iterrows():
                    json_row = row.to_json()

                time.sleep(100) 

