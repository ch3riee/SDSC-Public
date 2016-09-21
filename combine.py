import pandas as pd
from pandas import HDFStore
import os

df_list = []

for root, dirs, files in os.walk('/home/ssnazrul/Engility/Pickles', topdown=False):
	for file in files:
		path = os.path.join(root, file)
		p = pd.read_pickle(path)
		df_list.append(p)

merged_df = pd.concat(df_list, axis=0)
print merged_df.shape
#db = HDFStore('merged.pkl')
#db['Database'] = merged_df
#db.close()

merged_df.to_pickle('merged.pkl')
