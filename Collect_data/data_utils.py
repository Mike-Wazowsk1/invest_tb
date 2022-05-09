import pandas as pd
import os
from tqdm.auto import tqdm

#PATH_TO_SHARES = "Shares/shares/"
PATH_TO_SHARES = "Shares/2022_now/shares/"

def make_table(PATH):
    result_df = pd.DataFrame()
    files = os.listdir(PATH)
    for f in tqdm(files):
        df = pd.read_parquet(PATH + f)
        result_df = pd.concat([result_df, df], ignore_index=True)
    result_df.to_parquet('Shares_market_data2022_now.parquet')


make_table(PATH_TO_SHARES)
