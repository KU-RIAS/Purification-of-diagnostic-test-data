import numpy as np
import pandas as pd
import multiprocessing as mp


class MultiProcess:

    @staticmethod
    def multi_process(func, input_df, cores_to_use):
        # multiprocessing.freeze_support()
        df_split = np.array_split(input_df, cores_to_use)
        pool = mp.Pool(cores_to_use)
        df = pd.concat(pool.map(func, df_split))
        pool.close()
        pool.join()
        return df
        
