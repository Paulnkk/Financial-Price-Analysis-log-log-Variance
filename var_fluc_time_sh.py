import pandas as pd 
import numpy as np
import statistics 
import math 
import multiprocessing
import os

class sd_calc:
    
    # load data and drop irrelevant cols, price based on Close
    def __init__(self):
        
        # distribute work on 6 cores (my machine has 8 cores)
        self.num_cpus = os.cpu_count() - 2
        self.es = pd.read_csv('ES.csv')
        self.es.drop(self.es.columns.difference(['Close']), 1, inplace=True)
        self.ft = pd.read_csv('FT-1min.csv')
        self.ft.drop(self.ft.columns.difference(['Close']), 1, inplace=True)
    # calc sd change 'es' for ft data

    def compute(self):
        # split operations into 10 subproblems
        arg_list = [(x, x*1000, (x+1)*1000, self.es) for x in range(0, 10, 1)]
        
        # start multiprocessing
        with multiprocessing.Pool(self.num_cpus) as mp:
            mp.starmap(self._compute_worker, arg_list)

    @staticmethod
    def _compute_worker(worker_num, start_idx, end_idx, es):

        sd_list = []
        close_list = es['Close'].to_numpy()
        
        # start calc sd 
        for tau in range(start_idx, end_idx, 1):
            return_vec = []
            #print('val of tau:', tau)
            for t in range(len(close_list) - 1):
                
                # break if t + tau exceeds total len of time units
                if (t + tau >= len(close_list)):
                    break
                # Calc percentage change in price over time shifts
                else:
                    delta_return_perc = ((close_list[t + tau] - close_list[t]) / close_list[t + tau]) * 100
                    return_vec.append(delta_return_perc)
            # generate array with var values for 1000 time shifts 
            var_return = np.var(return_vec)
            sd_return = math.sqrt(var_return)
            print('Standard dev:', sd_return)
            sd_list.append(sd_return)

        # convert to numpy
        sd_list = np.array(sd_list)
        np.save('./sd_list_es_fin_'+str(worker_num)+'.npy', sd_list)

if __name__ == '__main__':
    sd = sd_calc().compute()
