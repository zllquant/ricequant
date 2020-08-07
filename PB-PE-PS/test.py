import rqalpha as rqa
import rqalpha_plus
import rqdatac as rqd
import rqoptimizer as rqo
import datetime
import pickle
import os
import pprint
import pandas as pd
pd.set_option('display.max_columns', 200)
pd.set_option('display.width', 200)

rqd.init()

path = r'D:\jpnb\ricequant\ricequant实习项目\ricequant\backtest-results\shenwan-28-industry'
dirlist = [i for i in os.listdir(path) if os.path.splitext(i)[-1] == '.INDX']

for dir_ in dirlist[4:]:
    path1 = path + '\\' + dir_
    for pkl in [i for i in os.listdir(path1) if i in ['pb_ratio_ttm.pkl', 'pe_ratio_ttm.pkl', 'ps_ratio_ttm.pkl']][2:]:
        path2 = path1 + '\\' + pkl
        with open(path2, 'rb') as pf:
            result = pickle.load(pf)
        config, backtest = result[0], result[1]
        summary = backtest['sys_analyser']['summary']
        pprint.pprint(config)
        print()
        pprint.pprint(summary)
        print(backtest['sys_analyser']['stock_account'])
        break
    break
