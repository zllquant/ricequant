import rqalpha as rqa
import rqalpha_plus
import rqdatac as rqd
import rqoptimizer as rqo
import datetime
import pickle
import os
import pandas as pd

pd.set_option('display.width', 200)
rqd.init()

path = r'results-28-industry'
# 28个行业文件夹
industry_list = [i for i in os.listdir(path) if os.path.splitext(i)[-1] == '.INDX']
# 行业名称字典
industry_name_dict = {i: rqd.instruments(i).symbol for i in industry_list}

# 初始化一张表
dates = rqd.get_trading_dates('2015-03-11', '2020-08-04')
total_results_df = pd.DataFrame(index=pd.MultiIndex.from_product([dates, list(industry_name_dict.values())]),
                                columns=['pb_ratio_ttm', 'pe_ratio_ttm', 'ps_ratio_ttm'])
# 每个行业里面的因子
for ids in industry_list:
    industry_name = industry_name_dict[ids]
    path1 = path + '\\' + ids
    # 一个行业里面的因子
    for factor in [i for i in os.listdir(path1) if i in ['pb_ratio_ttm.pkl', 'pe_ratio_ttm.pkl', 'ps_ratio_ttm.pkl']]:
        path2 = path1 + '\\' + factor
        with open(path2, 'rb') as pf:
            result = pickle.load(pf)
        config, backtest = result[0], result[1]
        summary = backtest['sys_analyser']['portfolio']
        # 过去一年的收益
        last_year_profit = summary['unit_net_value'].pct_change(250).dropna()
        factor_name = factor.split('.')[0]
        # 每一天此行业此因子过去表现
        for date in last_year_profit.index:
            date = date.strftime('%Y-%m-%d')
            total_results_df.loc[(date, industry_name), factor_name] = last_year_profit[date]
        print(f'插入数据:行业:{industry_name} 因子:{factor_name}')
print(total_results_df.apply(lambda x: x.argmax(), axis=1))
