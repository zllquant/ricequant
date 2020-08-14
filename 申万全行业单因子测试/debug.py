import rqalpha as rqa
import rqalpha_plus
import rqdatac as rqd
import rqoptimizer as rqo
import datetime
import pickle
import os
import matplotlib.pyplot as plt

plt.style.use('ggplot')
import pandas as pd

pd.set_option('display.width', 200)
rqd.init()


def get_all_net_value_df(path=r'results-28-industry'):
    # 28个行业文件夹
    industry_list = [i for i in os.listdir(path) if os.path.splitext(i)[-1] == '.INDX']
    # 行业名称字典
    industry_name_dict = {i: rqd.instruments(i).symbol for i in industry_list}

    all_net_value_df = pd.DataFrame()
    # 每个行业里面的因子
    for ids in industry_list:
        industry_name = industry_name_dict[ids]
        path1 = path + '\\' + ids
        # 一个行业里面的因子
        factor_net_value = pd.DataFrame()
        for factor in [i for i in os.listdir(path1)]:
            path2 = path1 + '\\' + factor
            with open(path2, 'rb') as pf:
                result = pickle.load(pf)
            config, backtest = result[0], result[1]
            if backtest is None:
                continue
            portfolio = backtest['sys_analyser']['portfolio']
            # 净值
            net_value = portfolio['static_unit_net_value']
            factor_name = factor.split('.')[0]
            # 列名
            net_value.name = factor_name
            factor_net_value = pd.concat([factor_net_value, net_value], axis=1)
        factor_net_value['industry'] = industry_name
        all_net_value_df = pd.concat([all_net_value_df, factor_net_value])
        all_net_value_df.index.name = 'date'
        all_net_value_df = all_net_value_df.reset_index().set_index(['industry', 'date'])

        return all_net_value_df


if __name__ == '__main__':
    all_net_value_df = get_all_net_value_df('results')
    all_net_value_df.to_excel('全行业财务因子2014-2020净值(前10%).xlsx')
