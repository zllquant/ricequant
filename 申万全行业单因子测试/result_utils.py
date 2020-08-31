import rqdatac as rqd
import datetime
import pickle
import os
import pandas as pd
import matplotlib.pyplot as plt
import multiprocessing
from tqdm import tqdm

plt.style.use('ggplot')
rqd.init()


class BestFactorBacktest:

    @staticmethod
    def get_all_net_value_df(path):
        """
        获得28个行业每个因子每日净值
        返回:
                        因子1     因子2     因子3     ...
        行业1     2010    1         1        1
                  2011   1.2       1.5      1.8
                  2012   2.0       0.9      0.7
                  ...
        行业2     2010
                  2011
                  2012
                  ...
        ...       ...
        """
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
            for factor in [i for i in os.listdir(path1) if os.path.splitext(i)[-1] == '.pkl']:
                path2 = path1 + '\\' + factor
                with open(path2, 'rb') as pf:
                    result = pickle.load(pf)
                config, backtest = result[0], result[1]
                if backtest is None:
                    continue
                portfolio = backtest['sys_analyser']['portfolio']
                # 净值
                net_value = portfolio['unit_net_value']
                factor_name = factor[:-4]
                # 列名
                net_value.name = factor_name
                factor_net_value = pd.concat([factor_net_value, net_value], axis=1)
            factor_net_value['industry'] = industry_name
            all_net_value_df = pd.concat([all_net_value_df, factor_net_value])
        all_net_value_df.index.name = 'date'
        all_net_value_df = all_net_value_df.reset_index().set_index(['industry', 'date'])

        return all_net_value_df

    @staticmethod
    def get_best_factor(net_value, past_window, reverse=False):
        """
        得到每个行业每天每个因子过去past_window个交易日的收益
        从而得到当天过去表现最好或最差的因子
        """
        past_profit = net_value.groupby('industry').pct_change(past_window).dropna(how='all')
        if not reverse:
            best_factor = past_profit.idxmax(axis=1)
        else:
            best_factor = past_profit.idxmin(axis=1)
        best_factor = best_factor.reset_index().set_index(['date', 'industry'])
        return best_factor

    @staticmethod
    def get_industry_weight(date, benchmark):
        """
        获得给定日期某指数的申万一级行业权重
        返回:pd.Series()
        """
        # 今天指数成分股
        components = rqd.index_components(benchmark, date)
        # 股票所属行业series
        industry = rqd.shenwan_instrument_industry(components, date).loc[:, 'index_name'].rename('industry')
        # 指数行业权重
        industry_weight = rqd.index_weights(benchmark, date).groupby(industry).sum()

        return industry_weight

    @staticmethod
    def get_daily_profit(best_factor, net_value, interval=20, benchmark='000905.XSHG'):
        """
        
        :param best_factor: 每个日期每个行业过去表现最好的因子
        :param net_value: 每个行业每天每个因子的净值
        :param interval: 调仓间隔
        :param benchmark:
        :return: 策略和基准的每日净值及超额收益
        """
        dates = best_factor.index.levels[0].tolist()
        industry = best_factor.index.levels[1].tolist()
        # 初始化
        daily_profit = pd.DataFrame(index=best_factor.index, columns=['profit', 'weight'])
        lastdate = None
        # 调仓日列表
        balance_dates = dates[::interval]
        for date in tqdm(dates):
            if lastdate is None:
                best_factor_series = best_factor.loc[date]
                industry_weight = BestFactorBacktest.get_industry_weight(date, benchmark)
                lastdate = date
                continue

            # 调仓日轮换因子和行业权重
            if date in balance_dates:
                # 轮换所有行业过去表现最好的因子
                best_factor_series = best_factor.loc[lastdate]
                industry_weight = BestFactorBacktest.get_industry_weight(date, benchmark)

            for ids in industry:
                # 选股因子
                select_factor = best_factor_series.loc[ids].squeeze()
                last_net_value = net_value.loc[(ids, lastdate), select_factor]
                current_net_value = net_value.loc[(ids, date), select_factor]
                daily_profit.loc[(date, ids), 'profit'] = current_net_value / last_net_value - 1
                daily_profit.loc[(date, ids), 'weight'] = industry_weight[ids]
            # 权重归一
            daily_profit.loc[date, 'weight'] = (daily_profit.loc[date, 'weight'] / daily_profit.loc[
                date, 'weight'].sum()).values
            lastdate = date

        daily_profit.dropna(inplace=True)
        daily_profit_total = daily_profit.groupby(level=0).apply(lambda x: (x['profit'] * x['weight']).sum())

        # benchmark
        benchmark_profit = rqd.get_price(benchmark, dates[0], dates[-1], fields=['close'])
        benchmark_profit = benchmark_profit.pct_change().dropna()
        # strategy,benchmark,excess
        profits_df = daily_profit_total.to_frame()
        profits_df.columns = ['strategy']
        profits_df['bench'] = benchmark_profit
        profits_df = profits_df.apply(lambda x: (x + 1).cumprod())
        profits_df['excess'] = profits_df['strategy'] - profits_df['bench']

        return profits_df

    @staticmethod
    def visualization(df, past_window, interval):
        plt.figure(figsize=(16, 6))
        plt.plot(df)
        plt.legend(['strategy', 'benchmark', 'excess'])
        plt.title(f'{past_window}past profit,{interval} rebalance')
        plt.show()

    @staticmethod
    def run(past_window, interval, all_net_value_df):
        best_factor = BestFactorBacktest.get_best_factor(all_net_value_df, past_window)
        profits_df = BestFactorBacktest.get_daily_profit(best_factor, all_net_value_df, interval, '000905.XSHG')
        BestFactorBacktest.visualization(profits_df, past_window, interval)


if __name__ == '__main__':
    all_net_value_df = BestFactorBacktest.get_all_net_value_df('results')
    # pool = multiprocessing.Pool(6)
    # for i in [30, 60, 90, 120, 240]:
    #     for j in [5, 10, 20, 40, 60]:
    #         pool.apply_async(func=BestFactorBacktest.run,
    #                          args=(i, j, all_net_value_df))
    # pool.close()
    # pool.join()
