import pandas as pd
import numpy as np
from datetime import datetime
import rqalpha as rqa
import rqalpha_plus
import rqdatac as rqd
import pickle

rqd.init()

# 读取股票池
with open('data/2005-2020股票池.pkl', 'rb') as pf:
    universe_dict = pickle.load(pf)

# 读入pe_ttm数据
pe_ratio_ttm_df = pd.read_csv(r'data/pe_ratio_ttm_df_2005-2020.csv', index_col=0)
# 读入roe数据
roe_df = pd.read_csv(r'data/roe_年报数据_2000-2020.csv', index_col=0)
roe_df['announce_date'] = pd.to_datetime(roe_df['announce_date'])
roe_df = roe_df.reset_index().set_index(['order_book_id', 'announce_date'])
roe_df = roe_df.drop(columns='listed_date')

# 十年roe均值
roe_df_ma10 = roe_df.groupby(level=0).rolling(10, min_periods=1).mean().droplevel(0)


def buy_signal(context, code):
    factor1 = pe_ratio_ttm_df.loc[context.now.strftime('%Y-%m-%d'), code]
    if factor1 < 0:
        return
    if code not in roe_df_ma10.index.levels[0]:
        return
    # 十年平均roe
    factor2 = roe_df_ma10.loc[code, 'return_on_equity'].asof(context.now)
    # roe>0.18
    if factor2 < context.factor2_threshold * 100:
        return

    # 条件1:pe<11.27
    if factor1 <= context.factor1_threshold:
        context.pe_ratio_ttm[code] = factor1
        return True


def sell_signal(context, code):
    factor1 = pe_ratio_ttm_df.loc[context.now.strftime('%Y-%m-%d'), code]
    if code in context.pe_ratio_ttm:
        if (factor1 > context.pe_ratio_ttm[code] * context.sell_threshold1) or (factor1 > context.sell_threshold2):
            del context.pe_ratio_ttm[code]
            return True


def rebalance(context, to_sell, to_buy):
    if len(to_sell) > 0:
        for code in to_sell:
            rqa.api.order_to(code, 0)

    if len(to_buy) > 0:
        stocklist = set(context.portfolio.positions.keys())
        stocklist = stocklist.union(to_buy)
        weight = 1 / len(stocklist)
        for code in stocklist:
            rqa.api.order_target_percent(code, weight)


def _should_rebalance(context):
    """判断今天是否调仓"""
    # 获得上一个交易日
    prev_trading_day = rqd.get_previous_trading_date(context.now)
    # 判断是否是月初, 是月初返回True
    return prev_trading_day.month != context.now.month


def init(context):
    context.factor1 = 'pe_ratio_ttm'
    context.factor2 = 'return_on_equity_ttm'

    context.factor1_threshold = 11.27
    context.factor2_threshold = 0.18

    context.sell_threshold1 = 3
    context.sell_threshold2 = 30

    # 买入时的pe_ratio_ttm
    context.pe_ratio_ttm = {}


def handle_bar(context, bar_dict):
    if not _should_rebalance(context):
        return
    print(context.now)
    print("现有持仓数:", len(context.portfolio.positions))
    date = context.now.strftime('%Y-%m-%d')
    universe = universe_dict[date]
    to_sell, to_buy = set(), set()
    for code in context.portfolio.positions:
        if sell_signal(context, code):
            to_sell.add(code)
    for code in universe:
        if buy_signal(context, code) and code not in context.portfolio.positions:
            to_buy.add(code)

    print('当期卖出:', len(to_sell), '当期买入:', len(to_buy))
    rebalance(context, to_sell, to_buy)


if __name__ == '__main__':
    config = {
        "base": {
            "start_date": '2005-05-01',
            "end_date": '2020-08-20',
            "frequency": '1d',
            "accounts": {"stock": 1e8},
            "benchmark": '000300.XSHG'
        },

        "mod": {
            "sys_analyser": {
                "enabled": True,
                "plot": True,
            },
        },
        "extra": {
            "log_level": 'error'
        },
    }
    backtest_results = rqalpha_plus.run_func(init=init, handle_bar=handle_bar, config=config)
    unit_net_value = backtest_results['sys_analyser']['portfolio']['unit_net_value']
    trades = backtest_results['sys_analyser']['trades']
    trades.to_csv('策略回测/3倍或30倍卖出.csv')
