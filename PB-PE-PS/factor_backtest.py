import rqalpha as rqa
import rqalpha_plus
import rqdatac as rqd
import rqoptimizer as rqo
import utils
import datetime
import pickle
import os

rqd.init()


def get_target_stocks(context, factor, ascending, percent_selected, min_selected, grouper):
    """获得当天需要买入的股票"""
    # 前一个交易日
    selection_date = rqd.get_previous_trading_date(context.now)
    selection_date = selection_date.strftime('%Y-%m-%d')
    # 前一个交易日的全市场股票
    universe = utils.get_universe(selection_date)
    # 剔除当天停牌
    universe = utils.drop_suspended(universe, selection_date)
    # 剔除当天ST
    universe = utils.drop_st(universe, selection_date)
    # 剔除上市小于60天
    universe = utils.drop_recently_listed(universe, selection_date, 60)
    # 获得因子分数
    scores = utils.get_factor(universe, factor, selection_date, selection_date)
    # 返回需要买入的股票
    return utils.select_top_N_percent(
        universe, selection_date, scores, percent_selected, ascending, min_selected,
        grouper=grouper,
    )


def get_target_portfolio(context, **optimization_args):
    # 返回每只股票的权重Series
    # rqoptimizer 默认取date前一个交易日的数据
    optimization_args['bnds'] = {'*': (0, 0.15)}
    target_portfolio = rqo.portfolio_optimize(
        context.target_stocks, context.now, **optimization_args
    )
    return target_portfolio.loc[lambda x: x != 0]


# def init(context):
#     pass


def handle_bar(context, bar_dict):
    # 如果是False(不是月初),直接返回
    if not _should_rebalance(context):
        return
    print(context.now)
    print("现有持仓:%5s" % len(context.portfolio.positions))
    context.target_stocks = get_target_stocks(context, **context.stock_selection_args)
    context.target_portfolio = get_target_portfolio(context, **context.optimization_args)
    rebalance(context, bar_dict)


def _should_rebalance(context):
    """判断今天是否调仓"""
    # 获得上一个交易日
    prev_trading_day = rqd.get_previous_trading_date(context.now)
    # 判断是否是月初, 是月初返回True
    is_month_start = (prev_trading_day.month != context.now.month)
    return is_month_start


def rebalance(context, bar_dict):
    # 先清空不在目标组合里面的股票
    positions = context.stock_account.positions
    for order_book_id in positions:
        if order_book_id not in context.target_portfolio:
            rqa.api.order_to(order_book_id, 0)

    # 对每个股票计算目标价值和当前价值的差值
    # 差值为正的是买单, 反之为卖单
    capital = context.stock_account.total_value * (1 - context.cash_cushion)
    to_sell, to_buy = {}, {}
    _money_for_one_lot = lambda order_book_id: bar_dict[order_book_id].close * 100
    for order_book_id, weight in context.target_portfolio.items():
        # 股票目标价值
        target_value = capital * weight
        # 目标和现有之差
        gap = target_value - positions[order_book_id].market_value
        # 买卖至少大于1手股票价值
        if abs(gap) < _money_for_one_lot(order_book_id):
            continue
        elif gap > 0:
            to_buy[order_book_id] = gap
        else:
            to_sell[order_book_id] = gap

    # to avoid liquidity issue, sell first, buy second
    for order_book_id, value in to_sell.items():
        rqa.api.order_value(order_book_id, value)
    for order_book_id, value in to_buy.items():
        rqa.api.order_value(order_book_id, value)


if __name__ == '__main__':
    MILLION = 1_000_000
    BILLION = 1000 * MILLION
    s = datetime.datetime.now()

    config = {
        "base": {
            "start_date": '2015-03-11',
            "end_date": '2020-08-04',
            "frequency": '1d',
            "accounts": {"stock": 0.1 * BILLION},
            "data_bundle_path": r'C:\Users\Administrator\.rqalpha-plus\bundle'
        },

        "mod": {
            "sys_analyser": {
                "enabled": True,
                "plot": True,
                "benchmark": '000985.XSHG',
            },
        },

        "extra": {
            "log_level": 'error',
            "context_vars": {
                'cash_cushion': 0.005,
                'stock_selection_args': {
                    'factor': 'wrost_factor',
                    'ascending': True,
                    'percent_selected': 0.1,
                    'min_selected': 8,
                    'grouper': True
                },

                'optimization_args': {
                    'benchmark': '000985.XSHG',
                    'bnds': {'*': (0, 0.15)},
                    'objective': rqo.MinTrackingError()
                },
            },
        },
    }

    backtest_results = rqalpha_plus.run_func(handle_bar=handle_bar, config=config)
    if not os.path.exists('results'):
        os.makedirs('results')
    with open(f'results/wrost-asc.pkl', 'wb') as pf:
        pickle.dump((config, backtest_results), pf)

    e = datetime.datetime.now()
    print(f'共计用时: {e - s}')
