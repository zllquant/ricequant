import rqalpha as rqa
import rqdatac as rqd
import rqoptimizer as rqo
import multiprocessing
import utils
import datetime
import time
import pickle
import os

rqd.init()


def get_target_stocks(context, factor, ascending, percent_selected, min_selected, grouper):
    """获得当天需要买入的股票"""
    # 前一个交易日
    selection_date = rqd.get_previous_trading_date(context.now)
    # 前一个交易日的指数成分股
    universe = rqd.index_components(context.index_stockpool, selection_date)
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
    target_portfolio = rqo.portfolio_optimize(
        context.target_stocks, context.now, **optimization_args
    )
    return target_portfolio.loc[lambda x: x != 0]


def init(context):
    pass


def handle_bar(context, bar_dict):
    # 如果是False(不是月初),直接返回
    if not _should_rebalance(context):
        return
    print(context.now)
    print("现有持仓数:", len(context.portfolio.positions))
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
        if abs(gap)<_money_for_one_lot(order_book_id):
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


def my_run(init, handle_bar, config):
    _code = config['extra']['context_vars']['index_stockpool']
    _factor = config['extra']['context_vars']['stock_selection_args']['factor']
    backtest_results = rqa.run_func(init=init, handle_bar=handle_bar, config=config)
    if not os.path.exists(f'results/{_code}'):
        os.mkdir(f'results/{_code}')
    with open(f'results/{_code}/{_factor}.pkl', 'wb') as pf:
        pickle.dump((config, backtest_results), pf)


if __name__ == '__main__':

    MILLION = 1_000_000
    BILLION = 1000 * MILLION

    FACTORS = [
        ('depreciation_per_share_ttm', True),
        ('cash_equivalent_per_share_ttm', False),
        ('adjusted_return_on_equity_ttm', False),
        ('adjusted_return_on_equity_diluted_ttm', False),
        ('ocf_to_current_ratio_ttm', False),
        ('depreciation_and_amortization_ttm', False),
        ('cash_equivalent_increase_growth_ratio_ttm', False)
    ]

    s = datetime.datetime.now()
    for code in utils.INDUSTRY_SHENWAN_TOP_10:
        process_list = []
        for fac, asc in FACTORS:
            config = {
                "base": {
                    "matching_type": "current_bar",
                    "start_date": '2015-01-01',
                    "end_date": '2020-07-20',
                    "frequency": '1d',
                    "accounts": {"stock": 0.1 * BILLION},
                },

                "mod": {
                    "sys_analyser": {
                        "enabled": True,
                        "plot": True,
                        "benchmark": code,
                    },
                },

                "extra": {
                    "log_level": 'error',
                    "context_vars": {
                        'cash_cushion': 0.005,
                        'index_stockpool': code,
                        'stock_selection_args': {
                            'factor': fac,
                            'ascending': asc,
                            'percent_selected': 0.2,
                            'min_selected': 15,
                            'grouper': False
                        },

                        'optimization_args': {
                            'benchmark': code,
                            'objective': rqo.MinTrackingError(),
                            'cons': [rqo.WildcardIndustryConstraint(lower_limit=-0.05, upper_limit=0.05, relative=True)]
                        },
                    },
                },
            }

            p = multiprocessing.Process(target=my_run,
                                        kwargs={'init': init, 'handle_bar': handle_bar,
                                                'config': config}
                                        )
            p.start()
            process_list.append(p)
        for i in process_list:
            i.join()

    e = datetime.datetime.now()
    print(f'共计用时: {e - s}')
