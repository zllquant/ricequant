import rqalpha as rqa
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
    optimization_args['bnds'] = {'*': (0, 0.15)}
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


if __name__ == '__main__':
    MILLION = 1_000_000
    BILLION = 1000 * MILLION
    FACTORS = [
        ('pb_ratio_ttm', True),
        ('pe_ratio_ttm', True),
        ('ps_ratio_ttm', True),
        ('pcf_ratio_ttm', True),
        ('inc_revenue_ttm', False),
        ('inc_return_on_equity_ttm', False),
        ('operating_profit_growth_ratio_ttm', False),
        ('net_profit_growth_ratio_ttm', False),
        ('gross_profit_growth_ratio_ttm', False),
        ('net_asset_growth_ratio_ttm', False),
        ('net_cash_flow_growth_ratio_ttm', False),
        ('return_on_equity_ttm', False),
        ('return_on_asset_ttm', False),
        ('net_profit_margin_ttm', False),
        ('gross_profit_margin_ttm', False),
        ('profit_from_operation_to_revenue_ttm', False),
        ('account_payable_turnover_rate_ttm', False),
        ('inventory_turnover_ttm', False),
        ('current_ratio_ttm', False),
        ('quick_ratio_ttm', False),
        ('total_asset_turnover_ttm', False)
    ]

    s = datetime.datetime.now()
    for code in utils.INDUSTRY_SHENWAN:
        for fac, asc in FACTORS:
            config = {
                "base": {
                    "matching_type": "current_bar",
                    "start_date": '2014-03-01',
                    "end_date": '2020-07-20',
                    "frequency": '1d',
                    "accounts": {"stock": 0.1 * BILLION},
                },

                "mod": {
                    "sys_analyser": {
                        "enabled": True,
                        "plot": False,
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
                            'min_selected': 10,
                            'grouper': False
                        },

                        'optimization_args': {
                            'benchmark': code,
                            'bnds': '在get_target_portfolio里面改',
                            'objective': rqo.MinTrackingError()
                        },
                    },
                },
            }

            backtest_results = rqa.run_func(init=init, handle_bar=handle_bar, config=config)
            if not os.path.exists(f'results-28-industry/{code}'):
                os.makedirs(f'results-28-industry/{code}')
            with open(f'results-28-industry/{code}/{fac}.pkl', 'wb') as pf:
                pickle.dump((config, backtest_results), pf)

    e = datetime.datetime.now()
    print(f'共计用时: {e - s}')
