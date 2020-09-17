# 可以自己import我们平台支持的第三方python模块，比如pandas、numpy等。
import pandas as pd

# 定义一个自有方法类
class fT():
    
    # helper functions for normalizing data structure
    @staticmethod
    def days_listed_filter(symbols, date, cutoff):
        """Filter instrument with listed days less than a specified cutoff"""
        instrument_list = instruments(symbols)
        
        delta_days = lambda inst: (date - pd.Timestamp(inst.listed_date)).days
        return {inst.order_book_id for inst in instrument_list if delta_days(inst) > cutoff}
    
    @staticmethod
    def pe_filter(symbols, date, cutoff=0):
        """Filter instrument with PE ratio less than a specified cutoff"""
        pe = get_factor(symbols, 'pe_ratio')
        return set(pe.index[pe > cutoff])
    
    @staticmethod
    def market_cap_filter(symbols, date, cutoff):
        """Filter instrument with market cap less than a specified cutoff"""
        market_cap = get_factor(symbols, 'market_cap')
        return set(market_cap.index[market_cap > cutoff])
    
    # functions for computing factors
    @staticmethod
    def sp_ratio(symbols, date):
        ps = get_factor(symbols, 'ps_ratio')
        return 1 / ps
    
    @staticmethod
    def roe(symbols, date):
        return get_factor(symbols, 'return_on_equity')
    
    @staticmethod
    def cfp_ratio(symbols, date):
        pcf = get_factor(symbols, 'pcf_ratio')
        return 1 / pcf
    
    @staticmethod
    def ep_ratio(symbols, date):
        pe = get_factor(symbols, 'pe_ratio')
        return 1 / pe
    
    @staticmethod
    def asset_to_debt_ratio(symbols, date):
        d_to_a = get_factor(symbols, 'debt_to_asset_ratio')
        return 1 / d_to_a
    
    @staticmethod
    def inc_adjusted_net_profit(symbols, date):
        return get_factor(symbols, 'inc_adjusted_net_profit')
    
    # helper functions data processing
    @staticmethod
    def refine_universe(symbols, date):
        """
        Filter stocks by a set of criteria, ST, suspension, etc.
        Returns a pandas Index object
        """
        
        not_st = {sym for sym in symbols if not is_st_stock(sym)}
        not_suspended = {sym for sym in symbols if not is_suspended(sym)}
        listed_gt_180_days = fT.days_listed_filter(symbols, date, 180)
        
        # PE and market cap should use data on previous trading day
        prev_trading_day = get_previous_trading_date(date)
        pe_gt_0 = fT.pe_filter(symbols, prev_trading_day, 0)
        market_cap_gt_2B = fT.market_cap_filter(symbols, prev_trading_day, 2e9)
        
        refined = (
            not_st & 
            not_suspended &
            pe_gt_0 &
            market_cap_gt_2B
        )
        
        return list(refined)
    
    @staticmethod
    def weighted_score(scores, weights):
        weights = pd.Series({k: v for k, v in weights.items()})
        normalized_weights = weights / weights.sum()
        
        res = (scores
                .mul(normalized_weights, axis=1)
                .sum(axis=1))
        
        return res
    
    # helper functions for constructing portfolio
    @staticmethod
    def build_industry_portfoio(date, industry, num_stock, factor_funcs, factor_weights):
        """
        Select stocks from a given shenwan industry, using input factor functions and weights
        Returns a pandas Series with stock code as index and portfolio weight as values
        """
        universe = shenwan_industry(industry)
        refined_universe = fT.refine_universe(universe, date)
        
        prev_trading_day = get_previous_trading_date(date)
        factors = {name: func(refined_universe, prev_trading_day)
                   for name, func in factor_funcs.items()}
        
        factor_scores = pd.concat(factors, axis=1).rank(ascending=True)
        score = fT.weighted_score(factor_scores, factor_weights)
        
        selected = score.nlargest(num_stock).index
        portfolio = pd.Series(1 / len(selected), index=selected)
        return portfolio
    
    @staticmethod
    def merge_sub_portfolios(sub_portfolios, weights):
        portf_list = []
        for key, portf in sub_portfolios.items():
            portf_list.append(portf * weights[key])
            
        return pd.concat(portf_list)
    
    @staticmethod
    def build_portfolio(date, industry_weights, num_stock_by_industry, factor_funcs, factor_weights):
        sub_portfolios = {}
        for ind, num_stock in num_stock_by_industry.items():
            sub_portfolios[ind] = fT.build_industry_portfoio(date, ind, num_stock, factor_funcs, factor_weights)
        portfolio = fT.merge_sub_portfolios(sub_portfolios, industry_weights)
        return portfolio
    
    # helper functions for rebalncing 
    @staticmethod
    def is_limit_down(order_book_id, bar_dict):
        bar = bar_dict[order_book_id]
        return bar.last <= bar.limit_down

# 在这个方法中编写任何的初始化逻辑。context对象将会在你的算法策略的任何方法之间做传递。
def init(context):
    context_vars = {
        # industry weights
        "industry_weights": {
            '银行': 0.08,
            '休闲服务': 0.08,
            '食品饮料': 0.1,
            '钢铁': 0.1,
            '非银金融': 0.1,
            '采掘': 0.08,
            '计算机': 0.12,
            '医药生物': 0.12,
            '国防军工': 0.1,
            '化工': 0.12
        },
            
        # number of stocks for each industry
        "num_stock_by_industry": {
            '银行': 3,
            '休闲服务': 3,
            '食品饮料': 3,
            '钢铁': 3,
            '非银金融': 3,
            '采掘': 3,
            '计算机': 3,
            '医药生物': 3,
            '国防军工': 3,
            '化工': 3, 
        },

        # factor weights
        "factor_weights": {
            'sp_ratio': 1,
            'roe': 1,
            'cfp_ratio': 1,
        },

        "holding_period": 5, 
        
        # cash reserved for commisions as a proportion of total value
        "cash_cushion": 0.05,
    }
    # 在context中保存全局变量
    context.target_portfolio_history = {}
    context.industry_portfolio_history = {}
    context.industry_weights = context_vars['industry_weights']
    context.num_stock_by_industry = context_vars['num_stock_by_industry']
    context.factor_weights = context_vars['factor_weights']
    context.factor_funcs = {
        'sp_ratio': fT.sp_ratio,
        'roe': fT.roe,
        'cfp_ratio': fT.cfp_ratio,
    }
    context.SHENWAN_INDUSTRY_MAP = {
        "801010.INDX": "农林牧渔",
        "801020.INDX": "采掘",
        "801030.INDX": "化工",
        "801040.INDX": "钢铁",
        "801050.INDX": "有色金属",
        "801080.INDX": "电子",
        "801110.INDX": "家用电器",
        "801120.INDX": "食品饮料",
        "801130.INDX": "纺织服装",
        "801140.INDX": "轻工制造",
        "801150.INDX": "医药生物",
        "801160.INDX": "公用事业",
        "801170.INDX": "交通运输",
        "801180.INDX": "房地产",
        "801200.INDX": "商业贸易",
        "801210.INDX": "休闲服务",
        "801230.INDX": "综合",
        "801710.INDX": "建筑材料",
        "801720.INDX": "建筑装饰",
        "801730.INDX": "电气设备",
        "801740.INDX": "国防军工",
        "801750.INDX": "计算机",
        "801760.INDX": "传媒",
        "801770.INDX": "通信",
        "801780.INDX": "银行",
        "801790.INDX": "非银金融",
        "801880.INDX": "汽车",
        "801890.INDX": "机械设备"
    }
    context.holding_period = context_vars['holding_period']
    context.cash_cushion = context_vars['cash_cushion']
    # 实时打印日志
    logger.info("RunInfo: {}".format(context.run_info))

# before_trading此函数会在每天策略交易开始前被调用，当天只会被调用一次
def before_trading(context):
    days = (context.now.date() - context.run_info.start_date).days
    context.should_rebalance = (days % context.holding_period == 0)


# 你选择的证券的数据更新将会触发此段逻辑，例如日或分钟历史数据切片或者是实时数据切片更新
def handle_bar(context, bar_dict):
    # 开始编写你的主要的算法逻辑

    # bar_dict[order_book_id] 可以拿到某个证券的bar信息
    # context.portfolio 可以拿到现在的投资组合信息

    # 使用order_shares(id_or_ins, amount)方法进行落单

    # TODO: 开始编写你的算法吧！
    if context.should_rebalance:
        # build industry portfolios
        industry_portfolios = {}
        target_stocks = set()
        for ind, num_stock in context.num_stock_by_industry.items():
            portf = fT.build_industry_portfoio(
                context.now,
                ind,
                num_stock,
                context.factor_funcs,
                context.factor_weights
            )
            industry_portfolios[ind] = portf
            target_stocks.update(portf.index)
            
        # record for later analysis
        context.industry_portfolio_history[context.now.strftime('%Y%m%d')] = industry_portfolios
        
        # clear unwanted stocks
        # adjust industry weights for unsellable stocks
        industry_weights = context.industry_weights.copy()
        positions = context.portfolio.positions
        for order_book_id, pos in positions.items():
            if order_book_id not in target_stocks:
                # if unsellable, deduct its weight from the corresponding industry
                if is_suspended(order_book_id) or fT.is_limit_down(order_book_id, bar_dict):
                    ind_index = shenwan_instrument_industry(order_book_id)
                    ind = context.SHENWAN_INDUSTRY_MAP[ind_index]
                    industry_weights[ind] -= pos.value_percent
                else:
                    order_target_value(order_book_id, 0)
        
        
        # build target portfolio using adjusted industry weights
        target_portfolio = fT.merge_sub_portfolios(industry_portfolios, industry_weights)
        
        context.target_portfolio_history[context.now.strftime('%Y%m%d')] = target_portfolio
        
        # adjust to taget porfolio
        capital = context.stock_account.total_value * (1 - context.cash_cushion)
        positions = context.portfolio.positions
        to_sell, to_buy = {}, {}
        for order_book_id, weight in target_portfolio.items():
            target_value = capital * weight
            delta_value = target_value - positions[order_book_id].market_value
            if delta_value > 0:
                to_buy[order_book_id] = delta_value
            else:
                to_sell[order_book_id] = delta_value
                
        # to avoid liquidity issue, sell first, buy second
        for order_book_id, value in to_sell.items():
            order_value(order_book_id, value)
            
        for order_book_id, value in to_buy.items():
            order_value(order_book_id, value)

# after_trading函数会在每天交易结束后被调用，当天只会被调用一次
def after_trading(context):
    pass