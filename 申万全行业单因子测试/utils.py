from typing import Mapping
import pandas as pd
import rqdatac as rqd
import pickle

rqd.init()

INDUSTRY_SHENWAN_TOP_10 = [
    '801890.INDX', '801030.INDX', '801150.INDX', '801080.INDX', '801750.INDX',
    '801730.INDX', '801880.INDX', '801760.INDX', '801160.INDX', '801140.INDX'
]

INDUSTRY_SHENWAN = [
    '801010.INDX', '801020.INDX', '801030.INDX', '801040.INDX', '801050.INDX', '801080.INDX', '801110.INDX',
    '801120.INDX', '801130.INDX', '801140.INDX', '801150.INDX', '801160.INDX', '801170.INDX', '801180.INDX',
    '801200.INDX', '801210.INDX', '801230.INDX', '801710.INDX', '801720.INDX', '801730.INDX', '801740.INDX',
    '801750.INDX', '801760.INDX', '801770.INDX', '801780.INDX', '801790.INDX', '801880.INDX', '801890.INDX',
]

INDUSTRY_ZHONGXIN = [
    'CI005001.WI', 'CI005002.WI', 'CI005003.WI', 'CI005004.WI', 'CI005005.WI', 'CI005006.WI', 'CI005007.WI',
    'CI005008.WI', 'CI005009.WI', 'CI005010.WI', 'CI005011.WI', 'CI005012.WI', 'CI005013.WI', 'CI005014.WI',
    'CI005015.WI', 'CI005016.WI', 'CI005017.WI', 'CI005018.WI', 'CI005019.WI', 'CI005020.WI', 'CI005021.WI',
    'CI005022.WI', 'CI005023.WI', 'CI005024.WI', 'CI005025.WI', 'CI005026.WI', 'CI005027.WI', 'CI005028.WI',
    'CI005029.WI', 'CI005030.WI'
]


# def get_universe(date):
#     return (
#         rqd.all_instruments(type='CS', date=date)
#             .loc[lambda df: ~df['status'].isin(['Delisted', 'Unknown'])]
#             .loc[:, 'order_book_id']
#             .tolist()
#     )


def get_industry(universe, date):
    return (
        rqd.shenwan_instrument_industry(universe, date).loc[:, 'index_name'].rename('industry')
    )


def drop_st(universe, date):
    is_st = rqd.is_st_stock(universe, date, date).squeeze()
    assert isinstance(is_st, pd.Series), 'is_st is not series'
    return is_st.index[~is_st]


def drop_suspended(universe, date):
    # squeeze(axis=0):DataFrames with a single column or a single row are squeezed to a Series
    is_suspended = rqd.is_suspended(universe, date, date).squeeze()
    assert isinstance(is_suspended, pd.Series), 'is_suspended is not series'
    return is_suspended.index[~is_suspended]


def drop_recently_listed(universe, date, min_days_listed=60):
    # 一个列表,里面元素是Instrument对象
    instruments = rqd.instruments(universe)
    return [inst.order_book_id
            for inst in instruments
            if inst.days_from_listed(date) > min_days_listed]


def select_top_N_percent(universe, date, scorer, n, ascending, min_selected, grouper):
    """选前N%的股票"""
    if isinstance(scorer, Mapping):
        scores = pd.Series(scorer)
    elif isinstance(scorer, pd.Series):
        scores = scorer
    # scorer是个函数
    elif callable(scorer):
        scores = scorer(universe, date)
    else:
        raise ValueError(f'Unsupported scorer type: {type(scorer)}')

    if not grouper:
        # 排名,0-1的分位数
        ranking = scores.rank(ascending=ascending, pct=True)
        # 取 N 和最小比例的较大值
        assert min_selected < len(scores), 'min_selected is less than len(scores)'
        cutoff = max(n, min_selected / len(scores))
        # 取排名较小的股票
        selected = ranking[ranking <= cutoff].index
        return selected
    # 行业分组
    else:
        # grouping:一个分组的series
        grouping = get_industry(universe, date)
        selected = []
        # 对每一组进行排序选股
        for _, scores_of_group in scores.groupby(grouping):
            group = scores_of_group.index
            sel = select_top_N_percent(
                group, date, scores_of_group, n, ascending, min_selected,
                grouper=False
            )
            selected.extend(sel)
        return selected


_RICEQUANT_FACTORS = rqd.get_all_factor_names()

_CACHE = {}  # factor -> pd.Series


def _get_factor_from_local_file(universe, factor, start_date):
    if factor in _CACHE:
        return _CACHE[factor][start_date][universe]
    else:
        with open(f'{factor}.pkl', 'rb') as pf:
            data = pickle.load(pf)
        _CACHE[factor] = data
        return data[start_date][universe]


def get_factor(universe, factor, start_date, end_date):
    if factor in _RICEQUANT_FACTORS:
        return rqd.get_factor(universe, factor, start_date, end_date)
    else:
        return _get_factor_from_local_file(universe, factor, start_date)
