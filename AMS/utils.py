from typing import Mapping
import pandas as pd
import rqdatac as rqd
import pickle

rqd.init()

def get_universe(date):
    return (
        rqd.all_instruments(type='CS', date=date)
            .loc[lambda df: ~df['status'].isin(['Delisted', 'Unknown'])]
            .loc[:, 'order_book_id']
            .tolist()
    )


def get_industry(universe, date):
    return (
        rqd.shenwan_instrument_industry(universe, date).loc[:, 'index_name'].rename('industry')
    )


def drop_st(universe, date):
    is_st = rqd.is_st_stock(universe, date, date).squeeze()
    assert isinstance(is_st, pd.Series), 'is_st is not series'
    return is_st.index[~is_st]


def drop_suspended(universe, date):
    is_suspended = rqd.is_suspended(universe, date, date).squeeze()
    assert isinstance(is_suspended, pd.Series), 'is_suspended is not series'
    return is_suspended.index[~is_suspended]


def drop_recently_listed(universe, date, min_days_listed=60):
    # 一个列表,里面元素是Instrument对象
    instruments = rqd.instruments(universe)
    return [inst.order_book_id
            for inst in instruments
            if inst.days_from_listed(date) > min_days_listed]


def select_top_N_percent(universe, date, score, n, ascending, min_selected, grouper):
    """选前N%的股票"""
    if isinstance(score, Mapping):
        scores = pd.Series(score)
    elif isinstance(score, pd.Series):
        scores = score
    # scorer是个函数
    elif callable(score):
        scores = score(universe, date)
    else:
        raise ValueError(f'Unsupported scorer type: {type(scorer)}')

    if not grouper:
        # 排名,0-1的分位数
        ranking = scores.rank(ascending=ascending, pct=True)
        # 取 N 和最小比例的较大值
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


def get_factor(universe, factor, start_date, end_date):
    if factor in _RICEQUANT_FACTORS:
        return rqd.get_factor(universe, factor, start_date, end_date)
    else:
        return _get_factor_from_local_file(universe, factor, start_date)
    
    
def _get_factor_from_local_file(universe, factor, start_date):
    if factor in _CACHE:
        return _CACHE[factor][start_date][universe]
    else:
        with open(f'{factor}.pkl', 'rb') as pf:
            data = pickle.load(pf)
        _CACHE[factor] = data
        return data[start_date][universe]



