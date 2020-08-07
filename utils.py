from typing import Mapping
import pandas as pd
import rqdatac as rqd

def get_universe(date):
    return (
        rqd.all_instruments(type='CS', date=date)
        .loc[lambda df: ~df['status'].isin(['Delisted', 'Unknown'])]
        .loc[:, 'order_book_id']
        .tolist()
    )

def get_industry(universe, date):
    return (
        rqd.shenwan_instrument_industry(universe, date)
        .loc[:, 'index_name']
        .rename('industry')
    )

def drop_ST(universe, date):
    is_ST = rqd.is_st_stock(universe, date, date).squeeze()
    return is_ST.index[~is_ST]


def drop_suspended(universe, date):
    is_suspended = rqd.is_suspended(universe, date, date).squeeze()
    return is_suspended.index[~is_suspended]


def drop_recently_listed(universe, date, min_days_listed=60):
    instruments = rqd.instruments(universe)
    return [inst.order_book_id
            for inst in instruments
            if inst.days_from_listed(date) > min_days_listed]

def select_top_N_percent(universe, date, scorer, N, min_selected=0, grouper=None,):
    if isinstance(scorer, Mapping):
        scores = pd.Series(scorer)
    elif isinstance(scorer, pd.Series):
        scores = scorer
    elif callable(scorer):
        scores = scorer(universe, date)
    else:
        raise ValueError(f'Unsupported scorer type: {type(scorer)}')
        
    if grouper is None:
        ranking = scores.rank(ascending=False, pct=True)
        cutoff = max(N, min_selected / len(scores))
        selected = ranking[ranking <= cutoff].index
        return selected

    else:
        grouping = grouper(universe, date) if callable(grouper) else grouper
        selected = []
        for key, scores_of_group in scores.groupby(grouping):
            group = scores_of_group.index
            sel = select_top_N_percent(
                group, date, scores_of_group, N, min_selected,
                grouper=None, 
            )
            selected.extend(sel)
        return selected
    
    
