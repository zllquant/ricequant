import pandas as pd
from rqams_client import RQAMSClient
import rqdatac as rqd
rqd.init()


client = None
def get_AMS_client(account, password):
    global client
    if client is None:
        client = RQAMSClient(account, password)
    return client


def get_asset_unit(name):
    for key, au in get_AMS_client().asset_units.items():
        if au.name == name:
            return au
    else:
        raise ValueError(f'There are no asset units named {name}')

        
def get_holdings(name, date):
    date = pd.to_datetime(date).date()
    today = pd.datetime.now().date()
    if date == today:
        return get_realtime_holdings(name)
    else:
        return get_historical_holdings(name, date)
    
def get_historical_holdings(name, date):
    au = get_asset_unit(name)
    holdings = au.get_holdings(date)
    return _parse_historical_holdings(holdings)

def get_realtime_holdings(name):
    au = get_asset_unit(name)
    holdings = au.get_current_holdings()
    return _parse_realtime_holdings(holdings)

def _parse_realtime_holdings(holdings):
    return (
        holdings
        .droplevel(1)
        .loc[lambda df: df.asset_type == 'stock']
    )

def _parse_historical_holdings(holdings):
    return (
        holdings
        .set_index('order_book_id')
        .loc[lambda df: df.asset_type == 'stock']
    )

def generate_trade_records(current_position, target_portfolio, capital, datetime, round_to_lots=True):
    datetime = pd.to_datetime(datetime)
    
    union = target_portfolio.index.union(current_position.index)
    current_position = current_position.reindex(union, fill_value=0)
    target_portfolio = target_portfolio.reindex(union, fill_value=0)
    
    price = rqd.get_price(union, start_date=datetime, end_date=datetime, fields=['close']).squeeze()
    
    target_position = target_portfolio * capital / price
    
    trade_records = pd.DataFrame(index=union.rename('order_book_id'))
    trade_records['datetime'] = datetime
    
    if round_to_lots:
        _round = lambda x: round(x / 100) * 100
    else:
        _round = lambda x: x.round(0)
    quantity_to_trade = _round(target_position - current_position)
    trade_records['last_quantity'] = quantity_to_trade.abs()
    
    compute_side = lambda q: 'BUY' if q > 0 else 'SELL'
    trade_records['side'] = quantity_to_trade.apply(compute_side)
    
    trade_records['last_price'] = price
    trade_records['transaction_cost'] = 0
    
    trade_records = trade_records[trade_records['last_quantity'] != 0]
    
    return trade_records.reset_index()

def rebalance(source, target, execution_date, round_to_lots=True):
    current_position_info = get_holdings(source, execution_date)
    capital = current_position_info['market_value'].sum()
    current_position_size = current_position_info['quantity']
    
    target_position_info = get_holdings(target, execution_date)
    size = target_position_info['quantity']
    price = rqd.get_price(target_position_info.index, execution_date, execution_date, fields=['close']).squeeze()
    mv = size * price
    target_portfolio =  mv / mv.sum()
    
    trades = generate_trade_records(current_position_size, target_portfolio, capital, execution_date, round_to_lots)
    return trades

