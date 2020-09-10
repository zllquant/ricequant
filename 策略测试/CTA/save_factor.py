import numpy as np
import rqdatac as rqd
from datetime import datetime
import pickle

rqd.init()


def cal_score(close, indicator):
    close_list = close.tolist()
    total_score = 0
    for i, price in enumerate(close_list):
        score = 1
        if i == len(close_list) - 1:
            break
        # 等差数列
        arithmetic = np.linspace(price, close_list[-1], len(close_list) - i)
        # 比较序列
        compare = close_list[i:]
        if indicator == 'peak':
            # 有大于等差数列的得分为0
            condition = np.where(compare > arithmetic, 1, 0).sum()
        elif indicator == 'trough':
            condition = np.where(compare < arithmetic, 1, 0).sum()
        if condition > 0:
            score = 0
        total_score += score
    return total_score


start = datetime.now()
start_date = '2013-12-01'
end_date = '2020-09-01'
window = 60

dates = rqd.get_trading_dates(start_date, end_date)

universe = set()
for k, v in rqd.index_components('000985.XSHG', start_date=start_date, end_date=end_date).items():
    universe = universe.union(set(v))
universe = list(universe)

close_df = rqd.get_price(universe, start_date=start_date, end_date=end_date, fields='close', expect_df=True)

close_df['peak'] = close_df['close'].groupby(level=0).rolling(window).apply(cal_score,
                                                                            kwargs={'indicator': 'peak'}).values
close_df['trough'] = close_df['close'].groupby(level=0).rolling(window).apply(cal_score,
                                                                              kwargs={'indicator': 'trough'}).values
close_df['factor'] = close_df['peak'] - close_df['trough']
close_df.dropna(inplace=True)
with open(r'peak_trough_factor.pkl', 'wb') as pf:
    pickle.dump(close_df, pf)
end = datetime.now()
print(end - start)
