## 申万行业因子测试

### 开始时间

- `2014-03-01` (之前的行业成分股有缺失)

### 结束时间

- `2020-07-20`

### 初始资金

- `100,000,000`

### 交易日期

- 每月第一个交易日调仓

### 比较基准

- 行业指数

### 测试流程

1. 上一个交易日的行业指数成分股

2. 剔除当天停牌

3. 剔除ST股票

4. 剔除上市少于60天的股票

5. 获得当天过滤后的成分股的因子分数

6. 按分数排名,选择前` 0.2`的股票`select_top_N_percent`
   - `config`里设置因子名`factor`和排序规则`ascending`
   - 设置买入数量`min_selected:10`
   
7. 用优化器优化个股权重`get_target_portfolio`
   - 目标函数:`MinTrackingError`
   - 约束条件:个股权重约束`(0,0.15)`
   
8. 调仓函数`rebalance`

   1. 清空不在目标组合的股票
   2. 计算每个股票目标价值和当前价值的差值
   3. 先卖出
   4. 后买入

