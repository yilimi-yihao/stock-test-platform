# 数据库格式与存储说明

## 数据库文件

- 默认路径：`data/a_share.db`
- 类型：SQLite 3
- 路径可通过 `config/settings.json` 的 `database.path` 修改

## 核心表

### `stocks`

存储股票基础信息和部分估值字段。

字段：

- `code`：股票代码，主键
- `name`：股票名称
- `area`：地区
- `industry`：行业
- `list_date`：上市日期
- `market_cap`：总市值
- `circ_mv`：流通市值
- `pe_ratio`：市盈率
- `pb_ratio`：市净率
- `turnover_rate`：换手率
- `volume_ratio`：量比
- `adj_factor`：复权因子
- `updated_at`：最近更新时间

### `daily_prices`

存储股票日线数据。

字段：

- `code`
- `date`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `amount`

约束：`UNIQUE(code, date)`

### `fina_indicator`

存储财务指标。

字段：

- `ann_date`
- `end_date`
- `eps`
- `roe`
- `roa`
- `gross_margin`
- `net_margin`
- `debt_to_assets`
- `current_ratio`
- `quick_ratio`

### `income`

存储利润表摘要。

字段：

- `ann_date`
- `end_date`
- `revenue`
- `operate_profit`
- `net_profit`

### `balancesheet`

存储资产负债表摘要。

字段：

- `ann_date`
- `end_date`
- `total_assets`
- `total_liab`
- `total_equity`
- `current_assets`
- `current_liab`
- `cash`
- `accounts_payable`
- `advance_receipts`

### `cashflow`

存储现金流量表摘要。

字段：

- `ann_date`
- `end_date`
- `operate_cash_flow`
- `invest_cash_flow`
- `finance_cash_flow`

## 索引

程序会自动创建以下索引：

- `idx_daily_code_date`
- `idx_daily_date`
- `idx_stocks_industry`
- `idx_fina_code_date`
- `idx_income_code_date`
- `idx_bs_code_date`
- `idx_cf_code_date`

## 查询行为

- 股票列表按 `code` 排序
- 日线数据按 `date DESC` 返回
- 财务数据按 `end_date DESC` 返回
- 数据库统计可返回文件大小、表计数、更新时间和日期范围

## 更新策略

### 导入

1. 拉取股票列表
2. 对单只股票拉取基础信息、日线、财务指标、利润表、资产负债表、现金流
3. 使用 `INSERT OR REPLACE` 写入数据库

### 增量更新

1. 读取本地股票列表
2. 查询每只股票的最新交易日
3. 只写入更新后的新增日线
4. 同步刷新对应财务表

## 备份建议

直接备份 `data/a_share.db` 即可。

```bash
cp data/a_share.db data/a_share.db.backup
```

## 相关代码

- 数据库初始化与查询：`src/sql_tool/database.py`
- 业务封装：`src/sql_tool/service.py`
- API 暴露：`src/sql_tool/api.py`
