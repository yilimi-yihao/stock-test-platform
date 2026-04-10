# 数据库格式与存储说明

## 数据库概览

项目使用 6 个独立 SQLite 数据库，彼此无外键依赖，路径均可通过 `config/settings.json` 配置。

| 配置键 | 默认路径 | 职责 |
|--------|----------|------|
| `database.stock_path` | `data/a_share.db` | 股票核心库 |
| `database.etf_path` | `data/etf.db` | ETF 库 |
| `database.feature_path` | `data/a_share_features.db` | 股票扩展域 |
| `database.event_path` | `data/event_calendar.db` | 事件库 |
| `database.index_constituent_path` | `data/index_constituents.db` | 指数成分变化库 |
| `database.index_forecast_path` | `data/index_forecasts.db` | 指数预测库 |

---

## a_share.db — 股票核心库

代码：`src/sql_tool/db/stock.py`

### `stocks`

股票基础信息，`code` 为主键。

| 字段 | 说明 |
|------|------|
| `code` | 股票代码（6位） |
| `name` | 股票名称 |
| `area` | 地区 |
| `industry` | 行业 |
| `list_date` | 上市日期 |
| `market_cap` | 总市值 |
| `circ_mv` | 流通市值 |
| `pe_ratio` | 市盈率 |
| `pb_ratio` | 市净率 |
| `turnover_rate` | 换手率 |
| `volume_ratio` | 量比 |
| `adj_factor` | 复权因子 |
| `updated_at` | 最近更新时间 |

### `daily_prices`

日线数据，`UNIQUE(code, date)`，`code` 引用 `stocks`。

| 字段 | 说明 |
|------|------|
| `code` | 股票代码 |
| `date` | 交易日 |
| `open/high/low/close` | OHLC |
| `volume` | 成交量 |
| `amount` | 成交额 |

### `fina_indicator`

财务指标，`UNIQUE(code, end_date)`。

字段：`ann_date`、`end_date`、`eps`、`roe`、`roa`、`gross_margin`、`net_margin`、`debt_to_assets`、`current_ratio`、`quick_ratio`

### `income` / `balancesheet` / `cashflow`

利润表、资产负债表、现金流表摘要，均 `UNIQUE(code, end_date)`。

---

## etf.db — ETF 库

代码：`src/sql_tool/db/etf.py`

### `etfs`

ETF 基础信息，`code` 为主键。字段包括 `name`、`market`、`fund_type`、`benchmark`、`management`、`list_date` 等。

### `etf_daily_prices`

ETF 日线，`UNIQUE(code, date)`。

---

## a_share_features.db — 股票扩展域

代码：`src/sql_tool/db/feature.py`

与 `a_share.db` 无外键依赖，`code` 是公共查询键而非外键。

### 主要表

| 表名 | 内容 |
|------|------|
| `concepts` | 概念基表 |
| `stock_concepts` | 股票-概念关联 |
| `moneyflow` | 资金流向（按 code+date） |
| `top_list_events` | 龙虎榜事件 |
| `top_list_traders` | 龙虎榜席位 |

---

## event_calendar.db — 事件库

代码：`src/sql_tool/db/event.py`

### 主要表

| 表名 | 内容 |
|------|------|
| `holidays` | 节假日日历 |
| `major_events` | 重大赛事 / 事件 |

---

## index_constituents.db — 指数成分变化库

代码：`src/sql_tool/db/index_constituent.py`

与其他库无外键依赖，不需要股票库或 ETF 库中存在对应 code。

### `index_entities`

指数实体注册表。

| 字段 | 说明 |
|------|------|
| `index_id` | 自增主键 |
| `index_name` | 指数名称（UNIQUE） |
| `index_code` | 指数代码（如 000300） |
| `category` | 分类，默认"宽基" |
| `benchmark` | 基准说明 |
| `source` | 数据来源 |

注：`index_name` 是实体的唯一标识，中证100与中证A100视为同一实体（2024-10-28更名），数据库中统一以导入时的名称存储。

### `index_constituent_changes`

指数成分调整事件，一行一条调入/调出记录。

| 字段 | 说明 |
|------|------|
| `change_id` | 自增主键 |
| `index_id` | 关联 `index_entities` |
| `announcement_date` | 公告日（YYYYMMDD） |
| `trade_date` | 实施日（YYYYMMDD） |
| `change_type` | `added` / `removed` |
| `code` | 证券代码 |
| `name` | 证券名称 |
| `source_file` | 来源 CSV 文件名 |
| `note` | 备注 |

约束：`UNIQUE(index_id, trade_date, change_type, code)`

### `index_constituent_snapshots` / `index_constituent_items`

快照表，保存完整成分列表快照（非必须，当前主要用 changes 模型）。

---

## index_forecasts.db — 指数预测库

代码：`src/sql_tool/db/index_forecast.py`

### `index_forecasts`

券商对指数调整的预测记录。

| 字段 | 说明 |
|------|------|
| `forecast_id` | 自增主键 |
| `index_name` | 指数名称 |
| `forecast_month` | 预测生效月（YYYYMM） |
| `forecast_direction` | `预测调入` / `预测调出` |
| `stock_code` | 股票代码 |
| `stock_name` | 股票名称 |
| `broker_name` | 券商名称 |
| `source_note` | 来源备注 |

约束：`UNIQUE(index_name, forecast_month, forecast_direction, stock_code, broker_name)`

---

## 更新策略

### 股票 / ETF

1. 拉取列表
2. 对每只标的拉取基础信息 + 日线 + 财务
3. 使用 `INSERT OR REPLACE` / `INSERT OR IGNORE` 写入
4. 增量更新：读取最新日期，只写入更新后的新增数据

### 指数成分变化

通过 `src/sql_tool/tools/index_change_importer.py` 从 CSV 导入，不依赖 Tushare。
源 CSV 由 `src/sql_tool/tools/rebuild_index_csvs.py` 从 PDF / XLSX 原始公告重建。

---

## 备份

直接备份 `data/` 目录下对应 `.db` 文件即可。

```bash
cp data/a_share.db data/a_share.db.backup
cp data/index_constituents.db data/index_constituents.db.backup
```
