# 工程结构说明

## 总体分层

```text
sql-tool/
├── config/settings.json          # 配置：token、数据库路径、API host/port
├── data/                         # 6 个独立 SQLite 数据库（见下）
├── docs/                         # 文档
├── resource_data/
│   └── index_change_csvdata/     # 指数成分变化源 CSV
├── src/sql_tool/
│   ├── api/          app.py      # HTTP API（FastAPI）
│   ├── gui/          desktop.py  # 桌面 GUI（tkinter）
│   ├── services/     core.py     # 高层业务门面
│   ├── db/                       # 6 个数据库实现
│   ├── sources/                  # Tushare 数据源
│   ├── tools/                    # 指数 CSV 导入/重建脚本
│   ├── exporters_pkg/            # CSV/Excel/PDF 导出
│   ├── config.py
│   ├── base_database.py
│   └── base_source.py
├── tests/
├── main.py
└── requirements.txt
```

---

## 数据库文件

6 个数据库彼此独立，无跨库外键依赖：

| 文件 | 展示名 | 配置键 | 职责 | 代码 |
|------|--------|--------|------|------|
| `data/a_share.db` | 股票核心库 | `database.stock_path` | 股票信息、日线、财务 | `db/stock.py` |
| `data/etf.db` | ETF 行情库 | `database.etf_path` | ETF 信息、日线 | `db/etf.py` |
| `data/a_share_features.db` | 股票扩展域库 | `database.feature_path` | 概念、资金流、龙虎榜 | `db/feature.py` |
| `data/event_calendar.db` | 事件日历库 | `database.event_path` | 节假日、重大事件 | `db/event.py` |
| `data/index_constituents.db` | 指数成分变化库 | `database.index_constituent_path` | 指数成分变化 | `db/index_constituent.py` |
| `data/index_forecasts.db` | 指数预测库 | `database.index_forecast_path` | 指数调整预测 | `db/index_forecast.py` |

> `a_share_features.db` 与 `a_share.db` 共用 `code` 作为查询 key，但无外键约束，可独立使用。
>
> 理解方法：`a_share.db` 是“股票核心库”，放股票主表、日线、财务；`a_share_features.db` 是“股票扩展域库”，放围绕股票发生的市场行为特征，如概念归属、资金流向、龙虎榜。

---

## 各层职责

### `services/core.py` — 业务门面

`SqlToolService` 统一持有全部 6 个数据库实例，GUI / CLI / API 都通过它调用业务能力，不直接操作 db 对象。

主要能力分组：

| 方法前缀 | 对应数据库 |
|----------|-----------|
| `get_stocks / import_data / update_data` | `a_share.db` |
| `get_etfs / import_etf_data / update_etf_data` | `etf.db` |
| `get_concepts / sync_feature_concepts / sync_feature_moneyflow / sync_feature_moneyflow_all / sync_feature_for_stock / sync_feature_market_wide / sync_feature_top_list` | `a_share_features.db` |
| `get_holidays / get_events / seed_event_data` | `event_calendar.db` |
| `get_index_entities / get_index_changes / import_index_constituent_snapshot` | `index_constituents.db` |
| `get_index_forecasts / add_index_forecast / export_index_forecasts_*` | `index_forecasts.db` |

### `api/app.py` — HTTP 接口

FastAPI 应用工厂 `create_app()`，将 HTTP 请求转发给 `SqlToolService`。不直接访问 db 层（除 `service.db.get_stock_overview` 用于 404 判断）。

### `gui/desktop.py` — 桌面 GUI

`SqlToolGUI`，通过 `self.service` 调用所有业务能力。GUI 状态变量（`tk.StringVar` 等）均为页面私有，不跨页面共享。

页面结构：

| 页签 | 关联数据库 |
|------|-----------|
| 股票工作台 | `a_share.db` |
| ETF 工作台 | `etf.db` |
| 股票扩展域 | `a_share_features.db`（概念基表、单股扩展、全量资金流向、全市场龙虎榜） |
| 按库数据查看 | 所有库 |
| 接口检测 | 无（仅 Tushare 探测） |
| API 工具 | 无（进程管理） |
| 状态与帮助 | 所有库（统计） |

### `db/` — 数据库层

每个 db 类继承 `BaseDatabase`，独立管理自己的 SQLite 文件，提供 `_init_db()`、`get_connection()`、`get_stats()` 等标准接口。

### `sources/` — 数据源层

Tushare API 访问，仅在导入/更新时使用。`tushare.py`、`etf.py`、`feature.py` 各自独立，通过 `base_source.py` 共享初始化逻辑。

### `tools/` — 指数导入工具

不依赖 Tushare，独立可运行：

| 文件 | 功能 |
|------|------|
| `rebuild_index_csvs.py` | 从 PDF / XLSX 原始公告重建指数成分变化 CSV |
| `index_change_importer.py` | 读取 CSV，通过本地 API 批量导入 `index_constituents.db` |
| `import_broker_forecasts.py` | 读取 `resource_data/broker_forecasts`，将券商预测 CSV 批量导入 `index_forecasts.db` |
| `pdf_extract.py` | PDF 文本提取辅助 |

> `import_broker_forecasts.py` 的定位是**单独辅助工具**，类似 AI 事件提取工具；默认不并入主 GUI 工作流和主 CLI 命令树。

---

## 设计原则

1. GUI / CLI / API 都通过 `services/core.py` 进入业务能力，不直接访问 db。
2. 6 个数据库彼此独立，不引入跨库外键或隐式映射。
3. 指数成分变化以"调整事件"（changes）为主模型，不要求维护完整快照。
4. 新功能优先补到对应 db 模块，通过 service 方法暴露给上层。

---

## 推荐阅读顺序

1. `README.md`
2. `src/sql_tool/services/core.py`
3. `src/sql_tool/api/app.py`
4. `src/sql_tool/db/__init__.py` 及各 `db/*.py`
5. `src/sql_tool/gui/desktop.py`
6. `src/sql_tool/tools/`
7. `tests/`
