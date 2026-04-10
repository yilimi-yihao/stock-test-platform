# 特色数据本地化扩展实施交接文档

> 目的：给后续的 Claude / 开发者一份可直接执行的实施说明，在**不破坏现有核心数据库、不中断现有 GUI 导入流程**的前提下，为本项目增加“概念板块、资金流向、龙虎榜、热榜、筹码、机构调研、集合竞价、盈利预测、量化因子、券商金股、游资数据”等本地存储能力。
>
> 用户的真实目的不是做一份停留在沙箱里的验证稿，而是：**把当前项目扩展成一个可在本地长期沉淀特色数据的 A 股数据库工具**，并且让后续在新环境中的 Claude / 开发者能据此直接修改主工程完成落地。
>
> 本文档是**实施交接文档**，不是需求讨论稿。

---

## 1. 背景、目标与对后续实施者的关键说明

当前项目已经具备：

- 核心股票主表
- 日线行情
- 财务数据
- 同步状态记录
- GUI / CLI / 本地 API

当前核心数据库由 `src/sql_tool/db/stock.py` 实现，主流程由 `src/sql_tool/services/core.py` 驱动。

### 本次扩展目标

新增以下“特色数据”的本地存储能力：

1. 概念板块和成分
2. 资金流向
3. 券商金股
4. 筹码分布
5. 龙虎榜
6. 量化因子
7. 盈利预测
8. 机构调研
9. 集合竞价
10. 游资数据
11. 涨停榜单
12. 个股及行业热板

### 硬约束

后续实施时必须满足：

- **不能破坏现有 `data/a_share.db` 中的数据**
- **不能要求重建当前核心库**
- **不能默认把新同步逻辑直接并入当前 GUI 的“导入/更新”主流程**
- 如果用户当时正在运行 GUI 或导入任务，**不要打断、不要清库、不要执行会占用或迁移现网库的操作**

### 对后续实施者的关键说明

这次我为了避免影响用户当时正在运行的 GUI 和导入任务，采用了**隔离原型验证**方式来论证方案可行性；但这**不代表后续正式工作也应该只停留在 sandbox**。

后续真正实施时，应当：

- 直接修改主工程源码（`src/sql_tool/`、`tests/`、必要的 `docs/` 与配置）
- 在主工程中新增正式模块，例如 `src/sql_tool/db/feature.py`、`src/sql_tool/sources/feature.py`
- 在主工程测试体系里增加正式测试
- 将 sandbox 中的原型仅作为**设计参考和已验证证据**，而不是最终交付物

一句话：**sandbox 是为了这次安全验证；正式落地必须改主工程。**

---

## 2. 本文档内容的来源与验证方式

这份方案不是凭空设想出来的，而是基于以下几类证据整理得到。

### 2.1 已阅读和核对的现有工程实现

本次分析直接阅读了当前主工程中的关键文件：

- `src/sql_tool/database.py`
- `src/sql_tool/service.py`
- `src/sql_tool/tushare_source.py`
- `src/sql_tool/api.py`
- `src/sql_tool/gui.py`
- `src/sql_tool/config.py`
- `tests/test_database.py`
- `tests/test_service.py`
- `README.md`
- `docs/API_GUIDE.md`

据此确认了：

- 当前核心库是按数据域拆表，而不是单一宽表
- 当前主流程由 `SqlToolService` 驱动，适合新增独立特色服务层
- 当前 API 和 GUI 都依赖核心库，贸然把特色同步混入主流程风险较高
- 当前测试已经覆盖数据库层与服务层的基本风格，可沿用同样模式扩展

### 2.2 已读取并核对的当前 live 数据库现状

本次分析对当前 `data/a_share.db` 做了**只读检查**，确认了当前库已经承载较大体量数据，因此不应采用高风险改造方式。

当时看到的核心表规模如下：

- `stocks`: 3343 行
- `daily_prices`: 13,381,474 行
- `fina_indicator`: 172,468 行
- `income`: 249,317 行
- `balancesheet`: 200,152 行
- `cashflow`: 220,040 行
- `sync_status`: 23,401 行

并确认：

- 当前库使用 WAL 模式
- 当前核心数据已经具备实际规模，不能把它当成可以随便重建的测试库

这些现状直接影响了方案结论：**应优先保护核心库稳定性，并将特色数据独立落库。**

### 2.3 已做过的隔离原型验证

为避免影响正式代码，早期曾在 sandbox 中做过隔离原型验证；这些原型已完成历史使命。
当前主工程中已经具备正式实现：

- `src/sql_tool/feature_database.py`
- `src/sql_tool/feature_source.py`
- `tests/test_feature_etf_database.py`

因此后续不再依赖 sandbox 原型文件，而应直接阅读主工程正式实现。

### 2.4 已完成的验证内容

原型阶段实际验证过：

1. 特色库单独建表可行
2. 龙虎榜主表/明细表的父子结构可行
3. 外键级联删除可行
4. 每日型特色表支持刷新式写入
5. 市场级数据可以只存在特色库，而不污染核心库
6. 核心库和特色库可以通过 `ATTACH DATABASE` 做联查
7. 为了适配 SQLite 约束限制，某些“带表达式的唯一约束”必须改成“表 + 唯一索引”实现

### 2.5 已执行的测试结果

当前正式测试文件：

- `tests/test_feature_etf_database.py`

已覆盖：

- 建表测试
- 龙虎榜父子级联测试
- upsert/refresh 测试
- ETF 与特色库独立性测试
- 特色库与核心库分离存储测试

结果：请以当前主工程 `pytest` 为准。

### 2.6 这些验证意味着什么

这些验证说明：

- 当前推荐的双库架构不是空想，而是已经过最小可行性验证
- 但这些验证只是在避免打断用户现有工作的前提下，为正式实施提供证据
- **后续真正交付时，必须把方案落实到主工程源码中，而不是只保留 sandbox 原型**

---

## 3. 已验证结论

本仓库中已经做过一轮验证，当前应以主工程正式实现为准，关键文件如下：

- 正式 schema / DB 实现：`src/sql_tool/feature_database.py`
- 正式测试：`tests/test_feature_etf_database.py`

已验证通过：

1. 特色库单独建表可行
2. 父子事件表（龙虎榜主表/明细表）外键级联可行
3. 特色库与核心库可以分离存储
4. 可以通过 SQLite `ATTACH DATABASE` 做双库联查
5. 原型测试当前通过：**5 passed**

因此，推荐方案不是“扩核心单库”，而是：

## 结论：采用双库架构

- 核心库：`data/a_share.db`
- 特色库：`data/a_share_features.db`

---

## 4. 推荐总体架构

### 3.1 核心库保留不动

当前核心库继续只负责：

- `stocks`
- `daily_prices`
- `fina_indicator`
- `income`
- `balancesheet`
- `cashflow`
- `sync_status`

这些表、现有 GUI、现有 API、现有导入逻辑优先保持兼容。

### 3.2 新增特色库

新增数据库文件：

- `data/a_share_features.db`

该库专门负责“特色数据”。

### 3.3 双库联查策略

当需要做“单股特色画像”时：

- 主股票信息从 `a_share.db` 读取
- 特色数据从 `a_share_features.db` 读取
- 如需一条 SQL 联查，使用 SQLite：

```sql
ATTACH DATABASE 'data/a_share_features.db' AS feature_db;
```

然后进行：

- `stocks` ↔ `feature_db.stock_concepts`
- `stocks` ↔ `feature_db.stock_moneyflow_daily`
- `stocks` ↔ `feature_db.stock_hot_daily`
- `stocks` ↔ `feature_db.limit_list_events`

等联查。

### 3.4 为什么不建议全部塞进核心库

原因：

1. 这些特色数据并不全是“单股固定字段”
2. 很多是事件流、排行、明细、市场级/板块级数据
3. 直接并入核心库会增加现有写入压力
4. 当前 GUI 正在使用核心库，新增表和新同步逻辑混入主库风险更高
5. 双库更利于后续扩展、缓存、重建特色数据而不影响主库

---

## 4. 特色库表设计（建议作为正式落地版本）

下面是建议正式落地的表结构。字段已经过原型验证，可直接参考 `sandbox/feature_schema_prototype/schema_prototype.py` 实施。

---

## 4.1 概念板块

### `concepts`

概念/主题主表。

字段：

- `concept_id TEXT PRIMARY KEY`
- `concept_name TEXT NOT NULL`
- `source TEXT NOT NULL DEFAULT 'tushare'`
- `category TEXT NOT NULL DEFAULT 'concept'`
- `updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`

### `stock_concepts`

当前有效的股票-概念关系表。

字段：

- `code TEXT NOT NULL`
- `concept_id TEXT NOT NULL`
- `is_active INTEGER NOT NULL DEFAULT 1`
- `in_date TEXT`
- `out_date TEXT`
- `updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`

主键：

- `(code, concept_id)`

### `stock_concepts_daily`

按交易日记录概念成分历史快照。

字段：

- `trade_date TEXT NOT NULL`
- `code TEXT NOT NULL`
- `concept_id TEXT NOT NULL`
- `source TEXT NOT NULL DEFAULT 'tushare'`

主键：

- `(trade_date, code, concept_id)`

### 实施建议

- 第一阶段先落 `concepts + stock_concepts`
- `stock_concepts_daily` 先建表，历史回填可延后

---

## 4.2 资金流向

### `stock_moneyflow_daily`

字段：

- `code TEXT NOT NULL`
- `trade_date TEXT NOT NULL`
- `buy_sm_vol REAL`
- `buy_md_vol REAL`
- `buy_lg_vol REAL`
- `buy_elg_vol REAL`
- `sell_sm_vol REAL`
- `sell_md_vol REAL`
- `sell_lg_vol REAL`
- `sell_elg_vol REAL`
- `net_mf_vol REAL`
- `net_mf_amount REAL`
- `source TEXT NOT NULL DEFAULT 'tushare'`

主键：

- `(code, trade_date)`

---

## 4.3 龙虎榜

### `top_list_events`

主事件表。

字段：

- `event_id INTEGER PRIMARY KEY AUTOINCREMENT`
- `code TEXT NOT NULL`
- `trade_date TEXT NOT NULL`
- `reason TEXT NOT NULL`
- `close REAL`
- `pct_change REAL`
- `turnover_rate REAL`
- `amount REAL`
- `net_amount REAL`
- `source TEXT NOT NULL DEFAULT 'tushare'`

唯一约束：

- `(code, trade_date, reason)`

### `top_list_traders`

席位/营业部明细表。

字段：

- `trader_id INTEGER PRIMARY KEY AUTOINCREMENT`
- `event_id INTEGER NOT NULL`
- `broker_name TEXT NOT NULL`
- `direction TEXT NOT NULL CHECK(direction IN ('buy', 'sell'))`
- `rank_no INTEGER`
- `amount REAL`
- `net_amount REAL`
- `source TEXT NOT NULL DEFAULT 'tushare'`

外键：

- `event_id -> top_list_events(event_id) ON DELETE CASCADE`

### 说明

此父子关系已经过沙箱测试验证。

---

## 4.4 筹码分布

### `stock_chip_daily`

筹码摘要表。

字段：

- `code TEXT NOT NULL`
- `trade_date TEXT NOT NULL`
- `avg_cost REAL`
- `winner_rate REAL`
- `cost_5pct REAL`
- `cost_15pct REAL`
- `cost_50pct REAL`
- `cost_85pct REAL`
- `cost_95pct REAL`
- `concentration_70 REAL`
- `concentration_90 REAL`
- `source TEXT NOT NULL DEFAULT 'tushare'`

主键：

- `(code, trade_date)`

### `stock_chip_bins`

筹码价格分布明细表。

字段：

- `code TEXT NOT NULL`
- `trade_date TEXT NOT NULL`
- `price_level REAL NOT NULL`
- `chip_ratio REAL NOT NULL`
- `source TEXT NOT NULL DEFAULT 'tushare'`

主键：

- `(code, trade_date, price_level)`

### 实施建议

如果实际数据源只能拿到摘要，则只落 `stock_chip_daily`。

---

## 4.5 机构调研

### `institution_surveys`

主表。

字段：

- `survey_id INTEGER PRIMARY KEY AUTOINCREMENT`
- `code TEXT NOT NULL`
- `survey_date TEXT NOT NULL`
- `announcement_date TEXT`
- `title TEXT`
- `content_summary TEXT`
- `source TEXT NOT NULL DEFAULT 'tushare'`

唯一索引建议：

- `(code, survey_date, title)`

> 注意：不要在 SQLite 的 PRIMARY KEY / UNIQUE 约束里直接写表达式。原型里已经规避了这个坑，后续正式实现时也应继续使用“表 + 唯一索引”的方式。

### `institution_survey_participants`

参与机构明细表。

字段：

- `participant_id INTEGER PRIMARY KEY AUTOINCREMENT`
- `survey_id INTEGER NOT NULL`
- `institution_name TEXT NOT NULL`
- `institution_type TEXT`
- `source TEXT NOT NULL DEFAULT 'tushare'`

外键：

- `survey_id -> institution_surveys(survey_id) ON DELETE CASCADE`

---

## 4.6 集合竞价

### `stock_auction_daily`

字段：

- `code TEXT NOT NULL`
- `trade_date TEXT NOT NULL`
- `auction_price REAL`
- `auction_volume REAL`
- `auction_amount REAL`
- `matched_ratio REAL`
- `source TEXT NOT NULL DEFAULT 'tushare'`

主键：

- `(code, trade_date)`

---

## 4.7 个股热板 / 行业热板 / 热门板块

### `stock_hot_daily`

字段：

- `code TEXT NOT NULL`
- `trade_date TEXT NOT NULL`
- `heat_score REAL`
- `rank_no INTEGER`
- `board_type TEXT NOT NULL DEFAULT 'stock_hot'`
- `source TEXT NOT NULL DEFAULT 'tushare'`

主键：

- `(code, trade_date, board_type, source)`

### `sector_hot_daily`

字段：

- `sector_name TEXT NOT NULL`
- `sector_type TEXT NOT NULL`
  - 例如：`industry` / `concept` / `theme`
- `trade_date TEXT NOT NULL`
- `heat_score REAL`
- `rank_no INTEGER`
- `board_type TEXT NOT NULL DEFAULT 'sector_hot'`
- `source TEXT NOT NULL DEFAULT 'tushare'`

主键：

- `(sector_name, sector_type, trade_date, board_type, source)`

### 设计意图

统一热度与热榜数据结构，避免：

- 一张“行业热板表”
- 一张“概念热板表”
- 一张“个股热板表”
- 一张“专题热榜表”

无限拆表。

---

## 4.8 涨停榜单

### `limit_list_events`

字段：

- `event_id INTEGER PRIMARY KEY AUTOINCREMENT`
- `code TEXT NOT NULL`
- `trade_date TEXT NOT NULL`
- `board_type TEXT NOT NULL`
  - 例如：`limit_up` / `limit_down` / `炸板` / `连板`
- `status TEXT`
- `rank_no INTEGER`
- `close REAL`
- `pct_change REAL`
- `turnover_rate REAL`
- `first_limit_time TEXT`
- `last_limit_time TEXT`
- `open_count INTEGER`
- `reason TEXT`
- `source TEXT NOT NULL DEFAULT 'tushare'`

唯一约束：

- `(code, trade_date, board_type)`

---

## 4.9 券商金股

### `broker_picks`

字段：

- `code TEXT NOT NULL`
- `period TEXT NOT NULL`
- `broker_name TEXT NOT NULL`
- `pick_type TEXT`
- `score REAL`
- `source TEXT NOT NULL DEFAULT 'tushare'`

主键：

- `(code, period, broker_name)`

---

## 4.10 盈利预测

### `earnings_forecasts`

字段：

- `code TEXT NOT NULL`
- `report_date TEXT NOT NULL`
- `eps_consensus REAL`
- `np_consensus REAL`
- `inst_count INTEGER`
- `source TEXT NOT NULL DEFAULT 'tushare'`

主键：

- `(code, report_date)`

---

## 4.11 量化因子

### `alpha_factors_daily`

字段：

- `code TEXT NOT NULL`
- `trade_date TEXT NOT NULL`
- `factor_name TEXT NOT NULL`
- `factor_value REAL`
- `source TEXT NOT NULL DEFAULT 'tushare'`

主键：

- `(code, trade_date, factor_name)`

### 设计意图

采用**长表**而不是宽表。不要为每个新因子反复改表结构。

---

## 4.12 游资数据

### `roaming_capital_events`

字段：

- `event_id INTEGER PRIMARY KEY AUTOINCREMENT`
- `code TEXT NOT NULL`
- `trade_date TEXT NOT NULL`
- `trader_name TEXT NOT NULL`
- `action TEXT`
- `amount REAL`
- `note TEXT`
- `source TEXT NOT NULL DEFAULT 'tushare'`

唯一索引建议：

- `(code, trade_date, trader_name, action)`

---

## 4.13 特色同步日志

### `feature_sync_jobs`

字段：

- `dataset TEXT NOT NULL`
- `scope_type TEXT NOT NULL`
  - `stock`
  - `market`
  - `sector`
  - `concept`
- `scope_key TEXT NOT NULL`
- `status TEXT NOT NULL`
- `last_attempt_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`
- `last_success_at TIMESTAMP`
- `last_error TEXT`
- `row_count INTEGER NOT NULL DEFAULT 0`

主键：

- `(dataset, scope_type, scope_key)`

### 设计意图

它比当前核心库里的 `sync_status(code, dataset)` 更通用，适合特色库。

---

## 5. 索引建议

特色库至少应创建以下索引：

- `stock_concepts(concept_id, code)`
- `stock_concepts_daily(code, trade_date DESC)`
- `stock_moneyflow_daily(trade_date DESC, code)`
- `top_list_events(code, trade_date DESC)`
- `top_list_traders(event_id, direction, rank_no)`
- `stock_chip_daily(code, trade_date DESC)`
- `institution_surveys(code, survey_date DESC)`
- `stock_auction_daily(trade_date DESC, code)`
- `stock_hot_daily(trade_date DESC, rank_no)`
- `sector_hot_daily(trade_date DESC, sector_type, rank_no)`
- `limit_list_events(trade_date DESC, board_type, rank_no)`
- `broker_picks(period, broker_name)`
- `earnings_forecasts(report_date DESC, code)`
- `alpha_factors_daily(factor_name, trade_date DESC)`
- `roaming_capital_events(trade_date DESC, trader_name)`
- `feature_sync_jobs(dataset, status)`

参考原型：`sandbox/feature_schema_prototype/schema_prototype.py`

---

## 6. 建议文件改造点（未来正式实施时）

下面是未来实际编码时推荐修改的文件。

---

## 6.1 `src/sql_tool/config.py`

新增配置项支持：

```json
{
  "database": {
    "path": "data/a_share.db",
    "features_path": "data/a_share_features.db"
  }
}
```

建议新增方法：

- `get_features_db_path()`

注意：

- 不要改变现有 `get_db_path()` 行为
- 保持向后兼容

---

## 6.2 新增文件：`src/sql_tool/feature_database.py`

建议新建，而不是继续把所有表都塞进 `database.py`。

职责：

- 初始化特色库
- 创建所有特色表和索引
- 提供插入方法
- 提供查询方法
- 提供双库联查的帮助函数（可选）

建议方法：

- `get_connection()`
- `_init_db()`
- `insert_concepts(...)`
- `insert_stock_concepts(...)`
- `insert_moneyflow(...)`
- `insert_top_list_events(...)`
- `insert_top_list_traders(...)`
- `insert_chip_daily(...)`
- `insert_chip_bins(...)`
- `insert_institution_surveys(...)`
- `insert_institution_participants(...)`
- `insert_auction_daily(...)`
- `insert_stock_hot_daily(...)`
- `insert_sector_hot_daily(...)`
- `insert_limit_list(...)`
- `insert_broker_picks(...)`
- `insert_earnings_forecasts(...)`
- `insert_alpha_factors(...)`
- `insert_roaming_capital_events(...)`
- `set_sync_job_status(...)`
- `list_sync_job_status(...)`

---

## 6.3 新增文件：`src/sql_tool/feature_source.py`

建议新建，不要把所有特色接口继续堆进 `tushare_source.py`。

职责：

- 专门负责特色数据源封装
- 保持与 `TushareSource` 类似风格

建议方法：

- `get_concepts()`
- `get_concept_members(concept_id | trade_date)`
- `get_moneyflow(code, start_date, end_date)`
- `get_top_list(code | trade_date)`
- `get_top_list_traders(...)`
- `get_chip_distribution(...)`
- `get_institution_surveys(...)`
- `get_auction_data(...)`
- `get_stock_hot(...)`
- `get_sector_hot(...)`
- `get_limit_list(...)`
- `get_broker_picks(...)`
- `get_earnings_forecasts(...)`
- `get_alpha_factors(...)`
- `get_roaming_capital(...)`
- `detect_feature_capabilities(...)`

说明：

- 特色接口权限不一定都有
- 因此必须提供独立 capability 检测
- 不要假设 token 一定可用

---

## 6.4 新增文件：`src/sql_tool/feature_service.py`

建议新建一个独立服务层，而不是把特色同步直接并到现有 `SqlToolService`。

职责：

- 编排特色数据同步
- 写入 `FeatureDatabase`
- 维护 `feature_sync_jobs`
- 提供特色查询聚合接口

建议对外方法：

- `sync_concepts()`
- `sync_moneyflow(code=None, start_date=None, end_date=None)`
- `sync_top_list(code=None, trade_date=None)`
- `sync_stock_hot(...)`
- `sync_sector_hot(...)`
- `sync_limit_list(...)`
- `sync_chip(...)`
- `sync_institution_surveys(...)`
- `sync_auction(...)`
- `sync_broker_picks(...)`
- `sync_earnings_forecasts(...)`
- `sync_alpha_factors(...)`
- `sync_roaming_capital(...)`
- `get_stock_feature_profile(code)`

---

## 6.5 `src/sql_tool/service.py`

### 第一阶段建议

**尽量不改主同步流程。**

只允许：

- 在 `SqlToolService.__init__` 中可选挂载 `FeatureDatabase` / `FeatureService`
- 新增只读聚合方法，例如：
  - `get_stock_feature_profile(code)`

### 不建议第一阶段做的事

- 不要把所有特色数据同步塞进 `import_data()`
- 不要把所有特色数据同步塞进 `update_data()`
- 不要修改当前 `SYNC_DATASETS` 主列表去混合核心数据与特色数据

---

## 6.6 `src/sql_tool/api.py`

建议第二阶段新增接口：

### 单股画像

- `GET /stocks/{code}/features`

返回：

- 基础信息
- 概念板块
- 最新资金流
- 最近龙虎榜
- 热度
- 最近涨停榜信息
- 最近机构调研
- 近几日集合竞价

### 专题查询

- `GET /concepts`
- `GET /concepts/{concept_id}/stocks`
- `GET /top-list?trade_date=...`
- `GET /moneyflow/top?trade_date=...`
- `GET /sector-hot?trade_date=...`
- `GET /limit-list?trade_date=...&board_type=limit_up`

### 同步接口（可选）

- `POST /features/sync/concepts`
- `POST /features/sync/moneyflow`
- `POST /features/sync/top-list`
- `POST /features/sync/sector-hot`

第一阶段如果只做本地功能，可暂时不暴露 API 写接口。

---

## 6.7 `src/sql_tool/gui.py`

### 第一阶段建议

先不要动当前“工作台”导入按钮逻辑。

### 第二阶段建议

新增一个独立标签页，例如：

- `特色数据`

其中提供：

- 概念板块同步
- 资金流向同步
- 龙虎榜同步
- 热板同步
- 涨停榜单同步
- 机构调研同步
- 集合竞价同步
- 特征概览查询

### 注意

如果用户正在运行 GUI：

- 不要默认触碰当前正在执行的导入线程
- 不要自动迁移 live DB
- 不要自动清库

---

## 6.8 CLI

建议后续在 `src/sql_tool/cli.py` 中新增独立命令，而不是复用 `import/update`：

建议命令：

- `python main.py feature-sync concepts`
- `python main.py feature-sync moneyflow --code 000001`
- `python main.py feature-sync top-list --date 20260403`
- `python main.py feature-sync sector-hot --date 20260403`
- `python main.py feature-profile 000001`

---

## 7. 数据集命名规范

未来如果需要 capability 检测、日志、任务编排，统一使用以下 dataset 名称：

- `concepts`
- `concept_members`
- `moneyflow`
- `top_list`
- `top_list_traders`
- `chip`
- `chip_bins`
- `institution_surveys`
- `institution_participants`
- `auction`
- `stock_hot`
- `sector_hot`
- `limit_list`
- `broker_picks`
- `earnings_forecasts`
- `alpha_factors`
- `roaming_capital`

不要同时混用多套近义名字。

---

## 8. 推荐实施顺序

按风险和收益排序，建议分三期。

---

## 第一期：最稳、最有价值

优先落地：

1. `FeatureDatabase`
2. `FeatureSource`
3. `FeatureService`
4. 特色库初始化
5. 概念板块与成分
6. 资金流向
7. 龙虎榜
8. 基础测试

### 第一阶段目标

实现后即可支持：

- 单股画像的核心特色信息
- 概念筛股
- 资金流向分析
- 龙虎榜事件查询

---

## 第二期：增强热点与事件

新增：

1. 个股热板 / 行业热板
2. 涨停榜单
3. 机构调研
4. 集合竞价
5. 对应查询 API / GUI 页面

---

## 第三期：复杂特征

新增：

1. 筹码分布
2. 盈利预测
3. 量化因子
4. 券商金股
5. 游资数据

---

## 9. 历史回填建议

不要第一版就对所有数据做多年全量回填。

推荐：

- 概念关系：可全量
- 资金流向：先近 60~120 天
- 龙虎榜：先近 1 年
- 热榜：先近 30~90 天
- 涨停榜：先近 90~180 天
- 机构调研：先近 1~2 年
- 集合竞价：先近 30~90 天
- 筹码分布：先近 30~90 天
- 盈利预测：先近 1~2 年
- 量化因子：先近 60 天
- 券商金股：先近 1~2 年
- 游资数据：按实际接口能力决定

原因：

- 避免 SQLite 体积激增
- 避免首轮同步时间过长
- 避免接口限流 / 权限问题放大

---

## 10. 数据写入策略

建议统一使用：

- `INSERT OR REPLACE`
- 或 `INSERT ... ON CONFLICT DO UPDATE`

其中：

- 简单每日表可以使用 `INSERT OR REPLACE`
- 事件表/明细表更建议明确 `ON CONFLICT DO UPDATE`

注意：

- 不要对核心库里的 `stocks` 表继续依赖“删后插”的危险语义扩散到特色库
- 如果后续回头优化核心库，建议将 `insert_stock()` 从 `INSERT OR REPLACE` 改成 `ON CONFLICT(code) DO UPDATE`
- 这不是本次特色库落地的前置条件，但属于后续可改进项

---

## 11. 测试要求

未来正式实施时，至少应补以下测试。

### 数据库层测试

建议新增：

- `tests/test_feature_database.py`

覆盖：

- 建表成功
- 关键索引存在
- 主键/唯一键生效
- 外键级联删除
- 插入/更新行为
- `feature_sync_jobs` 读写

### 数据源层测试

建议新增：

- `tests/test_feature_source.py`

覆盖：

- 数据格式转换
- 空返回处理
- 权限不足处理
- 字段映射稳定性

### 服务层测试

建议新增：

- `tests/test_feature_service.py`

覆盖：

- 独立 dataset 同步
- 单股同步
- 市场级同步
- 同步失败后的状态记录
- 部分接口不可用时的降级行为

### API 层测试

建议新增：

- `tests/test_feature_api.py`

覆盖：

- `/stocks/{code}/features`
- `/concepts`
- `/top-list`
- `/sector-hot`
- `/limit-list`

### 保留原型测试

沙箱原型测试可继续保留，作为 schema 设计参考：

- `sandbox/feature_schema_prototype/test_feature_schema.py`

---

## 12. 实施时的安全约束

未来 Claude / 开发者实施时，请遵守：

1. **不要停止用户正在运行的 GUI**
2. **不要删除或清空 `data/a_share.db`**
3. **不要在未经确认的情况下对 live DB 做迁移/覆盖**
4. **先实现新库和新功能，再考虑 GUI 集成**
5. **优先在独立测试库和临时目录验证**
6. **如果需要对特色库做破坏性试验，只针对 `a_share_features.db` 的临时副本进行**

---

## 13. 最小可交付版本（MVP）定义

如果后续要先做一个最小可交付版本，建议以以下范围为准：

### 必做

- 新增 `FeatureDatabase`
- 新增 `FeatureSource`
- 新增 `FeatureService`
- 初始化 `data/a_share_features.db`
- 落地 3 类数据：
  - 概念板块/成分
  - 资金流向
  - 龙虎榜
- 新增单元测试
- 新增一个只读接口：`GET /stocks/{code}/features`

### 可暂缓

- GUI 入口
- 全部特色数据一次性接入
- 大规模历史回填
- 所有专题接口

---

## 14. 参考文件

未来实现前，优先阅读：

### 当前主工程

- `src/sql_tool/database.py`
- `src/sql_tool/service.py`
- `src/sql_tool/tushare_source.py`
- `src/sql_tool/api.py`
- `src/sql_tool/gui.py`
- `tests/test_database.py`
- `tests/test_service.py`

### 已验证原型

早期 sandbox 原型已由主工程正式实现替代，当前建议阅读：

- `src/sql_tool/feature_database.py`
- `src/sql_tool/feature_source.py`
- `tests/test_feature_etf_database.py`

---

## 15. 未来实施时的建议执行顺序（给 Claude 的简明指令）

如果后续由新的 Claude 会话来实施，建议按下面顺序做：

1. 阅读当前核心数据库和服务层实现
2. 阅读本交接文档
3. 阅读 `src/sql_tool/feature_database.py`
4. 先完善正式特色数据库实现
5. 先仅实现特色库初始化和数据库层测试
6. 再实现或补强 `FeatureSource`
7. 再实现 `FeatureService` / `SqlToolService` 对应门面
8. 第一期只接入：`concepts / moneyflow / top_list`
9. 新增只读聚合接口 `/stocks/{code}/features`
10. 跑测试
11. 在确认不影响现有 GUI/导入后，再继续接入第二期和第三期数据集

---

## 16. 最后结论

本项目的特色数据扩展，推荐采用“核心库 + 扩展域”的正式实现方案：

- 核心库保留稳定
- 特色库独立演进
- 通过 ATTACH 或服务层聚合完成联查
- 先做数据库层与服务层，再做 GUI 集成

这是当前风险最低、扩展性最好、最不容易打断现有使用方式的方案。
