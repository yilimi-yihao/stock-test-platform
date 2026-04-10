# API 使用指南

## 工具定位

sql-tool 是一个**本地数据库管理 + HTTP API 输出**工具。

- **写入**：GUI / CLI 导入、更新 Tushare 数据；工具脚本导入指数成分变化数据
- **读取**：本地 HTTP API 供外部脚本调用（回测、分析）
- **GUI**：负责管理与监控；**外部程序**负责分析消费

## 启动方式

```bash
python main.py api                        # 默认 127.0.0.1:8000
python main.py api --host 127.0.0.1 --port 8011
```

GUI 内"API 工具"页可直接启动/停止 API。

---

## 接口总览

| 方法 | 路径 | 说明 |
|------|------|------|
| GET  | `/health` | 健康检查 |
| GET  | `/stats` | 多数据库统计 |
| GET  | `/stocks` | 股票列表 |
| GET  | `/stocks/page` | 股票分页列表（推荐） |
| GET  | `/stocks/{code}/daily` | 单股日线 |
| GET  | `/stocks/{code}/financials` | 单股财务聚合 |
| GET  | `/stocks/{code}/features` | 单股扩展域（概念+资金流+龙虎榜） |
| GET  | `/etfs` | ETF 列表 |
| GET  | `/etfs/page` | ETF 分页列表 |
| GET  | `/etfs/{code}/daily` | ETF 日线 |
| GET  | `/concepts` | 概念列表 |
| GET  | `/concepts/{concept_id}/stocks` | 概念成分股 |
| GET  | `/feature/capabilities` | 扩展接口能力 |
| GET  | `/events/holidays` | 节假日日历 |
| GET  | `/events/major` | 重大事件 |
| GET  | `/indexes` | 指数实体列表 |
| GET  | `/indexes/{index_id}/changes` | 指数成分变化 |
| GET  | `/index-forecasts` | 指数变化预测列表 |
| GET  | `/capabilities` | Tushare 接口能力检测 |
| POST | `/daily/batch` | **批量日线**（回测/多股价格面板） |
| POST | `/stocks/overview/batch` | **批量概览**（横截面分析） |
| POST | `/stocks/{code}/update` | 单股增量更新 |
| POST | `/stocks/update` | 单股增量更新（body 传 code） |
| POST | `/etfs/{code}/update` | 单 ETF 增量更新 |
| POST | `/etfs/update` | 单 ETF 增量更新（body 传 code） |
| POST | `/features/sync/concepts` | 同步概念基表（概念列表 + 所有概念成分） |
| POST | `/features/sync/moneyflow` | 同步单股资金流向 |
| POST | `/features/sync/moneyflow/all` | **批量**同步全部已入库股票资金流向 |
| POST | `/features/sync/top-list` | 同步龙虎榜 |
| POST | `/events/seed` | 初始化事件数据 |
| POST | `/indexes/entities` | 注册指数实体 |
| POST | `/indexes/changes` | 写入指数成分变化 |
| POST | `/indexes/{index_id}/analyze-changes` | 从快照分析变化 |
| POST | `/indexes/derive` | 从 ETF 提取指数实体 |
| POST | `/index-forecasts` | 新增预测记录 |
| POST | `/index-forecasts/export` | 导出预测（CSV/Excel/PDF） |

---

## 常用场景示例

### 分页股票列表

```bash
GET /stocks/page?page=1&page_size=200
GET /stocks/page?page=1&page_size=200&industry=银行&order_by=code
```

返回：

```json
{
  "items": [...],
  "pagination": {"page": 1, "page_size": 200, "total": 5200, "pages": 26}
}
```

### 批量日线（回测 / 多股价格面板）

```bash
POST /daily/batch
Content-Type: application/json

{
  "codes": ["000001", "002594", "600519"],
  "start_date": "20240101",
  "end_date": "20241231"
}
```

返回 long-format，可直接 `pd.DataFrame(resp["items"])`。

### 指数成分变化

```bash
# 查询指数列表（找到 index_id）
GET /indexes

# 查询某指数的所有调入/调出记录
GET /indexes/195/changes
```

返回：

```json
{
  "items": [
    {
      "change_id": 1,
      "index_id": 195,
      "trade_date": "20251212",
      "announcement_date": "20251128",
      "change_type": "added",
      "code": "000403",
      "name": "派林生物"
    }
  ]
}
```

### 写入指数变化（通过工具脚本，不推荐直接调 API）

```bash
# 推荐方式：用 importer 脚本批量导入 CSV
PYTHONPATH=src python -m sql_tool.tools.index_change_importer --dry-run
PYTHONPATH=src python -m sql_tool.tools.index_change_importer
```

### Python 调用示例

```python
import requests

BASE = "http://127.0.0.1:8000"

# 分页股票列表
resp = requests.get(f"{BASE}/stocks/page", params={"page": 1, "page_size": 100})
stocks = resp.json()["items"]

# 批量日线
resp = requests.post(f"{BASE}/daily/batch", json={
    "codes": ["000001", "002594"],
    "start_date": "20240101",
    "end_date": "20241231"
})
import pandas as pd
df = pd.DataFrame(resp.json()["items"])

# 指数成分变化
resp = requests.get(f"{BASE}/indexes")
indexes = {e["index_name"]: e["index_id"] for e in resp.json()["items"]}

resp = requests.get(f"{BASE}/indexes/{indexes['中证1000']}/changes")
changes = resp.json()["items"]
```

---

## 扩展域（特色数据）说明

### 概念基表与股票成员关系

`concepts` 和股票是多对多关系，通过 `stock_concepts` 关联：

```
concepts (concept_id, concept_name)
    ↕ 多对多
stock_concepts (code, concept_id, in_date, out_date, is_active)
```

点击「同步概念基表」时：
1. 写入 `concepts` 全量概念列表
2. 逐概念调用 Tushare `concept_detail`，写入 `stock_concepts`（该概念下的全部成员股）

查询接口：
- `GET /concepts` — 概念列表
- `GET /concepts/{concept_id}/stocks` — 某概念下的全部成员股

**维护建议**：每周或月度重新执行一次「同步概念基表」。`replace_stock_concepts` 采用先删后插语义，每次同步都是当前最新快照，能正确反映成员退出（`out_date` 不为空）的情况。

### 三个扩展域同步按钮的实际范围

| 按钮 | 对应 API | 实际同步内容 |
|------|---------|------------|
| 同步概念基表 | `POST /features/sync/concepts` | 概念列表 + 所有概念成员股（耗时长） |
| 同步单股扩展 | — (直接调 service) | 指定股票的资金流向 + 其概念成分 |
| 全量资金流向 | `POST /features/sync/moneyflow/all` | 对已入库所有股票批量同步资金流向（耗时长） |
| 同步全市场龙虎榜 | `POST /features/sync/top-list` | 龙虎榜事件（非资金流向） |

### 资金流向 vs 财务概览

这两者不是一回事：

| 数据 | 所属库 | 时间粒度 | 反映内容 |
|------|--------|---------|---------|
| 资金流向 | `a_share_features.db`（股票扩展域库） | 交易日 | 市场行为特征：大单/主力买卖与净流入 |
| 财务概览 | `a_share.db`（股票核心库） | 季度/报告期 | 公司基本面：营收、利润、资产负债、财务指标 |

所以单股扩展里的“资金流向”不是财报数据，而是围绕股票发生的市场交易特征。

### 关于“200 条上限”的说明

- 同步/增量/全量任务的目标应当是对应实体全集。
- “最近 200 行”是**浏览页/列表接口的显示限制**，用于提升 GUI 与 API 浏览性能，不代表同步逻辑只处理 200 个实体。
- 若某处批量同步意外只跑了 200 个实体，应视为实现问题，而不是产品设计目标。

### 单股扩展日志怎么理解

单股扩展日志分为父任务与子任务：
- 父任务：`同步单股扩展`
- 子任务 1：资金流向
- 子任务 2：概念归属

因此日志里连续出现“开始同步单股扩展”与“开始同步资金流向”并不代表资金流向被重复更新，而是主任务进入了第一个子步骤。

### 事件库板块映射写入保护

`POST /events/holidays` 和 `POST /events/major` 写入时，若 `mappings` 为空列表，**不会覆盖已有板块映射**（保留现有数据）。若明确要清空映射，需调用对应 DELETE 端点后再重新写入。

---

## 常见问题

**接口检测全失败**：token 无效、权限不足或网络不可达。

**批量日线返回 0 条**：codes 格式须为纯 6 位数字；日期格式 `YYYYMMDD`，不含连字符。

**打开 /docs 无响应**：先访问 `/health` 确认 API 已启动。

**指数成分变化导入后查不到**：确认导入时 API 正在运行；用 `GET /indexes` 确认指数实体已创建。
