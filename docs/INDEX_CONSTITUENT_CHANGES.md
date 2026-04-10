# 指数成分变化数据库

本文档说明指数成分变化数据库（`data/index_constituents.db`）的结构定义、填充顺序和 API 接口，供手工录入或程序化写入使用。

## 数据库结构

数据分四层，层层关联：

### 1. `index_entities`（指数实体主表）

| 字段 | 类型 | 说明 |
|------|------|------|
| index_id | INTEGER PK | 自增主键 |
| index_code | TEXT | 中证指数代码（如 000300），可为空 |
| index_name | TEXT NN | 指数名称（如 沪深300） |
| etf_code | TEXT NN | 对应 ETF 代码（如 510300） |
| benchmark | TEXT | ETF 基准描述 |
| category | TEXT NN | 指数分类，默认 `宽基`（也可填 `行业`、`主题`） |
| source | TEXT NN | 数据来源，默认 `derived_from_etf` |

唯一约束：`(index_name, etf_code)`

### 2. `index_constituent_snapshots`（成分快照）

| 字段 | 类型 | 说明 |
|------|------|------|
| snapshot_id | INTEGER PK | 自增主键 |
| index_id | INTEGER FK | → index_entities |
| trade_date | TEXT NN | 快照日期 YYYYMMDD，即调整生效日 |
| effective_date | TEXT | 实际生效日（默认与 trade_date 相同） |
| announcement_date | TEXT | 公告日期（人工填写） |
| note | TEXT | 备注 |
| source | TEXT NN | 来源，默认 `manual_or_imported` |

唯一约束：`(index_id, trade_date)`

### 3. `index_constituent_items`（成分明细）

| 字段 | 类型 | 说明 |
|------|------|------|
| item_id | INTEGER PK | 自增主键 |
| snapshot_id | INTEGER FK | → index_constituent_snapshots |
| code | TEXT NN | 成分股代码 |
| name | TEXT | 成分股名称 |
| weight | REAL | 权重（%），无数据时可为空 |
| source | TEXT NN | 默认 `manual_or_imported` |

唯一约束：`(snapshot_id, code)`

### 4. `index_constituent_changes`（变化记录）

| 字段 | 类型 | 说明 |
|------|------|------|
| change_id | INTEGER PK | 自增主键 |
| index_id | INTEGER FK | → index_entities |
| trade_date | TEXT NN | 变化生效日期 |
| change_type | TEXT NN | `added`（调入）或 `removed`（调出） |
| code | TEXT NN | 股票代码 |
| name | TEXT | 股票名称 |
| from_snapshot_id | INTEGER | 变化前快照 ID |
| to_snapshot_id | INTEGER | 变化后快照 ID |
| note | TEXT | 备注 |

## 推荐填充顺序

### 方式一：手工通过 API 写入（推荐）

1. **建立指数实体**
```
POST /indexes/entities
{
  "index_name": "沪深300",
  "etf_code": "510300",
  "benchmark": "沪深300指数",
  "index_code": "000300",
  "category": "宽基"
}
# 返回 {"index_id": 1}
```

2. **导入某日期的成分快照**
```
POST /indexes/snapshots
{
  "index_id": 1,
  "trade_date": "20260401",
  "effective_date": "20260401",
  "announcement_date": "20260325",
  "note": "2026年4月调整",
  "items": [
    {"code": "000001", "name": "平安银行", "weight": 1.2},
    {"code": "600519", "name": "贵州茅台", "weight": 2.5}
  ]
}
# 返回 {"snapshot_id": 1, "rows": 2}
```

3. **导入另一快照（比较用）**
```
POST /indexes/snapshots
{
  "index_id": 1,
  "trade_date": "20260101",
  "items": [...]
}
```

4. **自动分析变化**
```
POST /indexes/1/analyze-changes
# 系统自动对比最新两次快照，生成 changes 记录
# 返回 {"index_id": 1, "changes": 5, "trade_date": "20260401"}
```

5. **或手工直接写入变化记录**
```
POST /indexes/changes
{
  "index_id": 1,
  "trade_date": "20260401",
  "changes": [
    {"change_type": "added",   "code": "300750", "name": "宁德时代"},
    {"change_type": "removed", "code": "000725", "name": "京东方A"}
  ]
}
```

### 方式二：批量从 ETF 侧派生实体

```
POST /indexes/derive
# 自动遍历 ETF 库，将指数型 ETF 提取为 index_entities
```

## 查询接口

```
GET /indexes                        # 列出所有指数实体
GET /indexes/{index_id}/changes     # 查询指定指数的变化记录
```

## 当前限制

- 成分明细（items）数据需人工整理后通过 API 写入，系统不自动从 Tushare 拉取
- 变化记录（changes）可由系统根据两次快照自动推断，也可手工直接写入
- `category` 字段用于后续按指数分类查询，目前建议填写：`宽基`、`行业`、`主题`

## 数据来源建议

- 中证指数公司官网：https://www.csindex.com.cn
- 基金公司 ETF 持仓公告（每季度或每次调整时公布）
- 交易所 ETF 信息页面（沪深交易所）
