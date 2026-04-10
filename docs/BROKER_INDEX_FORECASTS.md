# 券商指数成分预测变化（框架）

该文件用于维护**各券商对指数成分股潜在调入调出的预测**。

## 定位
- 这类数据通常来自研报、策略周报、量化报告
- 免费公开数据较零散，先用文档归档最稳妥
- 当前项目已提供**独立辅助导入工具**，用于把 `resource_data/broker_forecasts/` 的 CSV 批量写入 `index_forecasts.db`
- 它的定位和 AI 事件提取工具类似：**单独运行的辅助工具**，不并入主 GUI 工作流，也不接入主 CLI 命令树

## 建议字段
- 日期
- 券商
- 指数
- 预测调入
- 预测调出
- 依据
- 来源链接/文件

## 初始样例

| 指数名称 | 月份 | 预测调入 | 预测调出 | 预测源 | 备注 |
|---|---|---|---|---|---|
| 沪深300 | 2026年04月 | 宁德时代(300750) | 京东方A(000725) | 中信证券 | 样例数据，后续以研报更新 |
| 中证500 | 2026年05月 | 招商银行(600036) | 中国中铁(601390) | 华泰证券 | 样例数据 |
| 中证1000 | 2026年06月 | 优博讯(300531) | 云鼎科技(000409) | 国泰君安 | 样例数据 |

## 正式导入方式

项目已提供正式导入工具，可将 `resource_data/broker_forecasts/` 下的 CSV 批量写入 `data/index_forecasts.db`。

> 该工具定位为**单独辅助工具**，类似 `sandbox/event_ai_tool/` 的独立辅助定位；默认不接入主 GUI 按钮，也不并入 `main.py` 的主命令树。

```bash
PYTHONPATH=src python -m sql_tool.tools.import_broker_forecasts --dry-run
PYTHONPATH=src python -m sql_tool.tools.import_broker_forecasts
```

可选参数：

```bash
PYTHONPATH=src python -m sql_tool.tools.import_broker_forecasts --only-broker 华泰证券
PYTHONPATH=src python -m sql_tool.tools.import_broker_forecasts --only-index 沪深300
```

### 导入规则

- 目录名 = `broker_name`
- 文件名（去 `.csv`）= `index_name`
- 文件内 `调入 / 调出` 分段分别映射为：
  - `预测调入`
  - `预测调出`
- 通过股票核心库按**证券名称精确匹配**股票代码
- 未匹配到股票代码的记录会在 summary 中列为 `unmatched_names`，不会强行写入数据库
- 重复导入是幂等的，不会无限增行

