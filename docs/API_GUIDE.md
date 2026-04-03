# API 使用指南

## 启动方式

### CLI 启动

```bash
python main.py api
```

### 自定义地址

```bash
python main.py api --host 127.0.0.1 --port 8011
```

### GUI 启动

也可以在 GUI 的“API 服务”页中直接启动、停止并查看 API 状态。

## 默认地址

- API 根地址：`http://127.0.0.1:8000`
- Swagger 文档：`http://127.0.0.1:8000/docs`
- OpenAPI JSON：`http://127.0.0.1:8000/openapi.json`

## 接口列表

### `GET /health`

健康检查。

返回示例：

```json
{
  "status": "ok"
}
```

### `GET /stats`

返回数据库统计信息。

主要字段：

- `db_path`
- `db_exists`
- `db_size_bytes`
- `stock_count`
- `price_count`
- `table_counts`
- `latest_stock_update`
- `date_range`

### `GET /stocks`

查询股票列表。

参数：

- `limit`：返回数量上限，默认 200
- `search`：按代码、名称、行业模糊搜索

示例：

```bash
curl "http://127.0.0.1:8000/stocks?limit=20&search=银行"
```

### `GET /stocks/{code}/daily`

查询某只股票的日线数据。

参数：

- `start_date`：开始日期，可选，格式 `YYYY-MM-DD`
- `end_date`：结束日期，可选，格式 `YYYY-MM-DD`
- `limit`：返回条数，默认 60

示例：

```bash
curl "http://127.0.0.1:8000/stocks/000001/daily?limit=20"
```

### `GET /stocks/{code}/financials`

查询某只股票的财务聚合数据。

参数：

- `limit`：每类财务数据返回期数，默认 8

返回内容包含：

- `stock`
- `financials.income`
- `financials.fina_indicator`
- `financials.balancesheet`
- `financials.cashflow`

### `GET /capabilities`

检测当前 Tushare 账号可访问哪些接口。

参数：

- `sample_code`：测试用样本股票代码，可选，默认读取配置中的 `tushare.sample_stock`

返回内容包含：

- `sample_code`
- `available_count`
- `total_count`
- `results`

每条结果包含：

- `api_name`
- `display_name`
- `available`
- `empty`
- `rows`
- `error`

## Python 调用

```python
from sql_tool.service import SqlToolService

service = SqlToolService()

stats = service.get_stats()
stocks = service.get_stocks(limit=20)
daily = service.get_stock_daily('000001', limit=20)
financials = service.get_stock_financials('000001', limit=8)
capabilities = service.detect_capabilities()
```

## 常见问题

### 打开 `/docs` 没有响应

先确认 API 已启动；可直接访问 `/health` 检查是否返回 `{"status": "ok"}`。

### GUI 能否管理 API

可以。当前 GUI 已内置启动、停止、状态检查和打开文档入口。
