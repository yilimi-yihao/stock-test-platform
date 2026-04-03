# 股票数据库管理工具

一个面向 A 股本地数据维护的桌面工具，集成 Tushare、SQLite、本地 HTTP API 与 tkinter GUI。当前版本支持数据导入、增量更新、接口能力检测、股票浏览，以及在 GUI 内直接启动/停止本地 API。

## 主要能力

- 导入前 N 只股票或全量 A 股数据
- 增量更新本地已有股票数据
- 检测当前 Tushare token 可访问的核心接口
- 浏览股票最近 20 条日线和最近 8 期财务数据
- 启动本地 FastAPI 服务并通过 `/docs` 查看接口文档
- 查看数据库路径、体积、表行数和日期范围

## 环境要求

- Python 3.11+ 推荐
- 可联网访问 Tushare
- 已安装 `pip`

## 安装

```bash
pip install -r requirements.txt
```

## 配置

编辑 `config/settings.json`，至少填入 Tushare token。`api` 段可省略，程序会自动回退默认值。

```json
{
  "tushare": {
    "token": "your_token_here",
    "api_url": "http://api.tushare.pro",
    "enabled": true,
    "sample_stock": "000001"
  },
  "database": {
    "path": "data/a_share.db"
  },
  "api": {
    "host": "127.0.0.1",
    "port": 8000,
    "enabled": true
  }
}
```

## 启动方式

### GUI

```bash
python main.py gui
```

### CLI

```bash
python main.py stats
python main.py detect
python main.py import --limit 100
python main.py import --all
python main.py update
python main.py clear
```

### 本地 API

```bash
python main.py api
python main.py api --host 127.0.0.1 --port 8011
```

默认地址：

- API：`http://127.0.0.1:8000`
- Swagger：`http://127.0.0.1:8000/docs`
- OpenAPI JSON：`http://127.0.0.1:8000/openapi.json`

## 推荐使用顺序

1. 运行 `python main.py detect` 检查 token 权限。
2. 在 GUI 的“数据管理”页先导入少量股票验证链路。
3. 确认数据库正常后再做全量导入或日常增量更新。
4. 需要给其他工程供数时，在 GUI 的“API 服务”页或 CLI 中启动本地 API。

## GUI 页面说明

- `数据管理`：导入、更新、清库、刷新统计
- `API 服务`：配置 host/port，启动/停止 API，检查状态，打开 `/docs`
- `数据库状态`：查看数据库文件、表行数、更新时间和日期范围
- `接口检测`：测试 token 对核心接口的可用性
- `数据浏览`：查看股票日线和财务摘要
- `帮助`：打开本地帮助页和 API 文档入口

## HTTP API 概览

- `GET /health`：健康检查
- `GET /stats`：数据库统计
- `GET /stocks`：股票列表
- `GET /stocks/{code}/daily`：股票日线
- `GET /stocks/{code}/financials`：股票财务聚合
- `GET /capabilities`：接口能力检测结果

详细接口说明见 `docs/API_GUIDE.md`。

## 数据库说明

数据库默认文件是 `data/a_share.db`。表结构、索引与字段说明见 `docs/DATABASE_FORMAT.md`。

## 测试

```bash
pytest
```

也可以只跑 API / 服务层测试：

```bash
pytest tests/test_api.py tests/test_service.py tests/test_database.py
```

## 项目结构

```text
sql-tool/
├── config/settings.json
├── data/a_share.db
├── docs/
│   ├── API_GUIDE.md
│   ├── DATABASE_FORMAT.md
│   └── help.html
├── src/sql_tool/
│   ├── api.py
│   ├── cli.py
│   ├── config.py
│   ├── database.py
│   ├── gui.py
│   ├── service.py
│   └── tushare_source.py
├── tests/
├── main.py
└── requirements.txt
```

## 注意事项

- 首次导入可能较慢，建议先小规模验证。
- 如果 `detect` 全部失败，优先检查 token 权限与网络。
- GUI 中“打开 API 文档”无响应时，通常是本地 API 尚未启动。
- 清库操作不可撤销。
