# 股票数据库管理工具

A 股与 ETF 本地数据维护桌面工具，集成 Tushare、SQLite、本地 HTTP API 与 tkinter GUI。支持股票核心库、ETF 库、股票扩展域、事件库、指数成分变化库与指数预测库的统一管理。

## 主要能力

- 导入 / 更新股票日线与财务数据
- 导入 / 更新 ETF 日线数据
- 同步股票扩展域（概念、资金流、龙虎榜）
- 管理节假日与重大事件库
- 维护指数成分变化库（支持 CSV 批量导入，PDF/XLSX 解析）
- 维护指数调整预测库，并支持 CSV / Excel / PDF 导出
- 检测 Tushare 接口能力
- 启动本地 FastAPI 服务，通过 `/docs` 查看接口文档
- 查看各库状态与按表浏览数据

## 数据库文件

| 文件 | 展示名 | 职责 | 与其他库的关系 |
|------|--------|------|----------------|
| `data/a_share.db` | 股票核心库 | 股票信息、日线、财务 | 独立，内部 code 主键 |
| `data/etf.db` | ETF 行情库 | ETF 信息、日线 | 独立 |
| `data/a_share_features.db` | 股票扩展域库 | 概念、资金流、龙虎榜 | 独立，code 是公共查询 key，不是外键 |
| `data/event_calendar.db` | 事件日历库 | 节假日、重大赛事/事件 | 独立 |
| `data/index_constituents.db` | 指数成分变化库 | 指数成分变动记录 | 独立，不依赖股票库或 ETF 库 |
| `data/index_forecasts.db` | 指数预测库 | 指数调整预测 | 独立 |

六个数据库彼此无外键依赖，可以独立使用。

### 如何理解“核心库”与“扩展域库”

- **股票核心库**（`a_share.db`）存的是股票研究最基础的事实数据：股票主表、日线、财务报表、财务指标。
- **股票扩展域库**（`a_share_features.db`）存的是围绕股票发生的市场行为特征：概念归属、资金流向、龙虎榜等。

所以：
- **财务概览** 属于核心库，是公司基本面。
- **资金流向** 属于扩展域库，是市场行为特征，不是财务报表。

### 概念与股票群的关系

概念和股票是多对多关系：
- `concepts`：概念列表
- `stock_concepts`：概念与股票成员关系

“同步概念基表”做的是：
1. 刷新全部概念列表；
2. 再逐个概念刷新该概念下的成员股快照。

因此它不是同步某一只股票，而是同步“概念 -> 股票群”的全量映射。

## 环境要求

- Python 3.11+
- 可联网访问 Tushare（仅导入/更新时需要）

## 安装

```bash
pip install -r requirements.txt
```

## 配置

编辑 `config/settings.json`，至少填入 Tushare token：

```json
{
  "tushare": {
    "token": "your_token_here",
    "api_url": "http://api.tushare.pro",
    "sample_stock": "000001",
    "sample_etf": "510300",
    "request_interval_seconds": 0.25
  },
  "database": {
    "stock_path": "data/a_share.db",
    "etf_path": "data/etf.db",
    "feature_path": "data/a_share_features.db",
    "event_path": "data/event_calendar.db",
    "index_constituent_path": "data/index_constituents.db",
    "index_forecast_path": "data/index_forecasts.db"
  },
  "api": {
    "host": "127.0.0.1",
    "port": 8000
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

默认地址：`http://127.0.0.1:8000`，Swagger：`http://127.0.0.1:8000/docs`

## 推荐使用顺序

1. 运行 `python main.py detect` 检查 token 权限
2. GUI 的"股票工作台"页先导入少量股票验证链路
3. 确认数据库正常后再做全量导入或日常增量更新
4. 指数成分变化数据：用 `src/sql_tool/tools/index_change_importer.py` 从 CSV 导入，无需 Tushare token
5. 需要给外部程序供数时，在"API 工具"页或 CLI 启动本地 API

## GUI 页面说明

| 页面 | 功能 |
|------|------|
| 股票工作台 | 导入、更新、清库、刷新统计 |
| ETF 工作台 | 导入、更新 ETF |
| 股票扩展域 | 同步概念、资金流、龙虎榜 |
| 按库数据查看 | 按库/表浏览数据 |
| 接口检测 | 统一检测股票 / ETF / 扩展接口能力 |
| API 工具 | 配置 host/port，启动/停止 API，打开 `/docs` |
| 状态与帮助 | 查看各库状态、初始化事件数据、帮助文档 |

## 指数成分变化导入

指数成分变化数据不依赖 Tushare，通过本地 CSV 批量导入：

```bash
# dry-run 验证
PYTHONPATH=src python -m sql_tool.tools.index_change_importer --dry-run

# 正式导入（需先启动 API）
python main.py api &
PYTHONPATH=src python -m sql_tool.tools.index_change_importer
```

CSV 格式见 `resource_data/index_change_csvdata/`，重建脚本见 `src/sql_tool/tools/rebuild_index_csvs.py`。

## 测试

```bash
pytest
```

## 项目结构

详细说明见 `docs/PROJECT_STRUCTURE.md`。

```text
sql-tool/
├── config/settings.json
├── data/                        # 6 个独立 SQLite 数据库
├── docs/                        # API、数据库、结构文档
├── resource_data/
│   └── index_change_csvdata/    # 指数成分变化源 CSV
├── src/sql_tool/
│   ├── api/                     # FastAPI 应用
│   ├── gui/                     # tkinter GUI
│   ├── services/                # 高层服务门面
│   ├── db/                      # 6 个数据库实现
│   ├── sources/                 # Tushare 数据源
│   ├── tools/                   # 指数 CSV 导入/重建工具
│   ├── exporters_pkg/           # CSV/Excel/PDF 导出
│   ├── config.py
│   ├── base_database.py
│   └── base_source.py
├── tests/
├── main.py
└── requirements.txt
```

## 注意事项

- 首次导入可能较慢，建议先小规模验证
- `detect` 全部失败时，优先检查 token 权限与网络
- 清库操作不可撤销
- 指数成分变化库与预测库不依赖 Tushare，可独立使用
