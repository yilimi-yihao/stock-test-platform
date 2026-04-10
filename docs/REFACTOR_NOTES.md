# 迁移说明

当前工程的旧平铺兼容层已经清理，主实现已统一收敛到目录化结构：

- `src/sql_tool/api/app.py`：API 真源
- `src/sql_tool/gui/desktop.py`：GUI 真源
- `src/sql_tool/services/core.py`：服务层真源
- `src/sql_tool/db/*`：数据库实现
- `src/sql_tool/sources/*`：数据源实现
- `src/sql_tool/exporters_pkg/table.py`：导出工具

## 备份

已在本地生成重构前备份：

- `.claude/backups/pre_refactor_20260406_163252`
- `.claude/backups/aggressive_refactor_20260406_180544`

说明：
- 备份不包含 `data/` 下的本地数据库文件
- 备份不包含 `.git` 与 worktrees
- 用于后续更大胆但可回退的结构优化

## 当前状态

- 旧兼容层文件已删除，避免双路径维护
- 运行主链路已切到目录化真源
- 已完成模块清理后的回归测试验证
- 当前回归结果：46 个测试通过

## 当前建议

- 后续新增功能统一落在目录化真源中，不再新增平铺模块
- 文档、测试、代码结构保持同步更新
- sandbox 若确认无文档/代码依赖，可继续清理

详细结构说明见：`docs/PROJECT_STRUCTURE.md`
