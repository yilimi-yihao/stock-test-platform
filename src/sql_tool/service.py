"""嵌入式高层服务接口（供 GUI/其他工程调用）"""

from __future__ import annotations

from typing import Any, Callable, Dict, Optional

from sql_tool.config import ConfigManager
from sql_tool.database import StockDatabase
from sql_tool.tushare_source import TushareSource

LogFn = Optional[Callable[[str], None]]


class SqlToolService:
    """sql-tool 高层服务"""

    def __init__(self, config_path: str = 'config/settings.json'):
        self.config = ConfigManager(config_path=config_path)
        self.db = StockDatabase(self.config.get_db_path())
        self.source: Optional[TushareSource] = None

    def init_source(self) -> bool:
        token = self.config.get_tushare_token()
        api_url = self.config.get_tushare_api_url()
        if not token:
            return False
        self.source = TushareSource(token, api_url=api_url)
        return True

    def _ensure_source(self) -> TushareSource:
        if self.source is None and not self.init_source():
            raise ValueError('Tushare token 未配置，请先配置 token')
        return self.source

    def _log(self, log: LogFn, message: str) -> None:
        if log:
            log(message)

    def get_stats(self) -> Dict[str, Any]:
        return self.db.get_stats()

    def get_stocks(self, limit: Optional[int] = None, search: str = '') -> list[dict[str, Any]]:
        return self.db.get_stock_list(limit=limit, search=search)

    def get_stock_daily(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        return self.db.get_daily_prices(code, start_date=start_date, end_date=end_date, limit=limit)

    def get_stock_financials(self, code: str, limit: int = 8) -> dict[str, Any]:
        return {
            'stock': self.db.get_stock_overview(code),
            'financials': self.db.get_stock_financials(code, limit=limit),
        }

    def clear_data(self) -> None:
        self.db.clear_all()

    def detect_capabilities(self, sample_code: Optional[str] = None) -> dict[str, Any]:
        source = self._ensure_source()
        sample = sample_code or self.config.get_sample_stock()
        results = source.detect_capabilities(sample)
        available_count = sum(1 for item in results if item['available'])
        return {
            'sample_code': sample,
            'available_count': available_count,
            'total_count': len(results),
            'results': results,
        }

    def _sync_stock(self, code: str, name: str, stock: dict[str, Any], log: LogFn) -> dict[str, int]:
        source = self._ensure_source()
        info = source.get_stock_info(code)
        daily_df = source.get_daily_prices(code)
        if daily_df.empty:
            self._log(log, '  无日线数据')
            return {'daily_rows': 0, 'financial_rows': 0}

        with self.db.get_connection() as conn:
            self.db.insert_stock(
                code=code,
                name=info.get('name', name),
                area=info.get('area', stock.get('area', '')),
                industry=info.get('industry', stock.get('industry', '')),
                list_date=info.get('list_date', stock.get('list_date', '')),
                market_cap=info.get('market_cap', 0),
                circ_mv=info.get('circ_mv', 0),
                pe_ratio=info.get('pe_ratio', 0),
                pb_ratio=info.get('pb_ratio', 0),
                turnover_rate=info.get('turnover_rate', 0),
                volume_ratio=info.get('volume_ratio', 0),
                adj_factor=info.get('adj_factor', 0),
                conn=conn,
            )
            daily_rows = self.db.insert_daily_prices(code, daily_df, conn=conn)
            financial_rows = 0

            fina_df = source.get_fina_indicator(code)
            financial_rows += self.db.insert_fina_indicator(code, fina_df, conn=conn)

            income_df = source.get_income(code)
            financial_rows += self.db.insert_income(code, income_df, conn=conn)

            bs_df = source.get_balancesheet(code)
            financial_rows += self.db.insert_balancesheet(code, bs_df, conn=conn)

            cf_df = source.get_cashflow(code)
            financial_rows += self.db.insert_cashflow(code, cf_df, conn=conn)

        return {'daily_rows': daily_rows, 'financial_rows': financial_rows}

    def import_data(
        self,
        limit: Optional[int] = 100,
        skip_existing: bool = True,
        log: LogFn = None,
    ) -> Dict[str, Any]:
        source = self._ensure_source()
        stocks = source.get_stock_list()
        if limit:
            stocks = stocks[:limit]

        summary = {
            'mode': 'import',
            'total': len(stocks),
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'daily_rows': 0,
            'financial_rows': 0,
        }
        self._log(log, f'开始导入，股票数: {summary["total"]}')

        for index, stock in enumerate(stocks, 1):
            code = stock['code']
            name = stock.get('name', code)

            if skip_existing and self.db.get_latest_date(code):
                summary['skipped'] += 1
                self._log(log, f'[{index}/{summary["total"]}] 跳过 {code}（已有数据）')
                continue

            try:
                self._log(log, f'[{index}/{summary["total"]}] 导入 {code} {name}')
                row_stats = self._sync_stock(code, name, stock, log)
                if row_stats['daily_rows'] == 0:
                    summary['failed'] += 1
                    continue
                summary['success'] += 1
                summary['daily_rows'] += row_stats['daily_rows']
                summary['financial_rows'] += row_stats['financial_rows']
                self._log(log, f'  成功，新增 {row_stats["daily_rows"]} 条日线，财务 {row_stats["financial_rows"]} 条')
            except Exception as exc:
                summary['failed'] += 1
                self._log(log, f'  失败: {exc}')

        self._log(
            log,
            f'导入完成：成功 {summary["success"]}，失败 {summary["failed"]}，跳过 {summary["skipped"]}，日线 {summary["daily_rows"]}，财务 {summary["financial_rows"]}',
        )
        return summary

    def update_data(self, log: LogFn = None) -> Dict[str, Any]:
        source = self._ensure_source()
        stocks = self.db.get_stock_list()
        summary = {
            'mode': 'update',
            'total': len(stocks),
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'daily_rows': 0,
            'financial_rows': 0,
        }
        self._log(log, f'开始更新，股票数: {summary["total"]}')

        for index, stock in enumerate(stocks, 1):
            code = stock['code']
            name = stock.get('name', code)
            latest_date = self.db.get_latest_date(code)

            try:
                self._log(log, f'[{index}/{summary["total"]}] 更新 {code} {name}')
                daily_df = source.get_daily_prices(code)
                if daily_df.empty:
                    summary['skipped'] += 1
                    self._log(log, '  无数据')
                    continue

                if latest_date:
                    daily_df = daily_df[daily_df['date'] > latest_date]
                if daily_df.empty:
                    summary['skipped'] += 1
                    self._log(log, f'  已最新（{latest_date}）')
                    continue

                info = source.get_stock_info(code)
                with self.db.get_connection() as conn:
                    self.db.insert_stock(
                        code=code,
                        name=info.get('name', name),
                        area=info.get('area', stock.get('area', '')),
                        industry=info.get('industry', stock.get('industry', '')),
                        list_date=info.get('list_date', stock.get('list_date', '')),
                        market_cap=info.get('market_cap', 0),
                        circ_mv=info.get('circ_mv', 0),
                        pe_ratio=info.get('pe_ratio', 0),
                        pb_ratio=info.get('pb_ratio', 0),
                        turnover_rate=info.get('turnover_rate', 0),
                        volume_ratio=info.get('volume_ratio', 0),
                        adj_factor=info.get('adj_factor', 0),
                        conn=conn,
                    )
                    inserted_daily_rows = self.db.insert_daily_prices(code, daily_df, conn=conn)
                    financial_rows = 0
                    financial_rows += self.db.insert_fina_indicator(code, source.get_fina_indicator(code), conn=conn)
                    financial_rows += self.db.insert_income(code, source.get_income(code), conn=conn)
                    financial_rows += self.db.insert_balancesheet(code, source.get_balancesheet(code), conn=conn)
                    financial_rows += self.db.insert_cashflow(code, source.get_cashflow(code), conn=conn)

                summary['success'] += 1
                summary['daily_rows'] += inserted_daily_rows
                summary['financial_rows'] += financial_rows
                self._log(log, f'  成功，新增 {inserted_daily_rows} 条日线，财务 {financial_rows} 条')
            except Exception as exc:
                summary['failed'] += 1
                self._log(log, f'  失败: {exc}')

        self._log(
            log,
            f'更新完成：成功 {summary["success"]}，失败 {summary["failed"]}，跳过 {summary["skipped"]}，日线 {summary["daily_rows"]}，财务 {summary["financial_rows"]}',
        )
        return summary
