"""命令行接口模块"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import uvicorn

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sql_tool.api.app import create_app
from sql_tool.services.core import SqlToolService

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='A股本地数据库维护工具（Tushare 单源）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
示例:
  python main.py config --token YOUR_TUSHARE_TOKEN
  python main.py import --limit 100
  python main.py import --all
  python main.py update
  python main.py stats
  python main.py detect
  python main.py api --host 127.0.0.1 --port 8000
        ''',
    )

    subparsers = parser.add_subparsers(dest='command', help='命令')

    config_parser = subparsers.add_parser('config', help='配置管理')
    config_parser.add_argument('--token', help='设置 Tushare token')

    import_parser = subparsers.add_parser('import', help='导入股票数据')
    import_parser.add_argument('--limit', type=int, help='导入数量限制')
    import_parser.add_argument('--all', action='store_true', help='导入所有股票')
    import_parser.add_argument('--skip-existing', action='store_true', default=True, help='跳过已存在的数据')
    import_parser.add_argument('--no-skip-existing', action='store_false', dest='skip_existing', help='不跳过已存在的数据')

    update_parser = subparsers.add_parser('update', help='更新已有股票数据')
    update_parser.add_argument('--code', help='只更新指定股票代码，如 002594')
    subparsers.add_parser('stats', help='显示数据库统计')
    subparsers.add_parser('clear', help='清空数据库')

    detect_parser = subparsers.add_parser('detect', help='检测 Tushare 接口能力')
    detect_parser.add_argument('--code', help='用于检测的样本股票代码')

    etf_detect_parser = subparsers.add_parser('etf-detect', help='检测 ETF 接口能力')
    etf_detect_parser.add_argument('--code', help='用于检测的样本 ETF 代码')

    etf_import_parser = subparsers.add_parser('etf-import', help='导入 ETF 数据')
    etf_import_parser.add_argument('--limit', type=int, help='导入数量限制')
    etf_import_parser.add_argument('--all', action='store_true', help='导入全部 ETF')
    etf_import_parser.add_argument('--skip-existing', action='store_true', default=True, help='跳过已存在的数据')
    etf_import_parser.add_argument('--no-skip-existing', action='store_false', dest='skip_existing', help='不跳过已存在的数据')

    etf_update_parser = subparsers.add_parser('etf-update', help='更新 ETF 数据')
    etf_update_parser.add_argument('--code', help='只更新指定 ETF 代码，如 510300')

    feature_detect_parser = subparsers.add_parser('feature-detect', help='检测特色数据接口能力')
    feature_detect_parser.add_argument('--code', help='用于检测的样本股票代码')

    feature_sync_parser = subparsers.add_parser('feature-sync', help='同步特色数据')
    feature_sync_parser.add_argument('dataset', choices=['concepts', 'moneyflow', 'top-list'])
    feature_sync_parser.add_argument('--code', help='股票代码')
    feature_sync_parser.add_argument('--start-date', help='开始日期 YYYYMMDD')
    feature_sync_parser.add_argument('--end-date', help='结束日期 YYYYMMDD')
    feature_sync_parser.add_argument('--trade-date', help='交易日期 YYYYMMDD')

    api_parser = subparsers.add_parser('api', help='启动本地 HTTP API')
    api_parser.add_argument('--host', help='监听地址')
    api_parser.add_argument('--port', type=int, help='监听端口')
    return parser


def show_stats(service: SqlToolService) -> None:
    stats = service.get_all_stats()
    logger.info('📊 数据库统计:')
    for label, item in [
        ('股票库', stats.get('stocks', {})),
        ('ETF库', stats.get('etfs', {})),
        ('特色库', stats.get('features', {})),
        ('事件库', stats.get('events', {})),
        ('指数变化库', stats.get('indexes', {})),
        ('指数预测库', stats.get('index_forecasts', {})),
    ]:
        logger.info('  [%s] 数据库: %s', label, item.get('db_path', 'N/A'))
        logger.info('  [%s] 文件大小: %s 字节', label, item.get('db_size_bytes', 0))
        logger.info('  [%s] 各表行数: %s', label, item.get('table_counts', {}))


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return

    service = SqlToolService()

    if args.command == 'config':
        if args.token:
            service.config.set_tushare_token(args.token)
            logger.info('✓ Token 已设置')
        else:
            token = service.config.get_tushare_token()
            if token:
                logger.info('当前 Token: %s...%s', token[:8], token[-4:])
            else:
                logger.warning('⚠️ Token 未设置')
        return

    if args.command == 'import':
        limit = None if args.all else (args.limit or 100)
        result = service.import_data(limit=limit, skip_existing=args.skip_existing, log=logger.info)
        logger.info('导入结果: %s', result)
        return

    if args.command == 'update':
        result = service.update_data(log=logger.info, code=args.code)
        logger.info('更新结果: %s', result)
        return

    if args.command == 'stats':
        show_stats(service)
        return

    if args.command == 'detect':
        result = service.detect_capabilities(sample_code=args.code)
        logger.info('股票接口可用数: %s/%s', result['available_count'], result['total_count'])
        for item in result['results']:
            logger.info('%s | 可用=%s | 空数据=%s | 行数=%s | 错误=%s', item['api_name'], item['available'], item['empty'], item['rows'], item['error'])
        return

    if args.command == 'etf-detect':
        result = service.detect_etf_capabilities(sample_code=args.code)
        logger.info('ETF 接口可用数: %s/%s', result['available_count'], result['total_count'])
        for item in result['results']:
            logger.info('%s | 可用=%s | 空数据=%s | 行数=%s | 错误=%s', item['api_name'], item['available'], item['empty'], item['rows'], item['error'])
        return

    if args.command == 'etf-import':
        limit = None if args.all else (args.limit or 100)
        result = service.import_etf_data(limit=limit, skip_existing=args.skip_existing, log=logger.info)
        logger.info('ETF 导入结果: %s', result)
        return

    if args.command == 'etf-update':
        result = service.update_etf_data(log=logger.info, code=args.code)
        logger.info('ETF 更新结果: %s', result)
        return

    if args.command == 'feature-detect':
        result = service.detect_feature_capabilities(sample_code=args.code)
        logger.info('特色接口可用数: %s/%s', result['available_count'], result['total_count'])
        for item in result['results']:
            logger.info('%s | 可用=%s | 空数据=%s | 行数=%s | 错误=%s', item['api_name'], item['available'], item['empty'], item['rows'], item['error'])
        for item in result.get('pending', []):
            logger.info('%s | 待确认 | 错误=%s', item['api_name'], item['error'])
        return

    if args.command == 'feature-sync':
        if args.dataset == 'concepts':
            result = service.sync_feature_concepts(log=logger.info)
        elif args.dataset == 'moneyflow':
            if not args.code:
                raise ValueError('moneyflow 需要 --code')
            result = service.sync_feature_moneyflow(args.code, start_date=args.start_date, end_date=args.end_date, log=logger.info)
        else:
            result = service.sync_feature_top_list(code=args.code, trade_date=args.trade_date, log=logger.info)
        logger.info('特色同步结果: %s', result)
        return

    if args.command == 'clear':
        confirm = input('确定要清空所有数据吗？(yes/no): ')
        if confirm.lower() == 'yes':
            service.clear_data()
            logger.info('✓ 数据库已清空')
        else:
            logger.info('已取消')
        return

    if args.command == 'api':
        host = args.host or service.config.get_api_host()
        port = args.port or service.config.get_api_port()
        logger.info('启动 API: http://%s:%s', host, port)
        uvicorn.run(create_app(service), host=host, port=port)
        return


if __name__ == '__main__':
    main()
