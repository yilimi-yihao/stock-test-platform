"""简洁 GUI 界面（tkinter）"""

from __future__ import annotations

import contextlib
import os
import queue
import socket
import subprocess
import sys
import threading
import time
import tkinter as tk
import urllib.error
import urllib.request
import webbrowser
from typing import Any
from datetime import datetime
from pathlib import Path
from tkinter import messagebox, scrolledtext, ttk

from sql_tool.services import SqlToolService


API_CHECK_INTERVAL_MS = 1500

# 日志颜色标签
_LOG_TAGS = {
    'info':    ('#a3c4f3', 'normal'),
    'success': ('#6ee7b7', 'normal'),
    'warn':    ('#fde68a', 'normal'),
    'error':   ('#fca5a5', 'bold'),
    'api':     ('#c4b5fd', 'normal'),
    'dim':     ('#64748b', 'normal'),
}


def _is_port_open(host: str, port: int) -> bool:
    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.settimeout(0.3)
        return sock.connect_ex((host, port)) == 0


def _check_api_health(host: str, port: int, timeout: float = 1.0) -> tuple[bool, str]:
    url = f'http://{host}:{port}/health'
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            payload = response.read().decode('utf-8', errors='ignore')
        return response.status == 200 and 'ok' in payload.lower(), payload
    except urllib.error.URLError as exc:
        return False, str(exc)
    except Exception as exc:
        return False, str(exc)


def _check_api_health_async(host: str, port: int,
                             callback) -> None:
    """在后台线程检测 API 健康状态，结果通过 callback(ok, detail, port_open) 返回"""
    def _run():
        ok, detail = _check_api_health(host, port, timeout=0.5)
        port_open = False if ok else _is_port_open(host, port)
        callback(ok, detail, port_open)
    threading.Thread(target=_run, daemon=True).start()


def _classify_msg(msg: str) -> str:
    m = msg.lower()
    if msg.startswith('[api]') or msg.startswith('[API]'):
        return 'api'
    if any(k in m for k in ('失败', 'error', '错误', 'fail', 'exception', 'traceback')):
        return 'error'
    if any(k in m for k in ('成功', 'success', '完成', '导入完成', '更新完成')):
        return 'success'
    if any(k in m for k in ('警告', 'warn', '跳过')):
        return 'warn'
    return 'info'


class _LogPanel(tk.Frame):
    """可复用的日志面板：带时间戳、色彩分级、清除按钮。"""

    def __init__(self, parent, height=10, **kw):
        super().__init__(parent, bg='#0d1117', **kw)

        # 顶部工具栏
        toolbar = tk.Frame(self, bg='#161b22', pady=4)
        toolbar.pack(fill=tk.X)
        tk.Label(toolbar, text='  运行日志', bg='#161b22', fg='#8b949e',
                 font=('Microsoft YaHei', 9)).pack(side=tk.LEFT)
        tk.Button(toolbar, text='清除', command=self.clear,
                  bg='#21262d', fg='#8b949e', relief=tk.FLAT,
                  font=('Microsoft YaHei', 8), padx=8, pady=2,
                  cursor='hand2', activebackground='#30363d',
                  activeforeground='#c9d1d9').pack(side=tk.RIGHT, padx=6)

        # 文本区
        self.text = tk.Text(
            self,
            height=height,
            font=('Consolas', 9),
            bg='#0d1117',
            fg='#c9d1d9',
            insertbackground='#c9d1d9',
            relief=tk.FLAT,
            bd=0,
            padx=10,
            pady=6,
            state=tk.DISABLED,
            wrap=tk.WORD,
        )
        sb = ttk.Scrollbar(self, orient=tk.VERTICAL, command=self.text.yview)
        self.text.configure(yscrollcommand=sb.set)
        self.text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sb.pack(side=tk.RIGHT, fill=tk.Y)

        # 注册颜色标签
        for tag, (fg, weight) in _LOG_TAGS.items():
            self.text.tag_config(tag, foreground=fg,
                                 font=('Consolas', 9, weight))
        self.text.tag_config('ts', foreground='#3d4f6b',
                              font=('Consolas', 9, 'normal'))

    def append(self, msg: str, tag: str | None = None) -> None:
        ts = datetime.now().strftime('%H:%M:%S')
        t = tag or _classify_msg(msg)
        self.text.configure(state=tk.NORMAL)
        self.text.insert(tk.END, f'[{ts}] ', 'ts')
        self.text.insert(tk.END, msg + '\n', t)
        self.text.see(tk.END)
        self.text.configure(state=tk.DISABLED)

    def clear(self) -> None:
        self.text.configure(state=tk.NORMAL)
        self.text.delete(1.0, tk.END)
        self.text.configure(state=tk.DISABLED)


class SqlToolGUI:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title('A股/ETF 数据库管理工具')
        self.root.geometry('1160x860')
        self.root.configure(bg='#f0f4f8')
        self._set_window_icon()

        self.service = SqlToolService()
        self.running = False
        self.current_task_label = tk.StringVar(value='空闲')
        self.log_queue: queue.Queue[tuple[str, str | None]] = queue.Queue()
        self.api_log_queue: queue.Queue[tuple[str, str | None]] = queue.Queue()
        self.action_buttons: list[tk.Button] = []
        self.api_process: subprocess.Popen[str] | None = None
        self.api_log_thread: threading.Thread | None = None

        self.api_host_var = tk.StringVar(value=self.service.config.get_api_host())
        self.api_port_var = tk.IntVar(value=self.service.config.get_api_port())
        self.api_status_var = tk.StringVar(value='未启动')
        self.api_hint_var = tk.StringVar(value='可在 GUI 内启动本地 API 并查看运行状态。')
        self.capability_summary_var = tk.StringVar(
            value='尚未检测接口能力，导入/更新将先按当前 token 实时判定可用接口。'
        )
        self.token_var = tk.StringVar(value=self.service.config.get_tushare_token())
        self.stock_search_var = tk.StringVar(value='002594')
        self.stock_update_var = tk.StringVar(value='002594')
        self.etf_search_var = tk.StringVar(value='562500')
        self.etf_update_var = tk.StringVar(value='562500')
        self.capability_code_var = tk.StringVar(value='002594')
        self.capability_etf_var = tk.StringVar(value='510300')
        self.table_domain_var = tk.StringVar(value='stocks')
        self.table_name_var = tk.StringVar(value='')

        self._setup_style()
        self._build_ui()
        self.refresh_stats()
        self.refresh_api_status(log_on_change=False)
        self.root.after(100, self._drain_log_queue)
        self.root.after(API_CHECK_INTERVAL_MS, self._poll_api_status)
        self.root.protocol('WM_DELETE_WINDOW', self._on_close)

    # ── Style ────────────────────────────────────────────────────────────────

    def _setup_style(self) -> None:
        style = ttk.Style()
        style.theme_use('clam')
        style.configure('TFrame', background='#f0f4f8')
        style.configure('Card.TFrame', background='#ffffff')
        style.configure('TLabel', background='#f0f4f8', foreground='#1e293b')
        style.configure('Card.TLabel', background='#ffffff', foreground='#1e293b')
        style.configure('Title.TLabel',
                         font=('Microsoft YaHei', 17, 'bold'),
                         foreground='#0f172a', background='#f0f4f8')
        style.configure('Heading.TLabel',
                         font=('Microsoft YaHei', 10, 'bold'),
                         foreground='#1e293b', background='#f0f4f8')
        style.configure('Subtle.TLabel', background='#f0f4f8', foreground='#64748b')
        style.configure('CardSubtle.TLabel', background='#ffffff', foreground='#64748b')
        style.configure('Treeview', rowheight=27, font=('Microsoft YaHei', 9),
                         background='#ffffff', fieldbackground='#ffffff',
                         foreground='#1e293b')
        style.configure('Treeview.Heading', font=('Microsoft YaHei', 9, 'bold'),
                         background='#f8fafc', foreground='#334155')
        style.map('Treeview', background=[('selected', '#dbeafe')],
                  foreground=[('selected', '#1e40af')])
        style.configure('TLabelframe', background='#ffffff', relief='flat',
                         borderwidth=1)
        style.configure('TLabelframe.Label', background='#ffffff',
                         foreground='#475569', font=('Microsoft YaHei', 9, 'bold'))
        style.configure('TNotebook', background='#f0f4f8', borderwidth=0)
        style.configure('TNotebook.Tab',
                         padding=(16, 9),
                         font=('Microsoft YaHei', 10),
                         background='#e2e8f0',
                         foreground='#475569')
        style.map('TNotebook.Tab',
                  background=[('selected', '#ffffff')],
                  foreground=[('selected', '#1e40af')])
        style.configure('TProgressbar', troughcolor='#e2e8f0',
                         background='#3b82f6', thickness=4)

    def _set_window_icon(self) -> None:
        project_root = Path(__file__).resolve().parents[3]
        ico_path = project_root / 'assets' / 'app_icon.ico'
        png_path = project_root / 'assets' / 'app_icon.png'
        try:
            if ico_path.exists():
                self.root.iconbitmap(default=str(ico_path))
                return
        except Exception:
            pass
        try:
            if png_path.exists():
                self._icon_image = tk.PhotoImage(file=str(png_path))
                self.root.iconphoto(True, self._icon_image)
        except Exception:
            pass

    # ── Root layout ──────────────────────────────────────────────────────────

    def _build_ui(self) -> None:
        outer = ttk.Frame(self.root, padding=(20, 14, 20, 14))
        outer.pack(fill=tk.BOTH, expand=True)

        hdr = ttk.Frame(outer)
        hdr.pack(fill=tk.X, pady=(0, 10))
        ttk.Label(hdr, text='股票数据库管理', style='Title.TLabel').pack(anchor='w')
        ttk.Label(hdr, text='Tushare 导入  ·  数据浏览  ·  本地 API',
                  style='Subtle.TLabel').pack(anchor='w', pady=(3, 0))

        quick = tk.Frame(outer, bg='#f0f4f8')
        quick.pack(fill=tk.X, pady=(0, 10))

        config_card = tk.Frame(quick, bg='#ffffff', highlightthickness=1, highlightbackground='#e2e8f0', padx=12, pady=10)
        config_card.pack(fill=tk.X)
        tk.Label(config_card, text='全局面板', bg='#ffffff', fg='#1e293b', font=('Microsoft YaHei', 10, 'bold')).grid(row=0, column=0, sticky='w', padx=(0, 10))
        tk.Label(config_card, text='Token:', bg='#ffffff', fg='#475569').grid(row=1, column=0, sticky='w', padx=(0, 6), pady=(8, 0))
        ttk.Entry(config_card, textvariable=self.token_var, width=32, show='*').grid(row=1, column=1, sticky='w', pady=(8, 0))
        tk.Label(config_card, text='API Host:', bg='#ffffff', fg='#475569').grid(row=1, column=2, sticky='w', padx=(14, 6), pady=(8, 0))
        ttk.Entry(config_card, textvariable=self.api_host_var, width=16).grid(row=1, column=3, sticky='w', pady=(8, 0))
        tk.Label(config_card, text='API Port:', bg='#ffffff', fg='#475569').grid(row=1, column=4, sticky='w', padx=(14, 6), pady=(8, 0))
        ttk.Entry(config_card, textvariable=self.api_port_var, width=10).grid(row=1, column=5, sticky='w', pady=(8, 0))
        btn_row = tk.Frame(config_card, bg='#ffffff')
        btn_row.grid(row=2, column=0, columnspan=6, sticky='w', pady=(12, 0))
        self._create_button(btn_row, '保存配置', self.save_settings, '#2563eb', track=False)
        self._create_button(btn_row, '刷新概览', self.refresh_stats, '#475569', track=False)
        self._create_button(btn_row, '分析优化', self.run_analyze, '#475569', track=False)
        self._create_button(btn_row, '打开 API 指南', self.open_api_guide, '#0f766e', track=False)
        self._create_button(btn_row, '打开帮助页', self.open_help_page, '#7c3aed', track=False)
        self.global_stop_button = self._create_button(btn_row, '停止当前任务', self._cancel_task, '#92400e', track=False)
        self.global_stop_button.config(state=tk.DISABLED)
        tk.Label(config_card, text='当前任务:', bg='#ffffff', fg='#475569').grid(row=3, column=0, sticky='w', padx=(0, 6), pady=(10, 0))
        tk.Label(config_card, textvariable=self.current_task_label, bg='#ffffff', fg='#1e293b').grid(row=3, column=1, columnspan=5, sticky='w', pady=(10, 0))

        self.summary_card = self._make_info_card(outer)
        self.summary_card.pack(fill=tk.X, pady=(0, 10))

        notebook = ttk.Notebook(outer)
        notebook.pack(fill=tk.BOTH, expand=True)

        for label, builder in [
            ('  股票工作台  ', self._build_data_tab),
            ('  ETF工作台  ', self._build_etf_tab),
            ('  股票扩展域  ', self._build_feature_tab),
            ('  按库数据查看  ', self._build_analysis_tab),
            ('  接口检测  ', self._build_capability_tab),
            ('  API 工具  ', self._build_api_tab),
            ('  全局/帮助  ', self._build_help_tab),
        ]:
            f = ttk.Frame(notebook)
            notebook.add(f, text=label)
            builder(f)

    def _make_info_card(self, parent) -> tk.Label:
        return tk.Label(
            parent,
            text='',
            justify=tk.LEFT,
            bg='#ffffff',
            fg='#1e293b',
            font=('Microsoft YaHei', 10),
            padx=16,
            pady=11,
            relief=tk.FLAT,
            bd=0,
            highlightthickness=1,
            highlightbackground='#e2e8f0',
        )

    # ── Tab: 数据管理 ─────────────────────────────────────────────────────────

    def _build_data_tab(self, parent: ttk.Frame) -> None:
        frame = ttk.Frame(parent, padding=(16, 12))
        frame.pack(fill=tk.BOTH, expand=True)

        ctrl = tk.Frame(frame, bg='#ffffff', highlightthickness=1, highlightbackground='#e2e8f0')
        ctrl.pack(fill=tk.X, pady=(0, 10))

        inner = tk.Frame(ctrl, bg='#ffffff', padx=16, pady=12)
        inner.pack(fill=tk.X)

        tk.Label(inner, text='股票工作台', bg='#ffffff', fg='#1e293b', font=('Microsoft YaHei', 10, 'bold')).grid(row=0, column=0, columnspan=4, sticky='w')
        tk.Label(inner, text='单股代码:', bg='#ffffff', fg='#475569', font=('Microsoft YaHei', 10)).grid(row=1, column=0, sticky='w', padx=(0, 6), pady=(10, 0))
        ttk.Entry(inner, textvariable=self.stock_update_var, width=14).grid(row=1, column=1, sticky='w', pady=(10, 0))

        btn_row = tk.Frame(inner, bg='#ffffff')
        btn_row.grid(row=2, column=0, columnspan=4, sticky='w', pady=(12, 0))
        self._create_button(btn_row, '导入全部股票', self.import_all_data, '#1d4ed8')
        self._create_button(btn_row, '增量更新', self.update_data, '#0f766e')
        self._create_button(btn_row, '更新单股', self.update_selected_stock, '#7c3aed')
        self._create_button(btn_row, '清除数据', self.clear_data, '#dc2626')

        self.task_log = _LogPanel(frame, height=18)
        self.task_log.pack(fill=tk.BOTH, expand=True)

    # ── Tab: ETF 工作台 ───────────────────────────────────────────────────────

    def _build_etf_tab(self, parent: ttk.Frame) -> None:
        frame = ttk.Frame(parent, padding=(16, 12))
        frame.pack(fill=tk.BOTH, expand=True)

        ctrl = tk.Frame(frame, bg='#ffffff', highlightthickness=1, highlightbackground='#e2e8f0')
        ctrl.pack(fill=tk.X, pady=(0, 10))
        inner = tk.Frame(ctrl, bg='#ffffff', padx=16, pady=12)
        inner.pack(fill=tk.X)

        tk.Label(inner, text='ETF 工作台', bg='#ffffff', fg='#1e293b', font=('Microsoft YaHei', 10, 'bold')).grid(row=0, column=0, columnspan=4, sticky='w')
        tk.Label(inner, text='ETF代码:', bg='#ffffff', fg='#475569', font=('Microsoft YaHei', 10)).grid(row=1, column=0, sticky='w', padx=(0, 6), pady=(10, 0))
        ttk.Entry(inner, textvariable=self.etf_update_var, width=14).grid(row=1, column=1, sticky='w', pady=(10, 0))

        btn_row = tk.Frame(inner, bg='#ffffff')
        btn_row.grid(row=2, column=0, columnspan=4, sticky='w', pady=(12, 0))
        self._create_button(btn_row, '导入全部 ETF', self.import_etf_data, '#2563eb')
        self._create_button(btn_row, '增量更新 ETF', self.update_etf_data, '#0f766e')
        self._create_button(btn_row, '更新单个 ETF', self.update_selected_etf, '#7c3aed')

        self.etf_log = _LogPanel(frame, height=16)
        self.etf_log.pack(fill=tk.BOTH, expand=True)

    # ── Tab: 特色数据 ─────────────────────────────────────────────────────────

    def _build_feature_tab(self, parent: ttk.Frame) -> None:
        frame = ttk.Frame(parent, padding=(16, 12))
        frame.pack(fill=tk.BOTH, expand=True)

        ctrl = tk.Frame(frame, bg='#ffffff', highlightthickness=1, highlightbackground='#e2e8f0')
        ctrl.pack(fill=tk.X, pady=(0, 10))
        inner = tk.Frame(ctrl, bg='#ffffff', padx=16, pady=12)
        inner.pack(fill=tk.X)

        tk.Label(inner, text='股票代码:', bg='#ffffff', fg='#475569', font=('Microsoft YaHei', 10)).grid(row=0, column=0, sticky='w', padx=(0, 6))
        self.feature_code_var = tk.StringVar(value='002594')
        ttk.Entry(inner, textvariable=self.feature_code_var, width=12).grid(row=0, column=1, sticky='w')
        tk.Label(inner, text='交易日:', bg='#ffffff', fg='#475569', font=('Microsoft YaHei', 10)).grid(row=0, column=2, sticky='w', padx=(16, 6))
        self.feature_trade_date_var = tk.StringVar(value='')
        ttk.Entry(inner, textvariable=self.feature_trade_date_var, width=12).grid(row=0, column=3, sticky='w')

        btn_row = tk.Frame(inner, bg='#ffffff')
        btn_row.grid(row=1, column=0, columnspan=4, sticky='w', pady=(12, 0))
        self._create_button(btn_row, '同步概念基表', self.sync_feature_concepts, '#7c3aed')
        self._create_button(btn_row, '同步单股扩展', self.sync_feature_for_stock, '#2563eb')
        self._create_button(btn_row, '全量资金流向', self.sync_feature_moneyflow_all, '#0369a1')
        self._create_button(btn_row, '同步全市场龙虎榜', self.sync_feature_market_wide, '#0f766e')

        self.feature_log = _LogPanel(frame, height=16)
        self.feature_log.pack(fill=tk.BOTH, expand=True)

    # ── Tab: 接口检测 ─────────────────────────────────────────────────────────

    def _build_capability_tab(self, parent: ttk.Frame) -> None:
        frame = ttk.Frame(parent, padding=(16, 12))
        frame.pack(fill=tk.BOTH, expand=True)

        top = tk.Frame(frame, bg='#f0f4f8')
        top.pack(fill=tk.X, pady=(0, 8))
        tk.Label(top, text='股票样本:', bg='#f0f4f8', fg='#475569', font=('Microsoft YaHei', 10)).pack(side=tk.LEFT)
        ttk.Entry(top, textvariable=self.capability_code_var, width=12).pack(side=tk.LEFT, padx=8)
        tk.Label(top, text='ETF样本:', bg='#f0f4f8', fg='#475569', font=('Microsoft YaHei', 10)).pack(side=tk.LEFT, padx=(12, 0))
        ttk.Entry(top, textvariable=self.capability_etf_var, width=12).pack(side=tk.LEFT, padx=8)
        self._create_button(top, '统一检测', self.detect_all_capabilities, '#7c3aed')

        ttk.Label(frame, textvariable=self.capability_summary_var, style='Subtle.TLabel').pack(anchor='w', pady=(0, 8))

        self.capability_notebook = ttk.Notebook(frame)
        self.capability_notebook.pack(fill=tk.BOTH, expand=True)
        self.capability_trees = {}
        for key, title in [('stock', '股票接口'), ('etf', 'ETF接口'), ('feature', '扩展接口'), ('pending', '待确认')]:
            tab = ttk.Frame(self.capability_notebook)
            self.capability_notebook.add(tab, text=title)
            cols = ('api_name', 'display_name', 'available', 'empty', 'rows', 'error')
            tree = ttk.Treeview(tab, columns=cols, show='headings', height=12)
            headings = {'api_name': '接口名', 'display_name': '用途', 'available': '可用', 'empty': '空数据', 'rows': '返回行数', 'error': '错误信息'}
            widths = {'api_name': 140, 'display_name': 150, 'available': 70, 'empty': 70, 'rows': 90, 'error': 0}
            for col in cols:
                tree.heading(col, text=headings[col])
                anchor = 'center' if col in {'available', 'empty', 'rows'} else 'w'
                if widths[col]:
                    tree.column(col, width=widths[col], minwidth=70, anchor=anchor, stretch=True)
                else:
                    tree.column(col, anchor=anchor, stretch=True)
            tree.tag_configure('ok', foreground='#16a34a')
            tree.tag_configure('fail', foreground='#dc2626')
            sb = ttk.Scrollbar(tab, orient=tk.VERTICAL, command=tree.yview)
            tree.configure(yscrollcommand=sb.set)
            tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            sb.pack(side=tk.RIGHT, fill=tk.Y)
            self.capability_trees[key] = tree

    # ── Tab: 数据浏览 ─────────────────────────────────────────────────────────

    def _build_analysis_tab(self, parent: ttk.Frame) -> None:
        frame = ttk.Frame(parent, padding=(16, 12))
        frame.pack(fill=tk.BOTH, expand=True)

        # ── 单股 / 单ETF 快速查询 ─────────────────────────────────────────────
        quick = ttk.LabelFrame(frame, text='单股 / 单ETF 快速查看', padding=10)
        quick.pack(fill=tk.X, pady=(0, 10))

        qrow = ttk.Frame(quick)
        qrow.pack(fill=tk.X)
        ttk.Label(qrow, text='代码:').pack(side=tk.LEFT)
        self.quick_code_var = tk.StringVar()
        ttk.Entry(qrow, textvariable=self.quick_code_var, width=12).pack(side=tk.LEFT, padx=(6, 12))
        ttk.Label(qrow, text='数据:').pack(side=tk.LEFT)
        self.quick_type_var = tk.StringVar(value='日线')
        ttk.Combobox(qrow, textvariable=self.quick_type_var,
                     values=['日线', '财务概览', 'ETF日线'],
                     state='readonly', width=10).pack(side=tk.LEFT, padx=(6, 12))
        self._create_button(qrow, '查询', self._on_quick_lookup, '#2563eb', track=False)

        # ── 通用数据表查看 ────────────────────────────────────────────────────
        browser = ttk.LabelFrame(frame, text='通用数据表查看', padding=12)
        browser.pack(fill=tk.BOTH, expand=True)

        controls = ttk.Frame(browser)
        controls.pack(fill=tk.X, pady=(0, 10))
        ttk.Label(controls, text='数据域:').pack(side=tk.LEFT)
        self.table_domain_combo = ttk.Combobox(controls, textvariable=self.table_domain_var, state='readonly', width=14)
        self.table_domain_combo.pack(side=tk.LEFT, padx=(8, 12))
        self.table_domain_combo.bind('<<ComboboxSelected>>', lambda _event: self.refresh_table_options())
        ttk.Label(controls, text='数据表:').pack(side=tk.LEFT)
        self.table_name_combo = ttk.Combobox(controls, textvariable=self.table_name_var, state='readonly', width=28)
        self.table_name_combo.pack(side=tk.LEFT, padx=(8, 12))
        self._create_button(controls, '查看数据', self.load_selected_table, '#475569', track=False)
        self._create_button(controls, '刷新概览', self.refresh_stats, '#0f766e', track=False)

        table_hint = ttk.Frame(browser)
        table_hint.pack(fill=tk.X, pady=(0, 8))
        self.table_hint_var = tk.StringVar(value='数据浏览默认查看最近 200 行；同步/增量/全量任务不受此处限制。拖拽下方分隔条可快速调整表格高度。')
        ttk.Label(table_hint, textvariable=self.table_hint_var, style='Subtle.TLabel').pack(side=tk.LEFT, anchor='w')

        table_pane = ttk.PanedWindow(browser, orient=tk.VERTICAL)
        table_pane.pack(fill=tk.BOTH, expand=True)

        table_frame = ttk.Frame(table_pane)
        table_pane.add(table_frame, weight=4)
        self.data_table = ttk.Treeview(table_frame, show='headings')
        self.data_table.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        table_scroll_v = ttk.Scrollbar(table_frame, orient=tk.VERTICAL, command=self.data_table.yview)
        table_scroll_h = ttk.Scrollbar(browser, orient=tk.HORIZONTAL, command=self.data_table.xview)
        self.data_table.configure(yscrollcommand=table_scroll_v.set, xscrollcommand=table_scroll_h.set)
        table_scroll_v.pack(side=tk.RIGHT, fill=tk.Y)
        table_scroll_h.pack(fill=tk.X)

        detail_frame = ttk.Frame(table_pane, padding=(0, 6, 0, 0))
        table_pane.add(detail_frame, weight=1)
        self.table_meta_var = tk.StringVar(value='提示：拖拽中间分隔条可放大/缩小表格区域。')
        ttk.Label(detail_frame, textvariable=self.table_meta_var, style='Subtle.TLabel').pack(anchor='w')


        self.refresh_table_options()

    # ── Tab: API 服务 ─────────────────────────────────────────────────────────

    def _build_api_tab(self, parent: ttk.Frame) -> None:
        frame = ttk.Frame(parent, padding=(16, 12))
        frame.pack(fill=tk.BOTH, expand=True)

        ctrl = ttk.LabelFrame(frame, text='本地 API 控制', padding=12)
        ctrl.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(ctrl, text='Host:').grid(row=0, column=0, sticky='w')
        ttk.Entry(ctrl, textvariable=self.api_host_var, width=18).grid(
            row=0, column=1, sticky='w', padx=(8, 16))
        ttk.Label(ctrl, text='Port:').grid(row=0, column=2, sticky='w')
        ttk.Entry(ctrl, textvariable=self.api_port_var, width=10).grid(
            row=0, column=3, sticky='w', padx=(8, 0))

        btn_row = ttk.Frame(ctrl)
        btn_row.grid(row=1, column=0, columnspan=4, sticky='w', pady=(10, 0))
        self.api_start_button = self._create_button(
            btn_row, '启动 API', self.start_api_server, '#2563eb', track=False)
        self.api_stop_button = self._create_button(
            btn_row, '停止 API', self.stop_api_server, '#dc2626', track=False)
        self._create_button(btn_row, '检查状态', self.refresh_api_status,
                             '#475569', track=False)
        self._create_button(btn_row, '打开 API 文档', self.open_api_docs,
                             '#0f766e', track=False)

        status_card = self._make_info_card(frame)
        status_card.configure(textvariable=self.api_status_var,
                               font=('Microsoft YaHei', 11, 'bold'))
        status_card.pack(fill=tk.X, pady=(0, 6))

        ttk.Label(frame, textvariable=self.api_hint_var,
                   style='Subtle.TLabel').pack(anchor='w', pady=(0, 10))

        api_examples = scrolledtext.ScrolledText(
            frame, height=10, font=('Consolas', 9), relief=tk.FLAT, bd=0,
            highlightthickness=1, highlightbackground='#e2e8f0',
            padx=10, pady=10,
        )
        api_examples.pack(fill=tk.BOTH, expand=True)
        api_examples.insert(tk.END, (
            '常用接口示例\n\n'
            'GET /stocks/page?page=1&page_size=100\n'
            'GET /stocks?limit=50&search=银行\n'
            'GET /stocks/002594/daily?limit=60\n'
            'POST /stocks/002594/update\n\n'
            'POST /daily/batch\n'
            '{\n'
            '  "codes": ["000001", "002594", "600519"],\n'
            '  "start_date": "2024-01-01",\n'
            '  "end_date": "2024-12-31"\n'
            '}\n\n'
            'POST /stocks/overview/batch\n'
            '{\n'
            '  "codes": ["000001", "002594", "600519"]\n'
            '}\n'
        ))
        api_examples.configure(state='disabled')

        self.api_log = _LogPanel(frame, height=8)
        self.api_log.pack(fill=tk.BOTH, expand=True, pady=(10, 0))

    # ── Tab: 状态（内嵌到帮助页，不独立存在）────────────────────────────────────

    def _build_status_tab(self, parent) -> None:
        cols = ('key', 'value')
        self.status_tree = ttk.Treeview(parent, columns=cols, show='headings', height=8)
        self.status_tree.heading('key', text='项目')
        self.status_tree.heading('value', text='值')
        self.status_tree.column('key', width=200, anchor='w')
        self.status_tree.column('value', anchor='w', stretch=True)
        sb = ttk.Scrollbar(parent, orient=tk.VERTICAL, command=self.status_tree.yview)
        self.status_tree.configure(yscrollcommand=sb.set)
        self.status_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sb.pack(side=tk.RIGHT, fill=tk.Y)

    # ── Tab: 帮助 ─────────────────────────────────────────────────────────────

    def _build_help_tab(self, parent: ttk.Frame) -> None:
        frame = ttk.Frame(parent, padding=(16, 12))
        frame.pack(fill=tk.BOTH, expand=True)

        tools = ttk.LabelFrame(frame, text='全局帮助与资料', padding=12)
        tools.pack(fill=tk.X, pady=(0, 8))
        btn_row = ttk.Frame(tools)
        btn_row.pack(fill=tk.X)
        self._create_button(btn_row, '初始化事件数据', self.seed_event_data, '#2563eb', track=False)
        self._create_button(btn_row, '刷新各库状态', self.refresh_stats, '#0f766e', track=False)
        self._create_button(btn_row, '打开帮助页', self.open_help_page, '#7c3aed', track=False)
        self._create_button(btn_row, '打开指数成分文档', self.open_index_doc, '#475569', track=False)
        self._create_button(btn_row, '打开券商预测文档', self.open_broker_doc, '#0f766e', track=False)

        # 各库状态树
        status_lf = ttk.LabelFrame(frame, text='各库状态（点击「刷新各库状态」更新）', padding=8)
        status_lf.pack(fill=tk.X, pady=(8, 8))
        self._build_status_tab(status_lf)

        ttk.Label(frame, text='工具说明', style='Heading.TLabel').pack(anchor='w', pady=(4, 4))
        box = scrolledtext.ScrolledText(
            frame, height=8, font=('Microsoft YaHei', 10),
            relief=tk.FLAT, bd=0,
            highlightthickness=1, highlightbackground='#e2e8f0',
            padx=12, pady=10,
        )
        box.pack(fill=tk.BOTH, expand=True)
        box.insert(tk.END, (
            '推荐流程\n'
            '1. 配置 token → 接口检测 → 股票/ETF 工作台导入数据\n'
            '2. 需要供数给外部程序时，在 API 工具页启动本地 API\n'
            '3. 事件库通过「初始化事件数据」写入默认样本，或用 AI 事件工具批量生成\n'
            '4. 指数成分变化用 src/sql_tool/tools/index_change_importer.py 从 CSV 导入\n\n'
            '注意事项\n'
            '- 进度百分比在顶部「当前任务」旁显示\n'
            '- 清库操作不可撤销\n'
            '- 各库彼此独立，无跨库外键\n'
        ))
        box.configure(state='disabled')

    # ── Widgets ───────────────────────────────────────────────────────────────

    def _create_button(self, parent, text: str, command, color: str,
                       track: bool = True) -> tk.Button:
        btn = tk.Button(
            parent,
            text=text,
            command=command,
            bg=color,
            fg='white',
            font=('Microsoft YaHei', 9, 'bold'),
            relief=tk.FLAT,
            padx=14,
            pady=7,
            cursor='hand2',
            activebackground=color,
            activeforeground='white',
            bd=0,
        )
        btn.pack(side=tk.LEFT, padx=(0, 6))
        if track:
            self.action_buttons.append(btn)
        return btn

    # ── State ─────────────────────────────────────────────────────────────────

    def _set_running(self, running: bool) -> None:
        self.running = running
        self.current_task_label.set('运行中' if running else '空闲')
        for button in self.action_buttons:
            button.config(state=tk.DISABLED if running else tk.NORMAL)
        self.global_stop_button.config(state=tk.NORMAL if running else tk.DISABLED)

    def _cancel_task(self) -> None:
        self.service.cancel()
        self.log('正在停止当前任务，等待当前处理单元完成…', 'warn')
        self.global_stop_button.config(state=tk.DISABLED)

    def _set_api_buttons(self, is_running: bool) -> None:
        self.api_start_button.config(
            state=tk.DISABLED if is_running else tk.NORMAL)
        self.api_stop_button.config(
            state=tk.NORMAL if is_running else tk.DISABLED)

    # ── Logging ───────────────────────────────────────────────────────────────

    def log(self, msg: str, tag: str | None = None) -> None:
        self.log_queue.put((msg, tag))

    def api_log_message(self, msg: str, tag: str | None = None) -> None:
        self.api_log_queue.put((msg, tag))

    def _drain_log_queue(self) -> None:
        # 每次最多消费 30 条，避免批量日志撑死主线程渲染
        count = 0
        while count < 30 and not self.log_queue.empty():
            msg, tag = self.log_queue.get_nowait()
            self.task_log.append(msg, tag)
            if '[ETF]' in msg:
                self.etf_log.append(msg, tag)
            if '[FEATURE]' in msg:
                self.feature_log.append(msg, tag)
            count += 1
        api_count = 0
        while api_count < 30 and not self.api_log_queue.empty():
            msg, tag = self.api_log_queue.get_nowait()
            self.api_log.append(msg, tag)
            api_count += 1
        self.root.after(100, self._drain_log_queue)

    # ── Thread runner ─────────────────────────────────────────────────────────

    def _run_in_thread(self, func, *args) -> None:
        if self.running:
            messagebox.showwarning('提示', '已有任务运行中')
            return
        self.task_log.clear()
        self.current_task_label.set(getattr(func, '__name__', '后台任务'))
        self._set_running(True)
        threading.Thread(target=self._thread_wrapper,
                         args=(func, *args), daemon=True).start()

    def _thread_wrapper(self, func, *args) -> None:
        try:
            func(*args)
        except Exception as exc:
            self.log(f'错误: {exc}', 'error')
        finally:
            self.root.after(0, lambda: self._set_running(False))
            self.root.after(500, self.refresh_stats)  # 延迟 0.5s 等 DB 写入完成后刷新

    # ── API management ────────────────────────────────────────────────────────

    def _api_command(self, host: str, port: int) -> list[str]:
        project_root = Path(__file__).resolve().parents[3]
        return [sys.executable, str(project_root / 'main.py'),
                'api', '--host', host, '--port', str(port)]

    def _stream_api_logs(self) -> None:
        if not self.api_process or not self.api_process.stdout:
            return
        for line in self.api_process.stdout:
            self.api_log_message(f'[API] {line.rstrip()}', 'api')
        code = self.api_process.poll()
        self.api_log_message(f'[API] 进程已退出，返回码: {code}', 'api')
        self.api_process = None
        self.root.after(0, lambda: self.refresh_api_status(log_on_change=False))

    def _poll_api_status(self) -> None:
        if self.root.winfo_exists():
            # 异步检测，不阻塞主线程
            host = self.api_host_var.get().strip() or self.service.config.get_api_host()
            port = int(self.api_port_var.get() or self.service.config.get_api_port())
            process_alive = self.api_process is not None and self.api_process.poll() is None

            def _on_result(ok, detail, port_open):
                if self.root.winfo_exists():
                    self.root.after(0, lambda: self._apply_api_status(
                        ok, detail, port_open, process_alive, host, port))

            _check_api_health_async(host, port, _on_result)
            self.root.after(API_CHECK_INTERVAL_MS, self._poll_api_status)

    def refresh_api_status(self, log_on_change: bool = True) -> None:
        host = self.api_host_var.get().strip() or self.service.config.get_api_host()
        port = int(self.api_port_var.get() or self.service.config.get_api_port())
        process_alive = self.api_process is not None and self.api_process.poll() is None

        def _on_result(ok, detail, port_open):
            if self.root.winfo_exists():
                self.root.after(0, lambda: self._apply_api_status(
                    ok, detail, port_open, process_alive, host, port,
                    log_on_change=log_on_change))

        _check_api_health_async(host, port, _on_result)

    def _apply_api_status(self, healthy: bool, detail: str, port_open: bool,
                           process_alive: bool, host: str, port: int,
                           log_on_change: bool = False) -> None:
        previous = self.api_status_var.get()
        if healthy:
            status = f'● 运行中  http://{host}:{port}'
            hint = '健康检查通过，可直接打开 /docs。'
            self._set_api_buttons(True)
        elif process_alive:
            status = f'◐ 启动中  http://{host}:{port}'
            hint = 'API 进程已启动，正在等待健康检查通过。'
            self._set_api_buttons(True)
        elif port_open:
            status = f'○ 端口已占用  {host}:{port}'
            hint = '检测到端口被其他进程占用，GUI 不会强制接管。'
            self._set_api_buttons(False)
        else:
            status = f'○ 未启动  {host}:{port}'
            hint = f'当前不可访问 /health。{detail[:120] if detail else ""}'
            self._set_api_buttons(False)

        self.api_status_var.set(status)
        self.api_hint_var.set(hint)
        if log_on_change and previous != status:
            self.api_log_message(f'[API] {status}', 'api')

    def start_api_server(self) -> None:
        host = self.api_host_var.get().strip() or self.service.config.get_api_host()
        try:
            port = int(self.api_port_var.get())
        except Exception:
            messagebox.showerror('错误', 'Port 必须是整数')
            return

        if self.api_process and self.api_process.poll() is None:
            messagebox.showinfo('提示', 'API 已在运行')
            return

        def _start():
            ok, _ = _check_api_health(host, port, timeout=0.5)
            if ok or _is_port_open(host, port):
                self.root.after(0, self.refresh_api_status)
                self.root.after(0, lambda: messagebox.showwarning(
                    '提示', f'{host}:{port} 已被占用或已有服务运行'))
                return

            command = self._api_command(host, port)
            project_root = Path(__file__).resolve().parents[3]
            self.api_log_message(f'[API] 启动: {" ".join(command)}', 'api')
            self.api_process = subprocess.Popen(
                command,
                cwd=project_root,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding='utf-8',
                errors='replace',
                bufsize=1,
                env={**os.environ, 'PYTHONIOENCODING': 'utf-8'},
            )
            self.api_log_thread = threading.Thread(
                target=self._stream_api_logs, daemon=True)
            self.api_log_thread.start()

            for _ in range(8):
                time.sleep(0.35)
                ok, _ = _check_api_health(host, port)
                if ok:
                    break
            self.root.after(0, self.refresh_api_status)

        threading.Thread(target=_start, daemon=True).start()

    def stop_api_server(self, notify_when_missing: bool = True) -> None:
        if not self.api_process or self.api_process.poll() is not None:
            self.api_process = None
            self.refresh_api_status()
            if notify_when_missing:
                messagebox.showinfo('提示', '当前没有由 GUI 启动的 API 进程')
            return

        self.api_log_message('[API] 正在停止 API 进程', 'api')
        self.api_process.terminate()
        try:
            self.api_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.api_log_message('[API] terminate 超时，执行 kill', 'warn')
            self.api_process.kill()
            self.api_process.wait(timeout=5)
        finally:
            self.api_process = None
            self.refresh_api_status()

    # ── Stats ─────────────────────────────────────────────────────────────────

    def save_settings(self) -> None:
        try:
            token = self.token_var.get().strip()
            host = self.api_host_var.get().strip() or self.service.config.get_api_host()
            port = int(self.api_port_var.get() or self.service.config.get_api_port())
            if token:
                self.service.config.set_tushare_token(token)
            self.service.config.set_api_host_port(host, port)
            self.service.config.save()
            self.log('配置已保存', 'success')
        except Exception as exc:
            messagebox.showerror('错误', f'保存配置失败: {exc}')

    def run_analyze(self) -> None:
        def _run():
            self.root.after(0, lambda: self.log('开始执行全库分析优化（ANALYZE）', 'info'))
            results = self.service.analyze_all()
            self.root.after(0, lambda: self.log(f'分析优化完成：{results}', 'success'))
        threading.Thread(target=_run, daemon=True).start()

    def open_api_guide(self) -> None:
        guide_path = Path(__file__).resolve().parents[3] / 'docs' / 'API_GUIDE.md'
        webbrowser.open(guide_path.as_uri())

    def refresh_stats(self) -> None:
        def _load():
            try:
                stats = self.service.get_all_stats()
                self.root.after(0, lambda: self._apply_stats(stats))
            except Exception as exc:
                self.root.after(0, lambda: self.summary_card.config(text=f'统计失败: {exc}'))
        threading.Thread(target=_load, daemon=True).start()

    def _apply_stats(self, stats: dict) -> None:
        stock_stats  = stats.get('stocks', {})
        etf_stats    = stats.get('etfs', {})
        feature_stats = stats.get('features', {})
        event_stats  = stats.get('events', {})
        index_stats  = stats.get('indexes', {})
        forecast_stats = stats.get('index_forecasts', {})

        self.summary_card.config(
            text=(
                f'股票: {stock_stats.get("stock_count", 0)}  '
                f'ETF: {etf_stats.get("etf_count", 0)}  '
                f'概念: {feature_stats.get("concept_count", 0)}  '
                f'节假日: {event_stats.get("holiday_count", 0)}  '
                f'事件: {event_stats.get("event_count", 0)}  '
                f'指数实体: {index_stats.get("index_count", 0)}  '
                f'指数变化记录: {index_stats.get("change_count", 0)}  '
                f'预测: {forecast_stats.get("forecast_count", 0)}  '
                f'API: http://{self.api_host_var.get()}:{self.api_port_var.get()}'
            )
        )
        self.table_hint_var.set(
            f'概览已刷新：股票 {stock_stats.get("stock_count", 0)}，ETF {etf_stats.get("etf_count", 0)}，'
            f'节假日 {event_stats.get("holiday_count", 0)}，事件 {event_stats.get("event_count", 0)}，'
            f'指数 {index_stats.get("index_count", 0)}（变化 {index_stats.get("change_count", 0)} 条）'
        )
        # 更新状态树（帮助页）
        if hasattr(self, 'status_tree'):
            self.status_tree.delete(*self.status_tree.get_children())
            rows = [
                ('股票', stock_stats.get('stock_count', 0)),
                ('  日线条数', stock_stats.get('table_counts', {}).get('daily_prices', 0)),
                ('ETF', etf_stats.get('etf_count', 0)),
                ('特色概念', feature_stats.get('concept_count', 0)),
                ('节假日', event_stats.get('holiday_count', 0)),
                ('  节假日板块映射', event_stats.get('table_counts', {}).get('holiday_theme_mappings', 0)),
                ('重大事件', event_stats.get('event_count', 0)),
                ('  事件板块映射', event_stats.get('table_counts', {}).get('event_theme_mappings', 0)),
                ('指数实体', index_stats.get('index_count', 0)),
                ('  指数成分变化', index_stats.get('change_count', 0)),
                ('指数预测', forecast_stats.get('forecast_count', 0)),
                ('事件库文件大小', f'{event_stats.get("db_size_bytes", 0) // 1024} KB'),
                ('指数库文件大小', f'{index_stats.get("db_size_bytes", 0) // 1024} KB'),
            ]
            for key, val in rows:
                self.status_tree.insert('', tk.END, values=(key, val))
        # 同步更新表选项
        self.refresh_table_options()


    def refresh_table_options(self) -> None:
        """刷新数据域 / 数据表下拉选项，在 tab 初始化和手动刷新时调用"""
        try:
            options = self.service.get_database_table_options()
        except Exception:
            return
        domain_values = list(options.keys())
        self.table_domain_combo['values'] = domain_values
        domain = self.table_domain_var.get()
        if domain not in options:
            domain = domain_values[0] if domain_values else ''
            self.table_domain_var.set(domain)
        table_values = options.get(domain, [])
        self.table_name_combo['values'] = table_values
        current = self.table_name_var.get()
        if table_values:
            self.table_name_var.set(current if current in table_values else table_values[0])
        else:
            self.table_name_var.set('')

    def load_selected_table(self) -> None:
        domain = self.table_domain_var.get().strip()
        table = self.table_name_var.get().strip()
        if not domain or not table:
            messagebox.showwarning('提示', '请先选择数据域和数据表')
            return

        def _load():
            payload = self.service.get_database_table_rows(domain, table, limit=200)
            self.root.after(0, lambda: self._apply_table_rows(payload))

        threading.Thread(target=_load, daemon=True).start()

    def _apply_table_rows(self, payload: dict[str, Any]) -> None:
        columns = payload.get('columns', [])
        rows = payload.get('rows', [])
        self.data_table.delete(*self.data_table.get_children())
        self.data_table['columns'] = columns
        for col in columns:
            self.data_table.heading(col, text=col)
            self.data_table.column(col, width=140, minwidth=80, stretch=True, anchor='w')
        for row in rows:
            self.data_table.insert('', tk.END, values=[row.get(col, '') for col in columns])
        row_count = payload.get('row_count', 0)
        self.table_hint_var.set(f'{payload.get("domain")} / {payload.get("table")}：当前浏览显示最近 {row_count} 行（仅浏览限制，不影响同步范围）。')
        self.table_meta_var.set(f'表 {payload.get("table")} 共载入 {row_count} 行。可拖拽分隔条调整表格与说明区域高度。')

    def update_selected_stock(self) -> None:
        code = self.stock_update_var.get().strip() or '002594'
        self._run_in_thread(self._update_one_stock_thread, code)

    def _update_one_stock_thread(self, code: str) -> None:
        result = self.service.update_data(log=self.log, code=code)
        tag = 'warn' if result.get('cancelled') else 'success'
        self.log(
            f'单股更新完成 — {code} 状态 {result.get("status")} 日线 {result["daily_rows"]:,} 财务 {result["financial_rows"]:,} 回填 {result.get("backfilled", 0)}',
            tag,
        )

    def update_selected_etf(self) -> None:
        code = self.etf_update_var.get().strip() or '562500'
        self._run_in_thread(self._update_one_etf_thread, code)

    def _update_one_etf_thread(self, code: str) -> None:
        result = self.service.update_etf_data(log=lambda msg: self.log(f'[ETF] {msg}'), code=code)
        tag = 'warn' if result.get('cancelled') else 'success'
        # 单 ETF 更新: success=1 表示更新了新行; skipped=1 表示已最新
        if result.get('success', 0) > 0:
            status = 'success'
        elif result.get('skipped', 0) > 0:
            status = 'skipped'
        elif result.get('failed', 0) > 0:
            status = 'failed'
            tag = 'error'
        else:
            status = 'unknown'
        self.log(
            f'[ETF] 单个更新完成 — {code} 状态 {status} 日线 {result.get("daily_rows", 0):,}',
            tag,
        )

    def import_all_data(self) -> None:
        if not messagebox.askyesno('确认', '首次全量导入可能耗时很长，确定继续吗？'):
            return
        self._run_in_thread(self._import_thread, None)

    def _import_thread(self, limit) -> None:
        result = self.service.import_data(limit=limit, skip_existing=True, log=self.log)
        ok, fail, skip = result['success'], result['failed'], result['skipped']
        tag = 'warn' if result.get('cancelled') else 'success'
        suffix = '（已取消）' if result.get('cancelled') else ''
        self.log(
            f'导入完成{suffix} — 成功 {ok}  失败 {fail}  跳过 {skip}  '
            f'日线 {result["daily_rows"]:,}  财务 {result["financial_rows"]:,}',
            tag,
        )

    def update_data(self) -> None:
        self._run_in_thread(self._update_thread)

    def _update_thread(self) -> None:
        result = self.service.update_data(log=self.log)
        ok, fail, skip = result['success'], result['failed'], result['skipped']
        tag = 'warn' if result.get('cancelled') else 'success'
        suffix = '（已取消）' if result.get('cancelled') else ''
        self.log(
            f'更新完成{suffix} — 成功 {ok}  失败 {fail}  跳过 {skip}  '
            f'日线 {result["daily_rows"]:,}  财务 {result["financial_rows"]:,}  '
            f'回填 {result.get("backfilled", 0)}',
            tag,
        )

    def import_etf_data(self) -> None:
        self._run_in_thread(self._import_etf_thread, None)

    def _import_etf_thread(self, limit) -> None:
        result = self.service.import_etf_data(limit=limit, skip_existing=True, log=lambda msg: self.log(f'[ETF] {msg}'))
        reasons = result.get('failure_reasons', {})
        reason_text = f'，失败原因 {reasons}' if reasons else ''
        self.log(f'[ETF] 导入完成 — 成功 {result["success"]} 失败 {result["failed"]} 跳过 {result["skipped"]} 日线 {result["daily_rows"]:,}{reason_text}', 'success' if result['failed'] == 0 else 'warn')

    def update_etf_data(self) -> None:
        self._run_in_thread(self._update_etf_thread)

    def _update_etf_thread(self) -> None:
        result = self.service.update_etf_data(log=lambda msg: self.log(f'[ETF] {msg}'))
        self.log(f'[ETF] 更新完成 — 成功 {result["success"]} 失败 {result["failed"]} 跳过 {result["skipped"]} 日线 {result["daily_rows"]:,}', 'success')

    def detect_etf_capabilities(self) -> None:
        self._run_in_thread(self._detect_etf_thread, self.capability_etf_var.get().strip() or None)

    def _detect_etf_thread(self, sample_code: str | None) -> None:
        result = self.service.detect_etf_capabilities(sample_code=sample_code)
        self.log(f'[ETF] 检测完成 — {result["available_count"]}/{result["total_count"]} 个接口可用：{result.get("available_datasets", [])}', 'success' if result['available_count'] > 0 else 'warn')

    def sync_feature_concepts(self) -> None:
        self._run_in_thread(self._sync_feature_concepts_thread)

    def _sync_feature_concepts_thread(self) -> None:
        result = self.service.sync_feature_concepts(log=lambda msg: self.log(f'[FEATURE] {msg}'))
        self.log(f'[FEATURE] 概念基表同步完成 — 概念 {result.get("concept_rows", 0)} 成分 {result.get("member_rows", 0)}', 'success')

    def sync_feature_for_stock(self) -> None:
        self._run_in_thread(self._sync_feature_for_stock_thread)

    def _sync_feature_for_stock_thread(self) -> None:
        code = self.feature_code_var.get().strip() or '002594'
        result = self.service.sync_feature_for_stock(code, log=lambda msg: self.log(f'[FEATURE] {msg}'))
        self.log(f'[FEATURE] 单股扩展同步完成 — {code} 概念 {result.get("concept_rows", 0)} 资金流 {result.get("moneyflow_rows", 0)}', 'success')

    def sync_feature_market_wide(self) -> None:
        self._run_in_thread(self._sync_feature_market_wide_thread)

    def _sync_feature_market_wide_thread(self) -> None:
        trade_date = self.feature_trade_date_var.get().strip() or None
        result = self.service.sync_feature_market_wide(trade_date=trade_date, log=lambda msg: self.log(f'[FEATURE] {msg}'))
        self.log(f'[FEATURE] 全市场扩展同步完成 — 龙虎榜事件 {result.get("rows", 0)}', 'success')

    def sync_feature_moneyflow_all(self) -> None:
        self._run_in_thread(self._sync_feature_moneyflow_all_thread)

    def _sync_feature_moneyflow_all_thread(self) -> None:
        result = self.service.sync_feature_moneyflow_all(log=lambda msg: self.log(f'[FEATURE] {msg}'))
        self.log(
            f'[FEATURE] 全量资金流向同步完成 — 成功 {result.get("success", 0)} 跳过 {result.get("skipped", 0)} 失败 {result.get("failed", 0)} 共 {result.get("total_rows", 0)} 条',
            'success' if result.get('failed', 0) == 0 else 'warn',
        )

    def detect_feature_capabilities(self) -> None:
        self._run_in_thread(self._detect_feature_thread, self.feature_code_var.get().strip() or None)

    def _detect_feature_thread(self, sample_code: str | None) -> None:
        result = self.service.detect_feature_capabilities(sample_code=sample_code)
        pending = [item['api_name'] for item in result.get('pending', [])]
        self.log(f'[FEATURE] 检测完成 — {result["available_count"]}/{result["total_count"]} 个接口可用：{result.get("available_datasets", [])}，待确认：{pending}', 'success' if result['available_count'] > 0 else 'warn')

    def detect_capabilities(self) -> None:
        self.detect_all_capabilities()

    def detect_all_capabilities(self) -> None:
        self._run_in_thread(
            self._detect_all_thread,
            self.capability_code_var.get().strip() or None,
            self.capability_etf_var.get().strip() or None,
        )

    def _detect_all_thread(self, stock_code: str | None, etf_code: str | None) -> None:
        stock_result = self.service.detect_capabilities(sample_code=stock_code)
        etf_result = self.service.detect_etf_capabilities(sample_code=etf_code)
        feature_result = self.service.detect_feature_capabilities(sample_code=stock_code)

        def _fill_tree(tree, rows):
            for item in tree.get_children():
                tree.delete(item)
            for row in rows:
                tag = 'ok' if row.get('available') else 'fail'
                tree.insert(
                    '', tk.END,
                    values=(
                        row.get('api_name', ''),
                        row.get('display_name', ''),
                        '✓' if row.get('available') else '✗',
                        '是' if row.get('empty') else '否',
                        row.get('rows', 0),
                        row.get('error', ''),
                    ),
                    tags=(tag,),
                )

        def apply_results() -> None:
            _fill_tree(self.capability_trees['stock'], stock_result['results'])
            _fill_tree(self.capability_trees['etf'], etf_result['results'])
            _fill_tree(self.capability_trees['feature'], feature_result['results'])
            _fill_tree(self.capability_trees['pending'], feature_result.get('pending', []))
            self.capability_summary_var.set(
                f'股票 {stock_result["available_count"]}/{stock_result["total_count"]}，'
                f'ETF {etf_result["available_count"]}/{etf_result["total_count"]}，'
                f'扩展 {feature_result["available_count"]}/{feature_result["total_count"]}'
            )

        self.root.after(0, apply_results)
        self.log(
            f'统一检测完成 — 股票 {stock_result["available_count"]}/{stock_result["total_count"]}，ETF {etf_result["available_count"]}/{etf_result["total_count"]}，扩展 {feature_result["available_count"]}/{feature_result["total_count"]}',
            'success',
        )

    def clear_data(self) -> None:
        if not messagebox.askyesno('确认', '确定要清空所有数据吗？'):
            return
        try:
            self.service.clear_data()
            self.log('数据库已清空', 'warn')
            self.refresh_stats()
        except Exception as exc:
            messagebox.showerror('错误', f'清空失败: {exc}')

    def on_stock_changed(self) -> None:
        pass  # 旧方法保留避免引用错误，当前按库查看走 load_selected_table

    def _on_quick_lookup(self) -> None:
        code = self.quick_code_var.get().strip()
        dtype = self.quick_type_var.get()
        if not code:
            messagebox.showwarning('提示', '请输入股票或 ETF 代码')
            return

        def _load():
            try:
                if dtype == '日线':
                    rows = self.service.get_stock_daily(code, limit=200)
                    cols = ['code', 'date', 'open', 'high', 'low', 'close', 'volume', 'amount']
                    payload = {'domain': 'stocks', 'table': f'daily/{code}',
                               'columns': cols, 'rows': rows, 'row_count': len(rows)}
                elif dtype == '财务概览':
                    result = self.service.get_stock_financials(code, limit=20)
                    fina = result.get('financials', {})
                    all_rows = []
                    for k, v in fina.items():
                        for row in (v or []):
                            row = dict(row, _table=k)
                            all_rows.append(row)
                    cols = list(all_rows[0].keys()) if all_rows else []
                    payload = {'domain': 'stocks', 'table': f'financials/{code}',
                               'columns': cols, 'rows': all_rows, 'row_count': len(all_rows)}
                else:  # ETF日线
                    rows = self.service.get_etf_daily(code, limit=200)
                    cols = ['code', 'date', 'open', 'high', 'low', 'close', 'volume', 'amount']
                    payload = {'domain': 'etfs', 'table': f'etf_daily/{code}',
                               'columns': cols, 'rows': rows, 'row_count': len(rows)}
                self.root.after(0, lambda: self._apply_table_rows(payload))
            except Exception as exc:
                self.root.after(0, lambda: self.table_hint_var.set(f'查询失败: {exc}'))

        threading.Thread(target=_load, daemon=True).start()

    def _apply_stock_data(self, code: str, prices: list, financials: dict) -> None:
        # 保留供历史引用，当前不再由 UI 直接触发
        pass

    def open_help_page(self) -> None:
        help_path = Path(__file__).resolve().parents[3] / 'docs' / 'help.html'
        webbrowser.open(help_path.as_uri())

    def open_api_docs(self) -> None:
        host = self.api_host_var.get().strip() or self.service.config.get_api_host()
        port = int(self.api_port_var.get() or self.service.config.get_api_port())
        webbrowser.open(f'http://{host}:{port}/docs')

    def seed_event_data(self) -> None:
        result = self.service.seed_event_data()
        self.log(f'已初始化事件数据：节假日 {result.get("holidays", 0)}，映射 {result.get("holiday_mappings", 0)}，赛事 {result.get("events", 0)}', 'success')
        self.refresh_stats()

    def open_index_doc(self) -> None:
        path = Path(__file__).resolve().parents[3] / 'docs' / 'INDEX_CONSTITUENT_CHANGES.md'
        webbrowser.open(path.as_uri())

    def open_broker_doc(self) -> None:
        path = Path(__file__).resolve().parents[3] / 'docs' / 'BROKER_INDEX_FORECASTS.md'
        webbrowser.open(path.as_uri())

    def _on_close(self) -> None:
        if self.api_process and self.api_process.poll() is None:
            if messagebox.askyesno('确认', '检测到 GUI 启动的 API 仍在运行，退出时一并停止吗？'):
                self.stop_api_server(notify_when_missing=False)
        self.root.destroy()


def main() -> None:
    root = tk.Tk()
    SqlToolGUI(root)
    root.mainloop()


if __name__ == '__main__':
    main()
