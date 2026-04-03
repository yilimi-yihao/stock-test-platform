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
from pathlib import Path
from tkinter import messagebox, scrolledtext, ttk

from sql_tool.service import SqlToolService


API_CHECK_INTERVAL_MS = 1500


def _is_port_open(host: str, port: int) -> bool:
    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.settimeout(0.5)
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


class SqlToolGUI:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title('股票数据库管理工具')
        self.root.geometry('1120x820')
        self.root.configure(bg='#f4f7fb')

        self.service = SqlToolService()
        self.running = False
        self.log_queue: queue.Queue[str] = queue.Queue()
        self.action_buttons: list[tk.Button] = []
        self.api_process: subprocess.Popen[str] | None = None
        self.api_log_thread: threading.Thread | None = None

        self.api_host_var = tk.StringVar(value=self.service.config.get_api_host())
        self.api_port_var = tk.IntVar(value=self.service.config.get_api_port())
        self.api_status_var = tk.StringVar(value='未启动')
        self.api_hint_var = tk.StringVar(value='可在 GUI 内启动本地 API 并查看运行状态。')

        self._setup_style()
        self._build_ui()
        self.refresh_stats()
        self.refresh_api_status(log_on_change=False)
        self.root.after(150, self._drain_log_queue)
        self.root.after(API_CHECK_INTERVAL_MS, self._poll_api_status)
        self.root.protocol('WM_DELETE_WINDOW', self._on_close)

    def _setup_style(self) -> None:
        style = ttk.Style()
        style.theme_use('clam')
        style.configure('TFrame', background='#f4f7fb')
        style.configure('Card.TFrame', background='#ffffff')
        style.configure('TLabel', background='#f4f7fb', foreground='#243447')
        style.configure('Card.TLabel', background='#ffffff', foreground='#243447')
        style.configure('Title.TLabel', font=('Microsoft YaHei', 18, 'bold'), foreground='#14213d')
        style.configure('Heading.TLabel', font=('Microsoft YaHei', 11, 'bold'), foreground='#243447')
        style.configure('Subtle.TLabel', background='#f4f7fb', foreground='#667085')
        style.configure('CardSubtle.TLabel', background='#ffffff', foreground='#667085')
        style.configure('Treeview', rowheight=26)
        style.configure('TLabelframe', background='#ffffff')
        style.configure('TLabelframe.Label', background='#ffffff', foreground='#243447')
        style.configure('TNotebook', background='#f4f7fb', borderwidth=0)
        style.configure('TNotebook.Tab', padding=(14, 8), font=('Microsoft YaHei', 10))

    def _build_ui(self) -> None:
        main_frame = ttk.Frame(self.root, padding=18)
        main_frame.pack(fill=tk.BOTH, expand=True)

        header = ttk.Frame(main_frame)
        header.pack(fill=tk.X, pady=(0, 12))
        ttk.Label(header, text='股票数据库管理', style='Title.TLabel').pack(anchor='w')
        ttk.Label(header, text='Tushare 导入、数据库浏览与本地 API 管理', style='Subtle.TLabel').pack(anchor='w', pady=(4, 0))

        self.summary_card = tk.Label(
            main_frame,
            text='',
            justify=tk.LEFT,
            bg='#ffffff',
            fg='#243447',
            font=('Microsoft YaHei', 10),
            padx=14,
            pady=12,
            relief=tk.FLAT,
            bd=1,
        )
        self.summary_card.pack(fill=tk.X, pady=(0, 12))

        notebook = ttk.Notebook(main_frame)
        notebook.pack(fill=tk.BOTH, expand=True, pady=(0, 12))

        data_frame = ttk.Frame(notebook)
        notebook.add(data_frame, text='数据管理')
        self._build_data_tab(data_frame)

        api_frame = ttk.Frame(notebook)
        notebook.add(api_frame, text='API 服务')
        self._build_api_tab(api_frame)

        status_frame = ttk.Frame(notebook)
        notebook.add(status_frame, text='数据库状态')
        self._build_status_tab(status_frame)

        capability_frame = ttk.Frame(notebook)
        notebook.add(capability_frame, text='接口检测')
        self._build_capability_tab(capability_frame)

        analysis_frame = ttk.Frame(notebook)
        notebook.add(analysis_frame, text='数据浏览')
        self._build_analysis_tab(analysis_frame)

        help_frame = ttk.Frame(notebook)
        notebook.add(help_frame, text='帮助')
        self._build_help_tab(help_frame)

        ttk.Label(main_frame, text='运行日志', style='Heading.TLabel').pack(anchor='w', pady=(4, 6))
        self.log_text = scrolledtext.ScrolledText(
            main_frame,
            height=10,
            width=100,
            font=('Consolas', 9),
            bg='#0f172a',
            fg='#e2e8f0',
            insertbackground='#e2e8f0',
            relief=tk.FLAT,
            bd=1,
        )
        self.log_text.pack(fill=tk.BOTH, expand=False)

    def _build_data_tab(self, parent: ttk.Frame) -> None:
        frame = ttk.Frame(parent, padding=16)
        frame.pack(fill=tk.BOTH, expand=True)

        control_frame = ttk.LabelFrame(frame, text='数据任务', padding=12)
        control_frame.pack(fill=tk.X, pady=(0, 12))

        ttk.Label(control_frame, text='导入数量:').grid(row=0, column=0, sticky='w', padx=(0, 8), pady=4)
        self.limit_var = tk.IntVar(value=100)
        ttk.Spinbox(control_frame, from_=1, to=10000, textvariable=self.limit_var, width=12).grid(row=0, column=1, sticky='w', pady=4)

        btn_frame = ttk.Frame(control_frame)
        btn_frame.grid(row=1, column=0, columnspan=4, sticky='w', pady=(10, 0))
        self._create_button(btn_frame, '导入前 N 只', self.import_data, '#2563eb')
        self._create_button(btn_frame, '导入全部 A 股', self.import_all_data, '#1d4ed8')
        self._create_button(btn_frame, '增量更新', self.update_data, '#0f766e')
        self._create_button(btn_frame, '刷新统计', self.refresh_stats, '#475569')
        self._create_button(btn_frame, '清除数据', self.clear_data, '#dc2626')

        self.progress = ttk.Progressbar(frame, mode='indeterminate')
        self.progress.pack(fill=tk.X, pady=(0, 12))

        info_card = tk.Label(
            frame,
            text=(
                '建议顺序：先检测接口权限，再小规模导入验证，最后做全量导入或日常更新。\n'
                '如需给其他工程供数，可在“API 服务”页直接启动本地接口。'
            ),
            justify=tk.LEFT,
            bg='#ffffff',
            fg='#475467',
            font=('Microsoft YaHei', 10),
            padx=14,
            pady=12,
            relief=tk.FLAT,
            bd=1,
        )
        info_card.pack(fill=tk.X)

    def _build_api_tab(self, parent: ttk.Frame) -> None:
        frame = ttk.Frame(parent, padding=16)
        frame.pack(fill=tk.BOTH, expand=True)

        control = ttk.LabelFrame(frame, text='本地 API 控制', padding=12)
        control.pack(fill=tk.X, pady=(0, 12))

        ttk.Label(control, text='Host:').grid(row=0, column=0, sticky='w')
        ttk.Entry(control, textvariable=self.api_host_var, width=18).grid(row=0, column=1, sticky='w', padx=(8, 16))
        ttk.Label(control, text='Port:').grid(row=0, column=2, sticky='w')
        ttk.Entry(control, textvariable=self.api_port_var, width=10).grid(row=0, column=3, sticky='w', padx=(8, 16))

        btn_frame = ttk.Frame(control)
        btn_frame.grid(row=1, column=0, columnspan=4, sticky='w', pady=(12, 0))
        self.api_start_button = self._create_button(btn_frame, '启动 API', self.start_api_server, '#2563eb', track=False)
        self.api_stop_button = self._create_button(btn_frame, '停止 API', self.stop_api_server, '#dc2626', track=False)
        self._create_button(btn_frame, '检查状态', self.refresh_api_status, '#475569', track=False)
        self._create_button(btn_frame, '打开 API 文档', self.open_api_docs, '#0f766e', track=False)

        status_card = tk.Label(
            frame,
            textvariable=self.api_status_var,
            justify=tk.LEFT,
            bg='#ffffff',
            fg='#111827',
            font=('Microsoft YaHei', 11, 'bold'),
            padx=14,
            pady=12,
            relief=tk.FLAT,
            bd=1,
        )
        status_card.pack(fill=tk.X, pady=(0, 8))

        ttk.Label(frame, textvariable=self.api_hint_var, style='Subtle.TLabel').pack(anchor='w', pady=(0, 12))

        api_help = tk.Label(
            frame,
            text='启动后可访问 /health、/stats、/stocks、/stocks/{code}/daily、/stocks/{code}/financials、/capabilities 与 /docs。',
            justify=tk.LEFT,
            bg='#ffffff',
            fg='#475467',
            font=('Microsoft YaHei', 10),
            padx=14,
            pady=12,
            relief=tk.FLAT,
            bd=1,
        )
        api_help.pack(fill=tk.X)

    def _build_status_tab(self, parent: ttk.Frame) -> None:
        frame = ttk.Frame(parent, padding=16)
        frame.pack(fill=tk.BOTH, expand=True)

        columns = ('key', 'value')
        self.status_tree = ttk.Treeview(frame, columns=columns, show='headings', height=12)
        self.status_tree.heading('key', text='项目')
        self.status_tree.heading('value', text='值')
        self.status_tree.column('key', width=220, anchor='w')
        self.status_tree.column('value', width=760, anchor='w')
        self.status_tree.pack(fill=tk.BOTH, expand=True)

    def _build_capability_tab(self, parent: ttk.Frame) -> None:
        frame = ttk.Frame(parent, padding=16)
        frame.pack(fill=tk.BOTH, expand=True)

        top = ttk.Frame(frame)
        top.pack(fill=tk.X, pady=(0, 10))
        ttk.Label(top, text='样本股票代码:').pack(side=tk.LEFT)
        self.capability_code_var = tk.StringVar(value=self.service.config.get_sample_stock())
        ttk.Entry(top, textvariable=self.capability_code_var, width=14).pack(side=tk.LEFT, padx=8)
        self._create_button(top, '检测接口能力', self.detect_capabilities, '#7c3aed')

        columns = ('api_name', 'display_name', 'available', 'empty', 'rows', 'error')
        self.capability_tree = ttk.Treeview(frame, columns=columns, show='headings', height=14)
        headings = {
            'api_name': '接口名',
            'display_name': '用途',
            'available': '可用',
            'empty': '空数据',
            'rows': '返回行数',
            'error': '错误信息',
        }
        widths = {'api_name': 140, 'display_name': 160, 'available': 80, 'empty': 80, 'rows': 100, 'error': 500}
        for key in columns:
            self.capability_tree.heading(key, text=headings[key])
            self.capability_tree.column(key, width=widths[key], anchor='center' if key in {'available', 'empty', 'rows'} else 'w')
        self.capability_tree.pack(fill=tk.BOTH, expand=True)

    def _build_analysis_tab(self, parent: ttk.Frame) -> None:
        frame = ttk.Frame(parent, padding=16)
        frame.pack(fill=tk.BOTH, expand=True)

        select_frame = ttk.Frame(frame)
        select_frame.pack(fill=tk.X, pady=(0, 12))
        ttk.Label(select_frame, text='选择股票:').pack(side=tk.LEFT, padx=(0, 8))
        self.stock_combo = ttk.Combobox(select_frame, width=30, state='readonly')
        self.stock_combo.pack(side=tk.LEFT)
        self.stock_combo.bind('<<ComboboxSelected>>', lambda e: self.on_stock_changed())

        ttk.Label(frame, text='日线数据（最近 20 条）', style='Heading.TLabel').pack(anchor='w', pady=(8, 5))
        self.daily_tree = ttk.Treeview(
            frame,
            columns=('date', 'open', 'close', 'high', 'low', 'volume', 'amount'),
            height=8,
            show='headings',
        )
        for key, title, width in [
            ('date', '日期', 100),
            ('open', '开盘', 80),
            ('close', '收盘', 80),
            ('high', '最高', 80),
            ('low', '最低', 80),
            ('volume', '成交量', 120),
            ('amount', '成交额', 120),
        ]:
            self.daily_tree.heading(key, text=title)
            self.daily_tree.column(key, width=width, anchor='center')
        self.daily_tree.pack(fill=tk.BOTH, expand=True, pady=(0, 12))

        ttk.Label(frame, text='财务数据（最近 8 期）', style='Heading.TLabel').pack(anchor='w', pady=(8, 5))
        self.fin_tree = ttk.Treeview(
            frame,
            columns=('period', 'revenue', 'net_profit', 'roe', 'margin', 'debt'),
            height=6,
            show='headings',
        )
        for key, title, width in [
            ('period', '报告期', 100),
            ('revenue', '营收(亿)', 120),
            ('net_profit', '净利润(亿)', 120),
            ('roe', 'ROE(%)', 90),
            ('margin', '毛利率(%)', 100),
            ('debt', '负债率(%)', 100),
        ]:
            self.fin_tree.heading(key, text=title)
            self.fin_tree.column(key, width=width, anchor='center')
        self.fin_tree.pack(fill=tk.BOTH, expand=True)

    def _build_help_tab(self, parent: ttk.Frame) -> None:
        frame = ttk.Frame(parent, padding=16)
        frame.pack(fill=tk.BOTH, expand=True)

        ttk.Label(frame, text='帮助与文档', style='Heading.TLabel').pack(anchor='w', pady=(0, 10))
        ttk.Label(frame, text='帮助页会同步当前代码能力；API 文档依赖本地 API 已启动。', style='Subtle.TLabel').pack(anchor='w', pady=(0, 16))

        btns = ttk.Frame(frame)
        btns.pack(fill=tk.X, pady=(0, 12))
        self._create_button(btns, '打开帮助页', self.open_help_page, '#475569', track=False)
        self._create_button(btns, '打开 API 文档', self.open_api_docs, '#0f766e', track=False)

        help_box = scrolledtext.ScrolledText(frame, height=18, font=('Microsoft YaHei', 10), relief=tk.FLAT, bd=1)
        help_box.pack(fill=tk.BOTH, expand=True)
        help_box.insert(
            tk.END,
            '推荐使用顺序：\n'
            '1. 先在“接口检测”页确认 token 可访问的接口。\n'
            '2. 在“数据管理”页先导入少量股票验证。\n'
            '3. 需要给其他工程供数时，在“API 服务”页启动本地 API。\n'
            '4. 启动后可直接打开 `/docs` 查看自动生成文档。\n',
        )
        help_box.configure(state='disabled')

    def _create_button(self, parent, text: str, command, color: str, track: bool = True) -> tk.Button:
        btn = tk.Button(
            parent,
            text=text,
            command=command,
            bg=color,
            fg='white',
            font=('Microsoft YaHei', 10, 'bold'),
            relief=tk.FLAT,
            padx=14,
            pady=8,
            cursor='hand2',
        )
        btn.pack(side=tk.LEFT, padx=5)
        if track:
            self.action_buttons.append(btn)
        return btn

    def _set_running(self, running: bool) -> None:
        self.running = running
        for button in self.action_buttons:
            button.config(state=tk.DISABLED if running else tk.NORMAL)
        if running:
            self.progress.start()
        else:
            self.progress.stop()

    def _set_api_buttons(self, is_running: bool) -> None:
        self.api_start_button.config(state=tk.DISABLED if is_running else tk.NORMAL)
        self.api_stop_button.config(state=tk.NORMAL if is_running else tk.DISABLED)

    def log(self, msg: str) -> None:
        self.log_queue.put(msg)

    def _drain_log_queue(self) -> None:
        while not self.log_queue.empty():
            msg = self.log_queue.get_nowait()
            self.log_text.insert(tk.END, msg + '\n')
            self.log_text.see(tk.END)
        self.root.after(150, self._drain_log_queue)

    def _run_in_thread(self, func, *args) -> None:
        if self.running:
            messagebox.showwarning('提示', '已有任务运行中')
            return
        self.log_text.delete(1.0, tk.END)
        self._set_running(True)
        threading.Thread(target=self._thread_wrapper, args=(func, *args), daemon=True).start()

    def _thread_wrapper(self, func, *args) -> None:
        try:
            func(*args)
        except Exception as exc:
            self.log(f'错误: {exc}')
        finally:
            self.root.after(0, lambda: self._set_running(False))
            self.root.after(0, self.refresh_stats)

    def _api_command(self, host: str, port: int) -> list[str]:
        project_root = Path(__file__).resolve().parents[2]
        return [sys.executable, str(project_root / 'main.py'), 'api', '--host', host, '--port', str(port)]

    def _stream_api_logs(self) -> None:
        if not self.api_process or not self.api_process.stdout:
            return
        for line in self.api_process.stdout:
            self.log(f'[API] {line.rstrip()}')
        code = self.api_process.poll()
        self.log(f'[API] 进程已退出，返回码: {code}')
        self.api_process = None
        self.root.after(0, lambda: self.refresh_api_status(log_on_change=False))

    def _poll_api_status(self) -> None:
        if self.root.winfo_exists():
            self.refresh_api_status(log_on_change=False)
            self.root.after(API_CHECK_INTERVAL_MS, self._poll_api_status)

    def refresh_api_status(self, log_on_change: bool = True) -> None:
        host = self.api_host_var.get().strip() or self.service.config.get_api_host()
        port = int(self.api_port_var.get() or self.service.config.get_api_port())
        healthy, detail = _check_api_health(host, port, timeout=0.6)
        process_alive = self.api_process is not None and self.api_process.poll() is None
        port_open = _is_port_open(host, port)

        previous = self.api_status_var.get()
        if healthy:
            status = f'运行中： http://{host}:{port}'
            hint = '健康检查通过，可直接打开 /docs。'
            self._set_api_buttons(True)
        elif process_alive:
            status = f'启动中： http://{host}:{port}'
            hint = 'API 进程已启动，正在等待健康检查通过。'
            self._set_api_buttons(True)
        elif port_open:
            status = f'端口已占用： {host}:{port}'
            hint = '检测到端口被其他进程占用，GUI 不会强制接管。'
            self._set_api_buttons(False)
        else:
            status = f'未启动： {host}:{port}'
            hint = f'当前不可访问 /health。{detail[:120] if detail else ""}'
            self._set_api_buttons(False)

        self.api_status_var.set(status)
        self.api_hint_var.set(hint)
        if log_on_change and previous != status:
            self.log(f'[API] {status}')

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

        healthy, _ = _check_api_health(host, port)
        if healthy or _is_port_open(host, port):
            self.refresh_api_status()
            messagebox.showwarning('提示', f'{host}:{port} 已被占用或已有服务运行')
            return

        command = self._api_command(host, port)
        project_root = Path(__file__).resolve().parents[2]
        self.log(f'[API] 启动命令: {" ".join(command)}')
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
        self.api_log_thread = threading.Thread(target=self._stream_api_logs, daemon=True)
        self.api_log_thread.start()

        for _ in range(8):
            time.sleep(0.35)
            healthy, _ = _check_api_health(host, port)
            if healthy:
                break
        self.refresh_api_status()

    def stop_api_server(self, notify_when_missing: bool = True) -> None:
        if not self.api_process or self.api_process.poll() is not None:
            self.api_process = None
            self.refresh_api_status()
            if notify_when_missing:
                messagebox.showinfo('提示', '当前没有由 GUI 启动的 API 进程')
            return

        self.log('[API] 正在停止 API 进程')
        self.api_process.terminate()
        try:
            self.api_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.log('[API] terminate 超时，执行 kill')
            self.api_process.kill()
            self.api_process.wait(timeout=5)
        finally:
            self.api_process = None
            self.refresh_api_status()

    def refresh_stats(self) -> None:
        try:
            stats = self.service.get_stats()
            stock_count = stats.get('stock_count', 0)
            price_count = stats.get('price_count', 0)
            size_mb = stats.get('db_size_bytes', 0) / 1024 / 1024
            date_range = stats.get('date_range', {})
            start = date_range.get('start', 'N/A')
            end = date_range.get('end', 'N/A')
            self.summary_card.config(
                text=(
                    f'股票数: {stock_count}    日线条数: {price_count}    数据库: {size_mb:.2f} MB\n'
                    f'日期范围: {start} ~ {end}    API 地址: http://{self.api_host_var.get()}:{self.api_port_var.get()}'
                )
            )

            for item in self.status_tree.get_children():
                self.status_tree.delete(item)
            rows = [
                ('数据库路径', stats.get('db_path', 'N/A')),
                ('数据库存在', str(stats.get('db_exists', False))),
                ('数据库大小(字节)', str(stats.get('db_size_bytes', 0))),
                ('股票数', str(stock_count)),
                ('日线条数', str(price_count)),
                ('各表行数', str(stats.get('table_counts', {}))),
                ('最近股票更新时间', str(stats.get('latest_stock_update', 'N/A'))),
                ('日期范围', f'{start} ~ {end}'),
            ]
            for row in rows:
                self.status_tree.insert('', tk.END, values=row)

            stocks = self.service.get_stocks(limit=5000)
            self.stock_combo['values'] = [f"{stock['code']} {stock['name']}" for stock in stocks]
        except Exception as exc:
            self.summary_card.config(text=f'统计失败: {exc}')

    def import_data(self) -> None:
        self._run_in_thread(self._import_thread, self.limit_var.get())

    def import_all_data(self) -> None:
        if not messagebox.askyesno('确认', '首次全量导入可能耗时很长，确定继续吗？'):
            return
        self._run_in_thread(self._import_thread, None)

    def _import_thread(self, limit) -> None:
        result = self.service.import_data(limit=limit, skip_existing=True, log=self.log)
        self.log(f'导入完成: {result}')

    def update_data(self) -> None:
        self._run_in_thread(self._update_thread)

    def _update_thread(self) -> None:
        result = self.service.update_data(log=self.log)
        self.log(f'更新完成: {result}')

    def detect_capabilities(self) -> None:
        self._run_in_thread(self._detect_thread, self.capability_code_var.get().strip() or None)

    def _detect_thread(self, sample_code: str | None) -> None:
        result = self.service.detect_capabilities(sample_code=sample_code)

        def apply_results() -> None:
            for item in self.capability_tree.get_children():
                self.capability_tree.delete(item)
            for row in result['results']:
                self.capability_tree.insert(
                    '',
                    tk.END,
                    values=(
                        row['api_name'],
                        row['display_name'],
                        '是' if row['available'] else '否',
                        '是' if row['empty'] else '否',
                        row['rows'],
                        row['error'],
                    ),
                )

        self.root.after(0, apply_results)
        self.log(f"检测完成: {result['available_count']}/{result['total_count']} 个接口可用")

    def clear_data(self) -> None:
        if not messagebox.askyesno('确认', '确定要清空所有数据吗？'):
            return
        try:
            self.service.clear_data()
            self.log('数据库已清空')
            self.refresh_stats()
        except Exception as exc:
            messagebox.showerror('错误', f'清空失败: {exc}')

    def on_stock_changed(self) -> None:
        selection = self.stock_combo.get()
        if not selection:
            return
        code = selection.split()[0]

        prices = self.service.get_stock_daily(code, limit=20)
        for item in self.daily_tree.get_children():
            self.daily_tree.delete(item)
        for price in prices:
            self.daily_tree.insert(
                '',
                tk.END,
                values=(
                    price.get('date', ''),
                    f"{price.get('open', 0):.2f}",
                    f"{price.get('close', 0):.2f}",
                    f"{price.get('high', 0):.2f}",
                    f"{price.get('low', 0):.2f}",
                    f"{price.get('volume', 0):,}",
                    f"{price.get('amount', 0):,.2f}",
                ),
            )

        financials = self.service.get_stock_financials(code, limit=8)['financials']
        income = financials.get('income', [])
        fina = financials.get('fina_indicator', [])
        for item in self.fin_tree.get_children():
            self.fin_tree.delete(item)
        for index, item in enumerate(income):
            fina_item = fina[index] if index < len(fina) else {}
            self.fin_tree.insert(
                '',
                tk.END,
                values=(
                    item.get('end_date', ''),
                    f"{(item.get('revenue') or 0) / 1e8:.2f}",
                    f"{(item.get('net_profit') or 0) / 1e8:.2f}",
                    f"{fina_item.get('roe', 0) or 0:.2f}",
                    f"{fina_item.get('gross_margin', 0) or 0:.2f}",
                    f"{fina_item.get('debt_to_assets', 0) or 0:.2f}",
                ),
            )

    def open_help_page(self) -> None:
        help_path = Path(__file__).resolve().parents[2] / 'docs' / 'help.html'
        webbrowser.open(help_path.as_uri())

    def open_api_docs(self) -> None:
        host = self.api_host_var.get().strip() or self.service.config.get_api_host()
        port = int(self.api_port_var.get() or self.service.config.get_api_port())
        webbrowser.open(f'http://{host}:{port}/docs')

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
