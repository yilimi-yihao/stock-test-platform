"""
AI 辅助事件提取工具 (demo)
============================
独立 GUI 工具，功能：
  1. 调用 AI API（支持 DeepSeek/OpenAI/Kimi/ChatGLM/MiniMax）生成结构化事件 JSON
  2. 写入 sql-tool event_calendar.db（通过本地 API）
  3. 浏览当前事件库，支持筛选，可查看 relevance 评分
  4. 单条或全量删除

配置文件：sandbox/event_ai_tool/config.toml
运行：python sandbox/event_ai_tool/event_ai_tool.py
（需先启动 sql-tool 本地 API: python main.py api）
"""
from __future__ import annotations

import json
import time
import re
import threading
import tomllib
import tkinter as tk
from datetime import datetime, date, timedelta
from pathlib import Path
from tkinter import messagebox, scrolledtext, ttk

import httpx
from openai import OpenAI

CONFIG_PATH = Path(__file__).parent / "config.toml"
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

TODAY = date.today()


# ─── 配置加载 ──────────────────────────────────────────────────────────────────
def load_config() -> dict:
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "rb") as f:
            return tomllib.load(f)
    return {}

CFG = load_config()
MODELS: dict[str, dict] = CFG.get("models", {})
DEFAULT_MODEL_KEY = CFG.get("default_model", "deepseek")
DEFAULT_API_BASE = CFG.get("sqltool", {}).get("api_base", "http://127.0.0.1:8000")
MAX_MAPPINGS = CFG.get("prompt", {}).get("max_mappings", 5)
TEMPERATURE = float(CFG.get("prompt", {}).get("temperature", 0.2))


# ─── 行业列表（来自 a_share.db stocks.industry）────────────────────────────────
VALID_INDUSTRIES = [
    "IT设备","专用机械","中成药","乳制品","互联网","仓储物流","供气供热","保险",
    "元器件","全国地产","公共交通","公路","其他商业","其他建材","农业综合","农用机械",
    "农药化肥","出版业","化学制药","化工原料","化工机械","化纤","区域地产","医疗保健",
    "医药商业","半导体","商品城","商贸代理","啤酒","园区开发","塑料","多元金融",
    "家居用品","家用电器","小金属","工程机械","广告包装","建筑工程","影视音像",
    "房产服务","批发业","摩托车","文教休闲","新型电力","旅游景点","旅游服务","日用化工",
    "普钢","服饰","机场","机床制造","机械基件","林业","染料涂料","橡胶","水力发电",
    "水务","水泥","水运","汽车整车","汽车服务","汽车配件","渔业","港口","火力发电",
    "焦炭加工","煤炭开采","特种钢","环境保护","玻璃","生物制药","电信运营","电器仪表",
    "电器连锁","电气设备","白酒","百货","石油加工","石油开采","石油贸易","矿物制品",
    "种植业","空运","红黄酒","纺织","纺织机械","综合类","航空","船舶","装修装饰",
    "证券","超市连锁","路桥","软件服务","软饮料","轻工机械","运输设备","通信设备",
    "造纸","酒店餐饮","钢加工","铁路","铅锌","铜","铝","银行","陶瓷","食品","饲料","黄金",
]
INDUSTRY_ALIASES = {
    # 日用化工
    "化妆品": "日用化工", "美妆": "日用化工", "护肤品": "日用化工", "彩妆": "日用化工",
    "洗护": "日用化工", "洗发水": "日用化工", "卫生用品": "日用化工", "日化": "日用化工",
    # 仓储物流
    "物流": "仓储物流", "快递": "仓储物流", "冷链": "仓储物流", "供应链": "仓储物流",
    "仓储": "仓储物流", "配送": "仓储物流",
    # 旅游服务 / 旅游景点
    "旅游": "旅游服务", "景区": "旅游景点", "景点": "旅游景点", "文旅": "旅游服务",
    "出行": "旅游服务", "民宿": "酒店餐饮",
    # 酒店餐饮
    "餐饮": "酒店餐饮", "酒店": "酒店餐饮", "住宿": "酒店餐饮", "饭店": "酒店餐饮",
    "餐厅": "酒店餐饮", "外卖": "酒店餐饮",
    # 航空 / 机场
    "民航": "航空", "航运": "航空", "飞机": "航空", "空运": "航空",
    # 食品
    "零食": "食品", "粮食": "食品", "粮油": "食品", "肉类": "食品", "食品饮料": "食品",
    "烘焙": "食品", "糖果": "食品", "速食": "食品", "预制菜": "食品",
    # 白酒
    "酿酒": "白酒", "白酒行业": "白酒", "烈酒": "白酒",
    # 啤酒
    "啤酒行业": "啤酒",
    # 红黄酒
    "黄酒": "红黄酒", "红酒": "红黄酒", "葡萄酒": "红黄酒", "米酒": "红黄酒",
    # 软饮料
    "饮料": "软饮料", "碳酸饮料": "软饮料", "果汁": "软饮料", "茶饮": "软饮料",
    "功能饮料": "软饮料", "矿泉水": "软饮料",
    # 影视音像
    "影视": "影视音像", "传媒": "影视音像", "电影": "影视音像", "游戏": "影视音像",
    "动漫": "影视音像", "网络视频": "影视音像", "流媒体": "影视音像", "院线": "影视音像",
    "综艺": "影视音像", "媒体": "影视音像",
    # 文教休闲
    "教育": "文教休闲", "培训": "文教休闲", "体育": "文教休闲", "健身": "文教休闲",
    "休闲娱乐": "文教休闲", "玩具": "文教休闲", "户外运动": "文教休闲", "赛事": "文教休闲",
    "电竞": "文教休闲", "出版": "文教休闲",
    # 服饰
    "服装": "服饰", "鞋服": "服饰", "运动服": "服饰", "童装": "服饰", "奢侈品": "服饰",
    "纺织品": "服饰",
    # 百货 / 超市连锁
    "零售": "百货", "商超": "超市连锁", "超市": "超市连锁", "便利店": "超市连锁",
    "商场": "百货", "购物中心": "百货", "电商": "百货",
    # 医疗保健
    "医疗": "医疗保健", "医药": "医疗保健", "保健": "医疗保健", "养老": "医疗保健",
    "医院": "医疗保健", "诊断": "医疗保健", "医美": "医疗保健",
    # 化学制药 / 生物制药
    "制药": "化学制药", "西药": "化学制药", "仿制药": "化学制药",
    "生物药": "生物制药", "疫苗": "生物制药", "基因": "生物制药",
    # 银行 / 证券
    "金融": "银行", "商业银行": "银行", "股份行": "银行",
    "券商": "证券", "基金": "证券", "投行": "证券", "资管": "证券",
    # 半导体 / 通信设备 / 电气设备 / IT设备 / 元器件
    "芯片": "半导体", "集成电路": "半导体", "晶圆": "半导体",
    "5G": "通信设备", "光通信": "通信设备", "基站": "通信设备",
    "新能源": "电气设备", "储能": "电气设备", "充电桩": "电气设备",
    "电子": "元器件", "被动元件": "元器件", "电容": "元器件",
    # 新型电力
    "光伏": "新型电力", "风电": "新型电力", "太阳能": "新型电力", "绿电": "新型电力",
    "核电": "新型电力", "氢能": "新型电力",
    # 汽车整车 / 汽车配件
    "新能源车": "汽车整车", "电动车": "汽车整车", "智能汽车": "汽车整车",
    "汽车零部件": "汽车配件", "零部件": "汽车配件",
    # 软件服务
    "互联网": "软件服务", "人工智能": "软件服务", "AI": "软件服务", "云计算": "软件服务",
    "大数据": "软件服务", "SaaS": "软件服务", "数字经济": "软件服务",
    # 建筑工程
    "基建": "建筑工程", "房地产": "建筑工程", "建材": "建筑工程",
    "装配式建筑": "建筑工程", "市政": "建筑工程",
    # 黄金
    "贵金属": "黄金", "黄金珠宝": "黄金", "珠宝": "黄金",
    # 其他
    "煤炭": "煤炭开采", "石化": "石油加工", "石油": "石油开采",
    "钢铁": "普钢", "钢材": "普钢",
    "环保": "环境保护", "污水处理": "水务",
    "铁路": "铁路", "高铁": "铁路", "航运": "水运",
    "港口": "港口", "公路": "公路", "高速": "路桥",
    "保险": "保险", "再保险": "保险",
}



# ─── Prompt ───────────────────────────────────────────────────────────────────
def build_prompt(start_date: str, end_date: str, max_mappings: int = 5) -> str:
    return f"""生成 {start_date} 到 {end_date} 对A股有板块主题影响的节假日和重大事件。

【节假日必须覆盖（时间范围内的）】
法定假日(is_trading_closed=true)：元旦、春节、清明节、劳动节、端午节、中秋节、国庆假期
交易日节日(is_trading_closed=false)：妇女节(3/8)、儿童节(6/1)、518博物馆日、618购物节(6/18)、七夕节(农历七月初七)、重阳节、双十一(11/11)、双十二(12/12)、冬至、圣诞节(12/25)、美国感恩节(11月第四周四)
季节节点(is_trading_closed=false)：高考季(6月初)、毕业季(7月中)、开学季(9月初)

【重大事件（每类选最重要的）】
政策会议：全国两会(3月)、中央经济工作会议(12月)、FOMC议息(每季度代表性1次)
博览会：进博会(11月)、广交会(4月/10月)、CES、世界人工智能大会(5月)
体育赛事：奥运/世界杯/亚运会（如在时间范围内）

【联想规则（必须包含谐音/情感类，impact_level=phonetic）】
必须出现：七夕->红黄酒(谐音鹊桥,rel=0.2)、冬至->食品(羊肉,rel=0.3)、儿童节->文教休闲(rel=0.5)
其他可选：感恩节->食品(rel=0.4)、圣诞->影视音像(rel=0.4)、618/双十一->物流(rel=0.6)

 target_type=industry时，target_name只能从以下选：
白酒、旅游景点、旅游服务、酒店餐饮、食品、影视音像、航空、机场、服饰、百货、超市连锁、医疗保健、银行、证券、软件服务、通信设备、半导体、汽车整车、建筑工程、黄金、啤酒、文教休闲、红黄酒、日用化工、化学制药、生物制药、元器件、电气设备、新型电力、仓储物流

relevance评分：0.9-1.0强相关|0.7-0.9较强|0.5-0.7中等|0.3-0.5弱|0.1-0.3谐音联想
每条最多{max_mappings}条mappings，按relevance降序；连续假期只记录1条

输出JSON：{{"holidays":[{{"holiday_date":"YYYY-MM-DD","name":"假期","market_scope":"CN","is_trading_closed":true,"notes":"说明","mappings":[{{"impact_level":"direct","target_type":"industry","target_name":"白酒","code":"","notes":"","relevance":0.9}}]}}],"major_events":[{{"event_date":"YYYY-MM-DD","name":"事件","category":"policy","location":"","notes":"说明","mappings":[{{"impact_level":"direct","target_type":"industry","target_name":"银行","code":"","notes":"","relevance":0.8}}]}}]}}
impact_level: direct/indirect/phonetic | target_type: industry/concept/stock | category: sports/expo/policy/economy/other | relevance: 0.0-1.0浮点数"""


def build_batch_prompt(start_date: str, end_date: str, max_mappings: int = 3, strict: bool = False) -> str:
    note_limit = 6 if strict else 12
    return f"""生成 {start_date} 到 {end_date} 对A股有板块主题影响的节假日和重大事件，只输出一个合法JSON对象。

硬性要求：
1. 只输出JSON，不要markdown，不要解释，不要补充文字。
2. 结果必须尽量短，notes最多{note_limit}个字。
3. 每个节假日/事件最多{max_mappings}条mappings，只保留最重要的，宁少勿滥。
4. target_type=industry时，target_name只能从以下选：白酒、旅游景点、旅游服务、酒店餐饮、食品、影视音像、航空、机场、服饰、百货、超市连锁、医疗保健、银行、证券、软件服务、通信设备、半导体、汽车整车、建筑工程、黄金、啤酒、文教休闲、红黄酒、日用化工、化学制药、生物制药、元器件、电气设备、新型电力、仓储物流。
5. 必须覆盖时间范围内主要节假日：元旦、春节、清明节、劳动节、端午节、中秋节、国庆假期、妇女节、儿童节、518博物馆日、618购物节、七夕节、重阳节、双十一、双十二、冬至、圣诞节、美国感恩节、高考季、毕业季、开学季。
6. 重大事件只保留最重要项：全国两会、中央经济工作会议、FOMC议息、进博会、广交会、CES、世界人工智能大会，以及时间范围内真实存在的奥运/世界杯/亚运会。
7. 必须包含phonetic映射示例：七夕->红黄酒(relevance≈0.2)、冬至->食品(relevance≈0.3)、儿童节->文教休闲(relevance≈0.5)。
8. 连续假期只记1条；字段尽量简短；code通常留空字符串。

输出格式：{{"holidays":[{{"holiday_date":"YYYY-MM-DD","name":"节日","market_scope":"CN","is_trading_closed":true,"notes":"简述","mappings":[{{"impact_level":"direct","target_type":"industry","target_name":"食品","code":"","notes":"简述","relevance":0.8}}]}}],"major_events":[{{"event_date":"YYYY-MM-DD","name":"事件","category":"policy","location":"","notes":"简述","mappings":[{{"impact_level":"direct","target_type":"industry","target_name":"银行","code":"","notes":"简述","relevance":0.8}}]}}]}}
最后一个字符必须是 }}"""



# ─── 工具函数 ──────────────────────────────────────────────────────────────────
def save_log(label: str, content: str) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = LOG_DIR / f"{ts}_{label}.txt"
    path.write_text(content, encoding="utf-8")
    return path


def call_ai(model_cfg: dict, prompt: str) -> str:
    # Claude 系列用 Anthropic SDK，其他走 OpenAI-compatible
    provider = model_cfg.get("provider", "openai_compatible")
    if provider == "anthropic":
        import anthropic
        client = anthropic.Anthropic(api_key=model_cfg["api_key"])
        msg = client.messages.create(
            model=model_cfg["model_id"],
            max_tokens=8192,
            system="你是专业的A股市场事件数据库助手，只输出符合要求的JSON，不输出其他内容。target_type为industry时target_name必须来自给定行业列表。relevance必须是0.0到1.0之间的浮点数。",
            messages=[{"role": "user", "content": prompt}],
        )
        raw = msg.content[0].text
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        return m.group(0) if m else raw

    client = OpenAI(api_key=model_cfg["api_key"], base_url=model_cfg["base_url"])
    kw: dict = dict(
        model=model_cfg["model_id"],
        messages=[
            {"role": "system", "content": "你是专业的A股市场事件数据库助手，只输出符合要求的JSON，不输出其他内容。target_type为industry时target_name必须来自给定行业列表。relevance必须是0.0到1.0之间的浮点数。"},
            {"role": "user", "content": prompt},
        ],
        temperature=TEMPERATURE,
        max_tokens=8192,
        stream=False,
    )
    if model_cfg.get("json_mode", False):
        kw["response_format"] = {"type": "json_object"}
    response = client.chat.completions.create(**kw)
    raw = response.choices[0].message.content
    if not model_cfg.get("json_mode", False):
        m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", raw, re.DOTALL)
        if m:
            raw = m.group(1)
    return raw


def api_request(method: str, base_url: str, path: str, payload: dict | None = None) -> dict | list:
    url = base_url.rstrip("/") + path
    with httpx.Client(timeout=30) as client:
        if method == "POST":
            resp = client.post(url, json=payload)
        elif method == "DELETE":
            resp = client.delete(url)
        else:
            resp = client.get(url)
        resp.raise_for_status()
        return resp.json()


def validate_parsed(data: dict) -> list[str]:
    valid_impact = {"direct", "indirect", "phonetic"}
    valid_target = {"industry", "concept", "stock"}
    valid_cat = {"sports", "expo", "policy", "economy", "other"}
    issues = []
    for h in data.get("holidays", []):
        for m in h.get("mappings", []):
            if m.get("impact_level") not in valid_impact:
                issues.append(f"节假日[{h.get('name')}] impact_level={m.get('impact_level')} 非法")
            if m.get("target_type") not in valid_target:
                issues.append(f"节假日[{h.get('name')}] target_type={m.get('target_type')} 非法")
            if m.get("target_type") == "industry":
                target_name = m.get("target_name")
                if target_name in INDUSTRY_ALIASES:
                    m["target_name"] = INDUSTRY_ALIASES[target_name]
                    target_name = m["target_name"]
                if target_name not in VALID_INDUSTRIES:
                    issues.append(f"节假日[{h.get('name')}] industry={target_name} 不在行业列表")
            rel = m.get("relevance")
            if rel is not None and not (0.0 <= float(rel) <= 1.0):
                issues.append(f"节假日[{h.get('name')}] relevance={rel} 超出范围")
    for e in data.get("major_events", []):
        if e.get("category") not in valid_cat:
            issues.append(f"事件[{e.get('name')}] category={e.get('category')} 非法")
        for m in e.get("mappings", []):
            if m.get("impact_level") not in valid_impact:
                issues.append(f"事件[{e.get('name')}] impact_level={m.get('impact_level')} 非法")
            if m.get("target_type") not in valid_target:
                issues.append(f"事件[{e.get('name')}] target_type={m.get('target_type')} 非法")
            if m.get("target_type") == "industry":
                target_name = m.get("target_name")
                if target_name in INDUSTRY_ALIASES:
                    m["target_name"] = INDUSTRY_ALIASES[target_name]
                    target_name = m["target_name"]
                if target_name not in VALID_INDUSTRIES:
                    issues.append(f"事件[{e.get('name')}] industry={target_name} 不在行业列表")
    return issues


def build_cleanup_prompt(batch_rows: list[tuple]) -> str:
    lines = []
    for row in batch_rows:
        item_type, item_id, item_date, name, category, mapping_str, tag = row
        lines.append(f'- type={item_type}; id={item_id}; date={item_date}; name={name}; category={category}; mappings={mapping_str}')
    joined = "\n".join(lines)
    return f"""你是A股事件库数据清理助手。请检查以下事件/节假日记录，识别明显错误、时间不对齐、同年重复同一节日/事件、或语义高度重复导致应删除的候选项。

要求：
1. 只输出 JSON 对象，不要解释文本。
2. 只返回建议删除的候选项；如果没有问题，返回 {{"candidates":[]}}。
3. reason 必须简短明确，说明为什么应删除该条。
4. 对于重复项，优先保留更合理/更早的那一条，其余列为候选删除。
5. 不要猜测数据库外的信息；只能根据常识和给定记录判断明显问题。

输出格式：
{{
  "candidates": [
    {{"type":"节假日","id":123,"reason":"2027年重复重阳节，疑似错误重复"}},
    {{"type":"事件","id":456,"reason":"与同年同主题事件高度重复，保留另一条即可"}}
  ]
}}

待检查记录：
{joined}
"""


def detect_cleanup_candidates(rows: list[tuple]) -> list[dict]:
    candidates: list[dict] = []
    by_key: dict[tuple[str, str, str], list[tuple]] = {}

    for row in rows:
        item_type, item_id, item_date, name, category, mapping_str, tag = row
        key = (item_type, item_date[:4] if item_date else '', name.strip())
        by_key.setdefault(key, []).append(row)

    for (_item_type, _year, _name), items in by_key.items():
        if len(items) <= 1:
            continue
        keep = min(items, key=lambda r: int(r[1]))
        for row in items:
            if row == keep:
                continue
            item_type, item_id, item_date, name, category, mapping_str, tag = row
            candidates.append({
                'type': item_type,
                'id': int(item_id),
                'date': item_date,
                'name': name,
                'category': category,
                'reason': f'同年同名重复，建议保留 ID={keep[1]}，删除重复项',
                'tag': tag,
            })

    return candidates


# ─── GUI ───────────────────────────────────────────────────────────────────────
class EventAITool(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("AI 事件提取工具")
        self.configure(bg="#0d1117")
        self.geometry("1360x900")
        self._parsed_data: dict | None = None
        self._current_model_cfg: dict = {}
        self._cleanup_candidates: list[dict] = []
        self._cleanup_rows: list[tuple] = []
        self._build_ui()

    def _btn(self, parent, text, cmd, color="#238636", hover="#2ea043", state=tk.NORMAL):
        return tk.Button(parent, text=text, command=cmd, bg=color, fg="white",
                         relief=tk.FLAT, font=("Microsoft YaHei", 9, "bold"),
                         padx=12, pady=5, cursor="hand2", activebackground=hover,
                         activeforeground="white", state=state)

    def _lbl(self, parent, text, bg="#161b22"):
        return tk.Label(parent, text=text, bg=bg, fg="#8b949e", font=("Microsoft YaHei", 9))

    def _entry(self, parent, var, width=20, show=None):
        kw = dict(textvariable=var, width=width, bg="#21262d", fg="#c9d1d9",
                  insertbackground="#c9d1d9", relief=tk.FLAT)
        if show:
            kw["show"] = show
        return tk.Entry(parent, **kw)

    def _text(self, parent, height=20, fg="#c9d1d9") -> scrolledtext.ScrolledText:
        return scrolledtext.ScrolledText(
            parent, height=height, font=("Consolas", 9), bg="#0d1117", fg=fg,
            insertbackground=fg, relief=tk.FLAT, bd=0, padx=8, pady=6, wrap=tk.WORD,
        )

    def _build_ui(self):
        # ── 配置区 ──────────────────────────────────────────────────────────────
        cfg = tk.Frame(self, bg="#161b22", pady=6, padx=10)
        cfg.pack(fill=tk.X)

        self._lbl(cfg, "模型:").grid(row=0, column=0, sticky="w", padx=4, pady=2)
        model_names = [f"{k} ({v.get('name', k)})" for k, v in MODELS.items()] or ["deepseek"]
        self.model_select = ttk.Combobox(cfg, values=model_names, width=28, state="readonly")
        self.model_select.grid(row=0, column=1, padx=4)
        default_idx = list(MODELS.keys()).index(DEFAULT_MODEL_KEY) if DEFAULT_MODEL_KEY in MODELS else 0
        self.model_select.current(default_idx)
        self.model_select.bind("<<ComboboxSelected>>", self._on_model_change)

        self._lbl(cfg, "API Key:").grid(row=0, column=2, sticky="w", padx=4)
        self.api_key_var = tk.StringVar()
        self._entry(cfg, self.api_key_var, width=44, show="*").grid(row=0, column=3, padx=4)

        self._lbl(cfg, "Model ID:").grid(row=0, column=4, sticky="w", padx=4)
        self.model_id_var = tk.StringVar()
        self._entry(cfg, self.model_id_var, width=22).grid(row=0, column=5, padx=4)

        self._lbl(cfg, "sql-tool API:").grid(row=1, column=0, sticky="w", padx=4, pady=2)
        self.sqltool_api_var = tk.StringVar(value=DEFAULT_API_BASE)
        self._entry(cfg, self.sqltool_api_var, width=28).grid(row=1, column=1, padx=4, sticky="w")

        self._lbl(cfg, "开始:").grid(row=1, column=2, sticky="w", padx=4)
        self.start_var = tk.StringVar(value=(TODAY - timedelta(days=365)).isoformat())
        self._entry(cfg, self.start_var, width=13).grid(row=1, column=3, padx=4, sticky="w")

        self._lbl(cfg, "结束:").grid(row=1, column=4, sticky="w", padx=4)
        self.end_var = tk.StringVar(value=(TODAY + timedelta(days=365)).isoformat())
        self._entry(cfg, self.end_var, width=13).grid(row=1, column=5, padx=4, sticky="w")

        self._lbl(cfg, "每年重复:").grid(row=1, column=6, sticky="w", padx=4)
        self.repeat_var = tk.StringVar(value="3")
        tk.Entry(cfg, textvariable=self.repeat_var, width=4, bg="#21262d", fg="#c9d1d9",
                 insertbackground="#c9d1d9", relief=tk.FLAT).grid(row=1, column=7, padx=4, sticky="w")
        self._lbl(cfg, "次").grid(row=1, column=8, sticky="w")

        self._on_model_change()

        # ── 操作栏 ──────────────────────────────────────────────────────────────
        bar = tk.Frame(self, bg="#0d1117", pady=6)
        bar.pack(fill=tk.X, padx=10)

        self.btn_gen   = self._btn(bar, "▶ AI 生成", self._on_generate)
        self.btn_gen.pack(side=tk.LEFT, padx=4)
        self.btn_prev  = self._btn(bar, "  解析预览", self._on_preview,
                                   color="#1f6feb", hover="#388bfd", state=tk.DISABLED)
        self.btn_prev.pack(side=tk.LEFT, padx=4)
        self.btn_write = self._btn(bar, "  确认写入", self._on_write,
                                   color="#6e40c9", hover="#8b5cf6", state=tk.DISABLED)
        self.btn_write.pack(side=tk.LEFT, padx=4)
        self.btn_refresh = self._btn(bar, "⟳ 刷新事件库", self._on_refresh_db,
                                     color="#0d419d", hover="#1f6feb")
        self.btn_refresh.pack(side=tk.LEFT, padx=12)
        self.btn_batch = self._btn(bar, "⚡ 批量生成（9年）", self._on_batch_generate,
                                   color="#b45309", hover="#d97706")
        self.btn_batch.pack(side=tk.LEFT, padx=4)
        self.btn_cleanup = self._btn(bar, "🧹 批量清理预检", self._on_cleanup_scan,
                                     color="#7c3aed", hover="#8b5cf6")
        self.btn_cleanup.pack(side=tk.LEFT, padx=4)
        self.btn_cleanup_apply = self._btn(bar, "确认批量删除", self._on_cleanup_apply,
                                           color="#da3633", hover="#f85149", state=tk.DISABLED)
        self.btn_cleanup_apply.pack(side=tk.LEFT, padx=4)

        tk.Frame(bar, bg="#30363d", width=1, height=26).pack(side=tk.LEFT, padx=8, fill=tk.Y)

        self._lbl(bar, "删除:", "#0d1117").pack(side=tk.LEFT, padx=4)
        self.del_type_var = tk.StringVar(value="holiday")
        for val, lbl in [("holiday", "节假日"), ("event", "事件")]:
            tk.Radiobutton(bar, text=lbl, variable=self.del_type_var, value=val,
                           bg="#0d1117", fg="#c9d1d9", selectcolor="#21262d",
                           activebackground="#0d1117").pack(side=tk.LEFT, padx=2)
        tk.Label(bar, text="ID:", bg="#0d1117", fg="#8b949e",
                 font=("Microsoft YaHei", 9)).pack(side=tk.LEFT, padx=4)
        self.del_id_var = tk.StringVar()
        tk.Entry(bar, textvariable=self.del_id_var, width=6, bg="#21262d",
                 fg="#c9d1d9", insertbackground="#c9d1d9", relief=tk.FLAT).pack(side=tk.LEFT)
        self._btn(bar, "删除", self._on_delete_one, color="#b08800", hover="#d4a017").pack(side=tk.LEFT, padx=4)
        self._btn(bar, "清空节假日", self._on_clear_holidays, color="#da3633", hover="#f85149").pack(side=tk.LEFT, padx=2)
        self._btn(bar, "清空事件", self._on_clear_events, color="#da3633", hover="#f85149").pack(side=tk.LEFT, padx=2)

        self.status_var = tk.StringVar(value="就绪")
        tk.Label(bar, textvariable=self.status_var, bg="#0d1117", fg="#8b949e",
                 font=("Microsoft YaHei", 9)).pack(side=tk.RIGHT, padx=10)

        # ── Notebook ─────────────────────────────────────────────────────────────
        s = ttk.Style()
        s.theme_use("default")
        s.configure("TNotebook", background="#0d1117", borderwidth=0)
        s.configure("TNotebook.Tab", background="#21262d", foreground="#8b949e",
                    padding=[12, 4], font=("Microsoft YaHei", 9))
        s.map("TNotebook.Tab", background=[("selected", "#161b22")],
              foreground=[("selected", "#c9d1d9")])

        nb = ttk.Notebook(self)
        nb.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 8))

        t0 = tk.Frame(nb, bg="#0d1117"); nb.add(t0, text=" Prompt ")
        self.prompt_text = self._text(t0)
        self.prompt_text.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)
        self._refresh_prompt()

        t1 = tk.Frame(nb, bg="#0d1117"); nb.add(t1, text=" AI 原始输出 ")
        self.raw_text = self._text(t1)
        self.raw_text.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)

        t2 = tk.Frame(nb, bg="#0d1117"); nb.add(t2, text=" 结构预览 ")
        self.preview_text = self._text(t2, fg="#6ee7b7")
        self.preview_text.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)

        t3 = tk.Frame(nb, bg="#0d1117"); nb.add(t3, text=" 事件库浏览 ")
        self._build_db_tab(t3)

        t4 = tk.Frame(nb, bg="#0d1117"); nb.add(t4, text=" 操作日志 ")
        self.log_text = self._text(t4, fg="#a3c4f3")
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)

        t5 = tk.Frame(nb, bg="#0d1117"); nb.add(t5, text=" 清理预览 ")
        self._build_cleanup_tab(t5)

    # ── 模型切换 ─────────────────────────────────────────────────────────────────
    def _on_model_change(self, *_):
        sel = self.model_select.get()
        key = sel.split(" ")[0] if sel else DEFAULT_MODEL_KEY
        cfg = MODELS.get(key, {})
        self._current_model_cfg = cfg
        self.api_key_var.set(cfg.get("api_key", ""))
        self.model_id_var.set(cfg.get("model_id", ""))

    def _get_model_cfg(self) -> dict:
        cfg = dict(self._current_model_cfg)
        cfg["api_key"] = self.api_key_var.get().strip()
        cfg["model_id"] = self.model_id_var.get().strip()
        return cfg

    def _build_cleanup_tab(self, parent):
        cols = ("类型", "ID", "日期", "名称", "分类", "删除原因")
        frame = tk.Frame(parent, bg="#0d1117")
        frame.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)
        self.cleanup_tree = ttk.Treeview(frame, columns=cols, show="headings", style="DB.Treeview", selectmode="extended")
        widths = [60, 60, 100, 180, 100, 420]
        for col, w in zip(cols, widths):
            self.cleanup_tree.heading(col, text=col)
            self.cleanup_tree.column(col, width=w, minwidth=60)
        vsb = ttk.Scrollbar(frame, orient=tk.VERTICAL, command=self.cleanup_tree.yview)
        hsb = ttk.Scrollbar(frame, orient=tk.HORIZONTAL, command=self.cleanup_tree.xview)
        self.cleanup_tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.cleanup_tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        frame.rowconfigure(0, weight=1)
        frame.columnconfigure(0, weight=1)

        action_row = tk.Frame(parent, bg="#0d1117", pady=6)
        action_row.pack(fill=tk.X, padx=4)
        self._btn(action_row, "否决选中项", self._on_cleanup_reject_selected,
                  color="#475569", hover="#64748b").pack(side=tk.LEFT, padx=4)
        self.cleanup_summary_var = tk.StringVar(value="尚未生成删除候选")
        tk.Label(action_row, textvariable=self.cleanup_summary_var, bg="#0d1117", fg="#8b949e",
                 font=("Microsoft YaHei", 9)).pack(side=tk.RIGHT, padx=8)

    def _render_cleanup_candidates(self):
        self.cleanup_tree.delete(*self.cleanup_tree.get_children())
        for item in self._cleanup_candidates:
            self.cleanup_tree.insert(
                "", tk.END,
                values=(item["type"], item["id"], item["date"], item["name"], item["category"], item["reason"]),
                tags=(item.get("tag", ""),),
            )
        self.cleanup_summary_var.set(f"当前候选 {len(self._cleanup_candidates)} 条；可先否决不想删除的单项，再点确认批量删除")
        self.btn_cleanup_apply.config(state=tk.NORMAL if self._cleanup_candidates else tk.DISABLED)

    def _on_cleanup_scan(self):
        model_cfg = self._get_model_cfg()
        if not model_cfg.get("api_key"):
            messagebox.showwarning("配置缺失", "请填写用于清理预检的 AI API Key；建议使用 DeepSeek R1")
            return
        self.btn_cleanup.config(state=tk.DISABLED)
        self.status_var.set("正在分批扫描重复/错位事件...")
        threading.Thread(target=self._cleanup_scan_thread, daemon=True).start()

    def _cleanup_scan_thread(self):
        try:
            api_base = self.sqltool_api_var.get()
            model_cfg = self._get_model_cfg()
            h_resp = api_request("GET", api_base, "/events/holidays")
            e_resp = api_request("GET", api_base, "/events/major")
            holidays = h_resp.get("items", h_resp) if isinstance(h_resp, dict) else h_resp
            events = e_resp.get("items", e_resp) if isinstance(e_resp, dict) else e_resp
            rows = []
            for h in holidays:
                rows.append(("节假日", str(h["holiday_id"]), h["holiday_date"], h["name"], "休市" if h.get("is_trading_closed") else "交易", "", "holiday"))
            for e in events:
                rows.append(("事件", str(e["event_id"]), e["event_date"], e["name"], e.get("category", ""), "", "event"))

            candidates: list[dict] = []
            batch_size = 40
            for start in range(0, len(rows), batch_size):
                batch_rows = rows[start:start + batch_size]
                prompt = build_cleanup_prompt(batch_rows)
                prompt_log = save_log(f"cleanup_{start:03d}_prompt", prompt)
                try:
                    raw = call_ai(model_cfg, prompt)
                    raw_log = save_log(f"cleanup_{start:03d}_raw", raw)
                    parsed = json.loads(raw)
                    for item in parsed.get("candidates", []):
                        matched = next((row for row in batch_rows if int(row[1]) == int(item.get("id", -1)) and row[0] == item.get("type")), None)
                        if matched:
                            candidates.append({
                                'type': matched[0],
                                'id': int(matched[1]),
                                'date': matched[2],
                                'name': matched[3],
                                'category': matched[4],
                                'reason': item.get('reason', 'AI 判定为疑似异常/重复'),
                                'tag': matched[6],
                            })
                    self.after(0, self._log, f"[清理预检] batch {start//batch_size + 1} prompt={prompt_log.name} raw={raw_log.name}")
                except Exception as exc:
                    self.after(0, self._log, f"[清理预检失败] batch {start//batch_size + 1}: {exc}")

            self.after(0, self._on_cleanup_scan_done, rows, candidates)
        except Exception as exc:
            self.after(0, self._on_cleanup_scan_error, str(exc))

    def _on_cleanup_scan_done(self, rows: list[tuple], candidates: list[dict]):
        self._cleanup_rows = rows
        self._cleanup_candidates = candidates
        self._render_cleanup_candidates()
        self.btn_cleanup.config(state=tk.NORMAL)
        self.status_var.set(f"清理预检完成：候选 {len(candidates)} 条")
        self._log(f"[清理预检] 扫描 {len(rows)} 条，候选删除 {len(candidates)} 条")

    def _on_cleanup_scan_error(self, err: str):
        self.btn_cleanup.config(state=tk.NORMAL)
        self.status_var.set(f"清理预检失败: {err}")
        self._log(f"[清理预检失败] {err}")

    def _on_cleanup_apply(self):
        if not self._cleanup_candidates:
            messagebox.showinfo("提示", "当前没有待删除候选")
            return
        if not messagebox.askyesno("确认批量删除", f"将删除 {len(self._cleanup_candidates)} 条候选事件/节假日，是否继续？"):
            return
        self.btn_cleanup_apply.config(state=tk.DISABLED)
        self.status_var.set("正在批量删除候选数据...")
        threading.Thread(target=self._cleanup_apply_thread, daemon=True).start()

    def _cleanup_apply_thread(self):
        api_base = self.sqltool_api_var.get()
        deleted, failed = [], []
        for item in self._cleanup_candidates:
            try:
                path = f'/events/holidays/{item["id"]}' if item['type'] == '节假日' else f'/events/major/{item["id"]}'
                api_request("DELETE", api_base, path)
                deleted.append(item)
            except Exception as exc:
                failed.append((item, str(exc)))
        self.after(0, self._on_cleanup_apply_done, deleted, failed)

    def _on_cleanup_apply_done(self, deleted: list[dict], failed: list[tuple]):
        for item in deleted:
            self._log(f"[清理删除] {item['type']} ID={item['id']} {item['name']} - {item['reason']}")
        for item, err in failed:
            self._log(f"[清理删除失败] {item['type']} ID={item['id']} {item['name']}: {err}")
        self.status_var.set(f"批量删除完成：成功 {len(deleted)} / 失败 {len(failed)}")
        self._cleanup_candidates = [item for item in self._cleanup_candidates if item not in deleted]
        self._render_cleanup_candidates()
        self._on_refresh_db()

    def _on_cleanup_reject_selected(self):
        selected = self.cleanup_tree.selection()
        if not selected:
            messagebox.showinfo("提示", "请先选择要否决的候选项")
            return
        reject_ids = {int(self.cleanup_tree.item(item_id, 'values')[1]) for item_id in selected}
        before = len(self._cleanup_candidates)
        self._cleanup_candidates = [item for item in self._cleanup_candidates if item['id'] not in reject_ids]
        self._render_cleanup_candidates()
        self._log(f"[清理预检] 已否决 {before - len(self._cleanup_candidates)} 条候选删除项")

    # ── 事件库浏览 Tab ──────────────────────────────────────────────────────────
    def _build_db_tab(self, parent):
        flt = tk.Frame(parent, bg="#161b22", pady=6, padx=10)
        flt.pack(fill=tk.X)

        tk.Label(flt, text="类型:", bg="#161b22", fg="#8b949e",
                 font=("Microsoft YaHei", 9)).pack(side=tk.LEFT, padx=4)
        self.db_filter_type = tk.StringVar(value="全部")
        for val in ["全部", "节假日", "事件"]:
            tk.Radiobutton(flt, text=val, variable=self.db_filter_type, value=val,
                           bg="#161b22", fg="#c9d1d9", selectcolor="#21262d",
                           activebackground="#161b22",
                           command=self._apply_db_filter).pack(side=tk.LEFT, padx=4)

        tk.Label(flt, text="关键词:", bg="#161b22", fg="#8b949e",
                 font=("Microsoft YaHei", 9)).pack(side=tk.LEFT, padx=12)
        self.db_search_var = tk.StringVar()
        self.db_search_var.trace_add("write", lambda *_: self._apply_db_filter())
        tk.Entry(flt, textvariable=self.db_search_var, width=20,
                 bg="#21262d", fg="#c9d1d9", insertbackground="#c9d1d9",
                 relief=tk.FLAT).pack(side=tk.LEFT, padx=4)

        self.db_count_var = tk.StringVar(value="共 0 条")
        tk.Label(flt, textvariable=self.db_count_var, bg="#161b22",
                 fg="#6ee7b7", font=("Microsoft YaHei", 9)).pack(side=tk.RIGHT, padx=10)

        cols = ("类型", "ID", "日期", "名称", "分类/休市", "影响板块（类型/层级/相关性）")
        s = ttk.Style()
        s.configure("DB.Treeview", background="#0d1117", foreground="#c9d1d9",
                    fieldbackground="#0d1117", rowheight=22, font=("Microsoft YaHei", 9))
        s.configure("DB.Treeview.Heading", background="#21262d", foreground="#8b949e",
                    font=("Microsoft YaHei", 9, "bold"))
        s.map("DB.Treeview", background=[("selected", "#1f6feb")])

        frame = tk.Frame(parent, bg="#0d1117")
        frame.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)

        self.db_tree = ttk.Treeview(frame, columns=cols, show="headings", style="DB.Treeview")
        widths = [60, 40, 100, 160, 80, 450]
        for col, w in zip(cols, widths):
            self.db_tree.heading(col, text=col, command=lambda c=col: self._sort_tree(c))
            self.db_tree.column(col, width=w, minwidth=40)

        vsb = ttk.Scrollbar(frame, orient=tk.VERTICAL, command=self.db_tree.yview)
        hsb = ttk.Scrollbar(frame, orient=tk.HORIZONTAL, command=self.db_tree.xview)
        self.db_tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.db_tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        frame.rowconfigure(0, weight=1)
        frame.columnconfigure(0, weight=1)

        self.db_tree.tag_configure("holiday", foreground="#fde68a")
        self.db_tree.tag_configure("event",   foreground="#a3c4f3")
        self._db_rows: list[tuple] = []
        self._sort_col = "日期"
        self._sort_rev = False

    def _on_refresh_db(self):
        self.btn_refresh.config(state=tk.DISABLED)
        self.status_var.set("正在刷新事件库...")
        threading.Thread(target=self._refresh_db_thread, daemon=True).start()

    def _refresh_db_thread(self):
        try:
            api_base = self.sqltool_api_var.get()
            h_resp = api_request("GET", api_base, "/events/holidays")
            e_resp = api_request("GET", api_base, "/events/major")
            holidays = h_resp.get("items", h_resp) if isinstance(h_resp, dict) else h_resp
            events   = e_resp.get("items", e_resp) if isinstance(e_resp, dict) else e_resp

            rows = []
            for h in holidays:
                hid = h["holiday_id"]
                try:
                    m_resp = api_request("GET", api_base, f"/events/holidays/{hid}/mappings")
                    mappings = m_resp.get("items", [])
                except Exception:
                    mappings = []
                mapping_str = " | ".join(
                    f"[{m.get('impact_level','?')}/{m.get('target_type','?')} {m.get('relevance','')}] {m.get('target_name','')}"
                    for m in mappings
                )
                rows.append(("节假日", str(hid), h["holiday_date"], h["name"],
                             "休市" if h.get("is_trading_closed") else "交易",
                             mapping_str, "holiday"))

            for e in events:
                eid = e["event_id"]
                try:
                    m_resp = api_request("GET", api_base, f"/events/major/{eid}/mappings")
                    mappings = m_resp.get("items", [])
                except Exception:
                    mappings = []
                mapping_str = " | ".join(
                    f"[{m.get('impact_level','?')}/{m.get('target_type','?')} {m.get('relevance','')}] {m.get('target_name','')}"
                    for m in mappings
                )
                rows.append(("事件", str(eid), e["event_date"], e["name"],
                             e.get("category", ""), mapping_str, "event"))

            self.after(0, self._on_refresh_db_done, rows)
        except Exception as ex:
            self.after(0, self._on_refresh_db_error, str(ex))

    def _on_refresh_db_done(self, rows: list[tuple]):
        self._db_rows = rows
        self._apply_db_filter()
        self.btn_refresh.config(state=tk.NORMAL)
        self.status_var.set(f"事件库已刷新，共 {len(rows)} 条")
        self._log(f"[刷新] 事件库共 {len(rows)} 条")

    def _on_refresh_db_error(self, err: str):
        self.btn_refresh.config(state=tk.NORMAL)
        self.status_var.set(f"刷新失败: {err}")
        self._log(f"[刷新失败] {err}")

    def _apply_db_filter(self):
        ftype = self.db_filter_type.get()
        kw = self.db_search_var.get().strip().lower()
        filtered = [
            row for row in self._db_rows
            if (ftype == "全部" or (ftype == "节假日") == (row[0] == "节假日"))
            and (not kw or any(kw in str(v).lower() for v in row[:6]))
        ]
        self._fill_tree(filtered)
        self.db_count_var.set(f"共 {len(filtered)} 条 / 总 {len(self._db_rows)} 条")

    def _fill_tree(self, rows: list[tuple]):
        self.db_tree.delete(*self.db_tree.get_children())
        for row in rows:
            self.db_tree.insert("", tk.END, values=row[:6], tags=(row[6],))

    def _sort_tree(self, col: str):
        cols = ("类型", "ID", "日期", "名称", "分类/休市", "影响板块（类型/层级/相关性）")
        idx = cols.index(col)
        self._sort_rev = (col == self._sort_col) and not self._sort_rev
        self._sort_col = col
        self._db_rows.sort(key=lambda r: r[idx], reverse=self._sort_rev)
        self._apply_db_filter()

    def _refresh_prompt(self):
        prompt = build_prompt(self.start_var.get(), self.end_var.get(), MAX_MAPPINGS)
        self.prompt_text.delete("1.0", tk.END)
        self.prompt_text.insert(tk.END, prompt)

    def _on_generate(self):
        self._refresh_prompt()
        model_cfg = self._get_model_cfg()
        if not model_cfg.get("api_key"):
            messagebox.showwarning("配置缺失", "请在 config.toml 或界面中填入当前模型的 API Key")
            return
        self.btn_gen.config(state=tk.DISABLED)
        self.btn_prev.config(state=tk.DISABLED)
        self.btn_write.config(state=tk.DISABLED)
        self.status_var.set(f"正在调用 {model_cfg.get('name', model_cfg.get('model_id', 'AI'))}...")
        self._parsed_data = None
        threading.Thread(target=self._generate_thread, args=(model_cfg,), daemon=True).start()

    def _generate_thread(self, model_cfg: dict):
        prompt = self.prompt_text.get("1.0", tk.END).strip()
        log_path = save_log("prompt", prompt)
        try:
            raw = call_ai(model_cfg, prompt)
            raw_log = save_log("raw_output", raw)
            self.after(0, self._on_generate_done, raw, str(log_path), str(raw_log))
        except Exception as e:
            self.after(0, self._on_generate_error, str(e))

    def _on_generate_done(self, raw: str, prompt_log: str, raw_log: str):
        self.raw_text.delete("1.0", tk.END)
        self.raw_text.insert(tk.END, raw)
        self.status_var.set("生成完成")
        self.btn_gen.config(state=tk.NORMAL)
        self.btn_prev.config(state=tk.NORMAL)
        self._log(f"[生成] prompt={prompt_log}  raw={raw_log}")

    def _on_generate_error(self, err: str):
        self.status_var.set(f"生成失败: {err}")
        self.btn_gen.config(state=tk.NORMAL)
        self._log(f"[生成失败] {err}")

    def _on_preview(self):
        raw = self.raw_text.get("1.0", tk.END).strip()
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            messagebox.showerror("JSON 解析失败", str(e))
            return

        issues = validate_parsed(data)
        self._parsed_data = data
        holidays = data.get("holidays", [])
        events = data.get("major_events", [])

        lines = [f"范围: {self.start_var.get()} ~ {self.end_var.get()}\n"]
        if issues:
            lines.append(f"[!] 格式/行业/相关性问题 ({len(issues)} 条):")
            lines += [f"    {i}" for i in issues]
            lines.append("")

        lines.append(f"=== 节假日 ({len(holidays)} 条) ===\n")
        for h in holidays:
            lines.append(f"  [{h.get('holiday_date')}] {h.get('name')}  休市={h.get('is_trading_closed')}")
            lines.append(f"     {h.get('notes', '')}")
            for m in h.get("mappings", []):
                rel = m.get("relevance", "?")
                lines.append(f"     -> [{m.get('impact_level')}/{m.get('target_type')} rel={rel:.2f}] {m.get('target_name')}  {m.get('notes','')}" if isinstance(rel, float) else
                             f"     -> [{m.get('impact_level')}/{m.get('target_type')} rel={rel}] {m.get('target_name')}  {m.get('notes','')}")
            lines.append("")

        lines.append(f"\n=== 重大事件 ({len(events)} 条) ===\n")
        for e in events:
            lines.append(f"  [{e.get('event_date')}] {e.get('name')}  [{e.get('category')}]  {e.get('location','')}")
            lines.append(f"     {e.get('notes', '')}")
            for m in e.get("mappings", []):
                rel = m.get("relevance", "?")
                lines.append(f"     -> [{m.get('impact_level')}/{m.get('target_type')} rel={rel:.2f}] {m.get('target_name')}  {m.get('notes','')}" if isinstance(rel, float) else
                             f"     -> [{m.get('impact_level')}/{m.get('target_type')} rel={rel}] {m.get('target_name')}  {m.get('notes','')}")
            lines.append("")

        content = "\n".join(lines)
        self.preview_text.delete("1.0", tk.END)
        self.preview_text.insert(tk.END, content)
        save_log("preview", content)

        issue_note = f" / {len(issues)} 格式问题" if issues else " / 格式OK"
        self.status_var.set(f"解析: {len(holidays)} 节假日 / {len(events)} 事件{issue_note}")
        self.btn_write.config(state=tk.NORMAL)

    def _on_write(self):
        if not self._parsed_data:
            messagebox.showwarning("未解析", "请先点击「解析预览」")
            return
        issues = validate_parsed(self._parsed_data)
        msg = "将向 sql-tool API 写入以上事件数据。"
        if issues:
            msg += f"\n\n[!] 存在 {len(issues)} 条格式/行业问题，建议先确认。"
        if not messagebox.askyesno("确认写入", msg + "\n\n确认？"):
            return
        self.btn_write.config(state=tk.DISABLED)
        self.status_var.set("正在写入...")
        threading.Thread(target=self._write_thread, daemon=True).start()

    def _write_thread(self):
        api_base = self.sqltool_api_var.get()
        data = self._parsed_data
        results, errors = [], []
        for h in data.get("holidays", []):
            try:
                resp = api_request("POST", api_base, "/events/holidays", h)
                results.append(f"[节假日] {h.get('name')} -> id={resp.get('holiday_id')} mappings={resp.get('mapping_count')}")
            except Exception as e:
                errors.append(f"[节假日FAIL] {h.get('name')}: {e}")
        for e in data.get("major_events", []):
            try:
                resp = api_request("POST", api_base, "/events/major", e)
                results.append(f"[事件] {e.get('name')} -> id={resp.get('event_id')} mappings={resp.get('mapping_count')}")
            except Exception as ex:
                errors.append(f"[事件FAIL] {e.get('name')}: {ex}")
        self.after(0, self._on_write_done, results, errors)

    def _on_write_done(self, results: list[str], errors: list[str]):
        summary = "\n".join(["=== 写入结果 ==="] + results + (["\n=== 失败 ==="] + errors if errors else []))
        save_log("write_result", summary)
        for line in results:
            self._log(f"[OK] {line}")
        for line in errors:
            self._log(f"[FAIL] {line}")
        self._log(f"[写入] 成功={len(results)} 失败={len(errors)}")
        self.status_var.set(f"写入: 成功 {len(results)} / 失败 {len(errors)}")
        self.btn_write.config(state=tk.NORMAL)
        if errors:
            messagebox.showwarning("部分失败", f"成功 {len(results)} / 失败 {len(errors)}\n详见操作日志")
        else:
            messagebox.showinfo("写入成功", f"全部 {len(results)} 条写入成功")
        self._on_refresh_db()

    def _on_batch_generate(self):
        model_cfg = self._get_model_cfg()
        if not model_cfg.get("api_key"):
            messagebox.showwarning("配置缺失", "请在 config.toml 或界面中填入当前模型的 API Key")
            return
        try:
            start = date.fromisoformat(self.start_var.get().strip())
        except ValueError:
            messagebox.showwarning("日期错误", "开始日期格式应为 YYYY-MM-DD")
            return
        try:
            repeat = max(1, int(self.repeat_var.get().strip()))
        except ValueError:
            repeat = 3
        self.btn_batch.config(state=tk.DISABLED)
        self.btn_gen.config(state=tk.DISABLED)
        self.btn_write.config(state=tk.DISABLED)
        total_calls = 36 * repeat
        self.status_var.set(f"批量生成中（0/{total_calls}）...")
        threading.Thread(target=self._batch_generate_thread, args=(model_cfg, start, repeat), daemon=True).start()

    def _batch_generate_thread(self, model_cfg: dict, start: date, repeat: int = 3):
        api_base = self.sqltool_api_var.get()
        total_written = 0

        # 构造 36 个季度段（9年 × 4季度）
        quarter_windows: list[tuple[str, str, str]] = []
        for i in range(9):
            y = start.year + i
            quarter_windows.extend([
                (date(y, 1, 1).isoformat(), date(y, 3, 31).isoformat(), f"第{i+1}年Q1"),
                (date(y, 4, 1).isoformat(), date(y, 6, 30).isoformat(), f"第{i+1}年Q2"),
                (date(y, 7, 1).isoformat(), date(y, 9, 30).isoformat(), f"第{i+1}年Q3"),
                (date(y, 10, 1).isoformat(), date(y, 12, 31).isoformat(), f"第{i+1}年Q4"),
            ])

        total_calls = len(quarter_windows) * repeat
        call_idx = 0

        for s_str, e_str, period_label in quarter_windows:
            for r in range(repeat):
                call_idx += 1
                label = f"{period_label}第{r+1}次" if repeat > 1 else period_label
                self.after(0, self.start_var.set, s_str)
                self.after(0, self.end_var.set, e_str)
                self.after(0, self._refresh_prompt)
                self.after(0, self._log, f"[批量 {call_idx}/{total_calls}] {label} 生成 {s_str} ~ {e_str}")
                self.after(0, self.status_var.set,
                           f"批量生成中（{call_idx}/{total_calls}）{label}")
                time.sleep(0.15)

                prompt = build_prompt(s_str, e_str, MAX_MAPPINGS)
                prompt_log = save_log(f"batch_{call_idx:03d}_prompt", prompt)
                try:
                    raw = call_ai(model_cfg, prompt)
                    raw_log = save_log(f"batch_{call_idx:03d}_raw_output", raw)
                except Exception as ex:
                    self.after(0, self._log, f"[批量 {call_idx}/{total_calls}] AI调用失败: {ex}")
                    self.after(0, self._on_batch_done, total_written, f"{label}AI调用失败: {ex}")
                    return

                self.after(0, self.raw_text.delete, "1.0", tk.END)
                self.after(0, self.raw_text.insert, tk.END, raw)

                try:
                    data = json.loads(raw)
                except json.JSONDecodeError as ex:
                    save_log(f"batch_{call_idx:03d}_raw_error", raw)
                    self.after(0, self._log,
                               f"[批量 {call_idx}/{total_calls}] JSON解析失败（输出可能被截断）: {ex}，跳过本次")
                    time.sleep(1.0)
                    continue

                issues = validate_parsed(data)
                self._parsed_data = data
                holidays = data.get("holidays", [])
                events = data.get("major_events", [])
                lines = [f"范围: {s_str} ~ {e_str}  ({label})\n"]
                if issues:
                    lines.append(f"[!] 格式/行业问题 ({len(issues)} 条):")
                    lines += [f"    {x}" for x in issues]
                    lines.append("")
                lines.append(f"=== 节假日 ({len(holidays)} 条) ===\n")
                for h in holidays:
                    lines.append(f"  [{h.get('holiday_date')}] {h.get('name')}  休市={h.get('is_trading_closed')}")
                    for m in h.get("mappings", []):
                        rel = m.get("relevance", "?")
                        lines.append(f"     -> [{m.get('impact_level')}/{m.get('target_type')} rel={rel:.2f}] {m.get('target_name')}" if isinstance(rel, float) else
                                     f"     -> [{m.get('impact_level')}/{m.get('target_type')} rel={rel}] {m.get('target_name')}")
                    lines.append("")
                lines.append(f"\n=== 重大事件 ({len(events)} 条) ===\n")
                for e in events:
                    lines.append(f"  [{e.get('event_date')}] {e.get('name')}  [{e.get('category')}]")
                    for m in e.get("mappings", []):
                        rel = m.get("relevance", "?")
                        lines.append(f"     -> [{m.get('impact_level')}/{m.get('target_type')} rel={rel:.2f}] {m.get('target_name')}" if isinstance(rel, float) else
                                     f"     -> [{m.get('impact_level')}/{m.get('target_type')} rel={rel}] {m.get('target_name')}")
                    lines.append("")
                preview_content = "\n".join(lines)
                save_log(f"batch_{call_idx:03d}_preview", preview_content)
                self.after(0, self.preview_text.delete, "1.0", tk.END)
                self.after(0, self.preview_text.insert, tk.END, preview_content)
                self.after(0, self._log, f"  prompt={prompt_log.name} raw={raw_log.name}")
                if issues:
                    for iss in issues[:5]:
                        self.after(0, self._log, f"  [!] {iss}")

                written = 0
                for h in holidays:
                    try:
                        api_request("POST", api_base, "/events/holidays", h)
                        written += 1
                    except Exception as ex:
                        self.after(0, self._log, f"  [节假日FAIL] {h.get('name')}: {ex}")
                for e in events:
                    try:
                        api_request("POST", api_base, "/events/major", e)
                        written += 1
                    except Exception as ex:
                        self.after(0, self._log, f"  [事件FAIL] {e.get('name')}: {ex}")

                total_written += written
                self.after(0, self._log,
                           f"[批量 {call_idx}/{total_calls}] {label} 写入 {written} 条 "
                           f"(节假日{len(holidays)} 事件{len(events)})")

                if r < repeat - 1:
                    time.sleep(1.0)
                else:
                    time.sleep(1.5)

        self.after(0, self._on_batch_done, total_written, None)

    def _on_batch_done(self, total: int, error: str | None):
        self.btn_batch.config(state=tk.NORMAL)
        self.btn_gen.config(state=tk.NORMAL)
        self.btn_write.config(state=tk.NORMAL)
        if error:
            self.status_var.set(f"批量中止: {error}")
            self._log(f"[批量] 中止，累计写入 {total} 条")
        else:
            repeat = int(self.repeat_var.get().strip()) if self.repeat_var.get().strip().isdigit() else 3
            self.status_var.set(f"批量完成，36季×{repeat}次，共写入 {total} 条")
            self._log(f"[批量] 全部完成，共写入 {total} 条")
        self._on_refresh_db()

    def _on_delete_one(self):
        dtype = self.del_type_var.get()
        id_str = self.del_id_var.get().strip()
        if not id_str.isdigit():
            messagebox.showwarning("输入错误", "请输入有效的数字 ID")
            return
        eid = int(id_str)
        label = "节假日" if dtype == "holiday" else "事件"
        if not messagebox.askyesno("确认删除", f"删除 {label} ID={eid}？同时删除其板块映射。"):
            return
        try:
            path = f"/events/holidays/{eid}" if dtype == "holiday" else f"/events/major/{eid}"
            resp = api_request("DELETE", self.sqltool_api_var.get(), path)
            self._log(f"[删除] {label} ID={eid} -> {resp}")
            self.status_var.set(f"已删除 {label} ID={eid}")
            self._on_refresh_db()
        except Exception as e:
            self._log(f"[删除失败] {e}")
            messagebox.showerror("删除失败", str(e))

    def _on_clear_holidays(self):
        if not messagebox.askyesno("确认清空", "清空全部节假日数据（含板块映射）？不可恢复！"):
            return
        try:
            resp = api_request("DELETE", self.sqltool_api_var.get(), "/events/holidays")
            self._log(f"[清空] 节假日 -> {resp}")
            self.status_var.set(f"已清空节假日: {resp.get('cleared',0)} 条")
            self._on_refresh_db()
        except Exception as e:
            self._log(f"[清空失败] {e}")
            messagebox.showerror("清空失败", str(e))

    def _on_clear_events(self):
        if not messagebox.askyesno("确认清空", "清空全部重大事件数据（含板块映射）？不可恢复！"):
            return
        try:
            resp = api_request("DELETE", self.sqltool_api_var.get(), "/events/major")
            self._log(f"[清空] 重大事件 -> {resp}")
            self.status_var.set(f"已清空事件: {resp.get('cleared',0)} 条")
            self._on_refresh_db()
        except Exception as e:
            self._log(f"[清空失败] {e}")
            messagebox.showerror("清空失败", str(e))

    def _log(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert(tk.END, f"[{ts}] {msg}\n")
        self.log_text.see(tk.END)


if __name__ == "__main__":
    app = EventAITool()
    app.mainloop()
