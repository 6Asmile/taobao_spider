# taobao_spider_gui.py
import sys
import csv
import json
import re
import time
import httpx
import hashlib
import urllib.parse as urlparse
from math import ceil
from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit, QTextEdit,
    QPushButton, QListWidget, QListWidgetItem, QSpinBox, QFileDialog, QMessageBox,
    QComboBox
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal

# ------------------ 工具函数 ------------------
def remove_html_tags(text: str) -> str:
    txt = str(text)
    txt = re.sub(r'<[^>]+>', '', txt)
    txt = re.sub(r'\s+', ' ', txt).strip()
    return txt

def parse_cookies(cookie_str: str) -> dict:
    cookies = {}
    for kv in cookie_str.split(";"):
        if "=" in kv:
            k, v = kv.split("=", 1)
            cookies[k.strip()] = v.strip()
    return cookies

def parse_input(text: str):
    text = text.strip()
    if not text:
        return None
    if text.isdigit():
        return {"type": "id", "value": text}
    try:
        p = urlparse.urlparse(text)
        if p.scheme and p.netloc:
            qs = urlparse.parse_qs(p.query)
            if "id" in qs:
                return {"type": "id", "value": qs["id"][0]}
            if "itemId" in qs:
                return {"type": "id", "value": qs["itemId"][0]}
    except Exception:
        pass
    return {"type": "keyword", "value": text}

def sign_request(token, t, app_key, data_str):
    raw = f"{token}&{t}&{app_key}&{data_str}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()

def parse_jsonp_loose(text: str) -> dict:
    """
    更鲁棒的 JSONP 解析：寻找第一个 '{' 和最后一个 '}' 之间的 JSON 字符串并解析。
    """
    try:
        start = text.find('{')
        end = text.rfind('}')
        if start == -1 or end == -1 or end <= start:
            raise ValueError("无法定位 JSON 边界")
        json_str = text[start:end+1]
        return json.loads(json_str)
    except Exception as e:
        # 尝试用正则回退
        m = re.search(r'mtopjsonp\d*\((.*)\)\s*;?\s*$', text, re.S)
        if m:
            body = m.group(1).strip()
            try:
                return json.loads(body)
            except:
                # 尝试去掉尾部分号
                if body.endswith(';'):
                    body2 = body[:-1]
                    return json.loads(body2)
        raise RuntimeError(f"解析 JSONP 失败: {e}")

def normalize_value(val):
    """把复杂的 dict/list 展开为可读字符串"""
    if isinstance(val, str):
        return remove_html_tags(val)
    if val is None:
        return ""
    if isinstance(val, dict):
        # shopInfo 常见格式
        if "title" in val or "url" in val:
            title = val.get("title", "")
            url = val.get("url", "")
            if title and url:
                return f"{title} ({url})"
            return title or url or json.dumps(val, ensure_ascii=False)
        # 其他 dict 展开为 key:val; ...
        parts = []
        for k, v in val.items():
            parts.append(f"{k}:{normalize_value(v)}")
        return "; ".join(parts)
    if isinstance(val, (list, tuple)):
        parts = []
        for item in val:
            if isinstance(item, dict):
                # structuredUSPInfo 常用键 propertyName/propertyValueName
                if "propertyName" in item and "propertyValueName" in item:
                    parts.append(f"{item.get('propertyName')}={item.get('propertyValueName')}")
                else:
                    parts.append(";".join(f"{k}:{normalize_value(v)}" for k, v in item.items()))
            else:
                parts.append(normalize_value(item))
        return " | ".join(parts)
    # 其他类型
    return str(val)

# ------------------ 商品接口 ------------------
def build_params_data(keyword: str, page: int) -> dict:
    return {
        "device":"HMA-AL00","isBeta":"false","from":"nt_history","brand":"HUAWEI",
        "info":"wifi","index":"4","schemaType":"auction","client_os":"Android",
        "search_action":"initiative","sversion":"13.6","style":"list",
        "ttid":"600000@taobao_pc_10.7.0","needTabs":"true","areaCode":"CN","vm":"nw",
        "countryNum":"156","m":"pc_sem","page": str(page), "n": 48,
        "q": keyword, "qSource":"url","pageSource":"tbpc.pc_sem_alimama/a.search_history.d1",
        "tab":"all","pageSize":48,"sort":"_coefp"
    }

def get_goods_data(keyword: str, cookies: dict, page: int) -> dict:
    eP_data_dict = {"appId":"43356", "params": json.dumps(build_params_data(keyword, page), separators=(',', ':'))}
    eP_data = json.dumps(eP_data_dict, separators=(',', ':'))

    token_cookie = cookies.get("_m_h5_tk", "")
    if not token_cookie or "_" not in token_cookie:
        raise RuntimeError("Cookie 中未找到 _m_h5_tk（或格式不正确）。")
    token = token_cookie.split("_")[0]
    t = str(int(time.time() * 1000))
    app_key = "12574478"
    sign = sign_request(token, t, app_key, eP_data)

    url = "https://h5api.m.taobao.com/h5/mtop.relationrecommend.wirelessrecommend.recommend/2.0"
    headers = {"referer":"https://uland.taobao.com/","user-agent":"Mozilla/5.0"}
    params = {
        "jsv":"2.7.2","appKey":app_key,"t":t,"sign":sign,
        "api":"mtop.relationrecommend.wirelessrecommend.recommend","v":"2.0",
        "type":"jsonp","dataType":"jsonp","callback":"mtopjsonp4","data":eP_data
    }
    resp = httpx.get(url, headers=headers, cookies=cookies, params=params, timeout=30)
    resp.raise_for_status()
    return parse_jsonp_loose(resp.text)

# ------------------ 评论接口 ------------------
def get_comment_data(auction_id: str, cookies: dict, page: int, page_size: int = 20) -> dict:
    params_data = {
        "showTrueCount": False, "auctionNumId": str(auction_id),
        "pageNo": page, "pageSize": page_size, "rateType": "", "rateSrc": "pc_rate_list"
    }
    eP_data = json.dumps(params_data, separators=(',', ':'))

    token_cookie = cookies.get("_m_h5_tk", "")
    if not token_cookie or "_" not in token_cookie:
        raise RuntimeError("Cookie 中未找到 _m_h5_tk（或格式不正确）。")
    token = token_cookie.split("_")[0]
    t = str(int(time.time() * 1000))
    app_key = "12574478"
    sign = sign_request(token, t, app_key, eP_data)

    url = "https://h5api.m.tmall.com/h5/mtop.taobao.rate.detaillist.get/6.0/"
    headers = {"referer":"https://detail.tmall.com/","user-agent":"Mozilla/5.0"}
    params = {
        "jsv":"2.7.4","appKey":app_key,"t":t,"sign":sign,
        "api":"mtop.taobao.rate.detaillist.get","v":"6.0",
        "type":"jsonp","dataType":"jsonp","callback":"mtopjsonppcdetail4","data":eP_data
    }
    resp = httpx.get(url, headers=headers, cookies=cookies, params=params, timeout=30)
    resp.raise_for_status()
    return parse_jsonp_loose(resp.text)

# ------------------ 辅助提取函数 ------------------
def extract_comment_items(api_json: dict):
    """
    尽可能从评论接口返回的 json 中提取评论列表（兼容多种字段名/层次结构）
    返回 list 或空 list
    """
    d = api_json.get("data", {}) if isinstance(api_json, dict) else {}
    # 常见情况： data.rateDetail.rateList
    if isinstance(d.get("rateDetail"), dict):
        rd = d.get("rateDetail")
        for k in ("rateList", "rateDetailList", "rateListResult"):
            if k in rd and isinstance(rd[k], list):
                return rd[k]
    # data.rateList / data.rateDetailList
    for k in ("rateList", "rateDetailList", "rateDetail"):
        if k in d and isinstance(d[k], list):
            return d[k]
    if isinstance(d.get("rateDetail"), list):
        return d.get("rateDetail")
    # 尝试在 data 的任意 list 中寻找第一个元素为 dict（很可能是评论数组）
    for v in d.values():
        if isinstance(v, list) and v and isinstance(v[0], dict):
            return v
    return []

def extract_total_pages_for_comments(api_json: dict, page_size: int = 20):
    d = api_json.get("data", {}) if isinstance(api_json, dict) else {}
    # 优先使用 paginator
    paginator = None
    if isinstance(d.get("rateDetail"), dict):
        paginator = d["rateDetail"].get("paginator")
    if not paginator:
        paginator = d.get("paginator")
    if paginator and isinstance(paginator, dict):
        items = int(paginator.get("items", 0) or 0)
        page_size_val = int(paginator.get("pageSize", page_size) or page_size)
        if items:
            return max(1, (items + page_size_val - 1) // page_size_val)
    # fallback: try totalCount in rateDetail
    if isinstance(d.get("rateDetail"), dict):
        try:
            total = int(d["rateDetail"].get("totalCount", 0) or 0)
            if total:
                return max(1, (total + page_size - 1) // page_size)
        except:
            pass
    # 最后回退：1 页
    return 1

def extract_total_pages_for_goods(api_json: dict, page_size: int = 48):
    d = api_json.get("data", {}) if isinstance(api_json, dict) else {}
    main_info = d.get("mainInfo") or {}
    try:
        tp = int(main_info.get("totalPage") or d.get("totalPage") or 0)
        if tp:
            return max(1, tp)
    except:
        pass
    # fallback to totalResults
    try:
        total_results = int(d.get("totalResults") or 0)
        if total_results:
            return max(1, (total_results + page_size - 1) // page_size)
    except:
        pass
    return 1

def collect_fields(items):
    s = set()
    for it in items:
        if isinstance(it, dict):
            s.update(it.keys())
    return sorted(list(s))

# ------------------ Worker 线程 ------------------
class Worker(QThread):
    log_signal = pyqtSignal(str)

    def __init__(self, mode, input_val, cookies, fields, start_page, end_page, save_path):
        super().__init__()
        self.mode = mode
        self.input_val = input_val
        self.cookies = cookies
        self.fields = fields
        self.start_page = start_page
        self.end_page = end_page
        self.save_path = save_path

    def run(self):
        try:
            with open(self.save_path, "w", encoding="utf-8-sig", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=self.fields)
                writer.writeheader()
                for page in range(self.start_page, self.end_page + 1):
                    if self.mode == "商品信息":
                        self.log_signal.emit(f"[商品信息] 爬取第 {page} 页...")
                        data = get_goods_data(self.input_val, self.cookies, page)
                        items = data.get("data", {}).get("itemsArray") or data.get("data", {}).get("resultList") or []
                    else:
                        self.log_signal.emit(f"[评论] 爬取第 {page} 页...")
                        data = get_comment_data(self.input_val, self.cookies, page)
                        items = extract_comment_items(data)

                    for it in items:
                        row = {}
                        for field in self.fields:
                            row[field] = normalize_value(it.get(field, ""))
                        writer.writerow(row)
            self.log_signal.emit(f"✅ 导出成功：{self.save_path}")
        except Exception as e:
            self.log_signal.emit(f"❌ 爬取出错：{e}")

# ------------------ 主窗口 ------------------
class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("淘宝爬虫（商品信息 & 评论）")
        self.setGeometry(150, 150, 900, 700)

        v = QVBoxLayout()

        # 模式
        mode_h = QHBoxLayout()
        mode_h.addWidget(QLabel("模式:"))
        self.mode_combo = QComboBox()
        self.mode_combo.addItems(["商品信息", "评论"])
        mode_h.addWidget(self.mode_combo)
        v.addLayout(mode_h)

        # 输入框
        v.addWidget(QLabel("输入关键词 / 商品详情 URL / auctionNumId:"))
        self.input_edit = QLineEdit()
        v.addWidget(self.input_edit)

        # Cookie
        v.addWidget(QLabel("Cookie（必须包含 _m_h5_tk）："))
        self.cookie_edit = QTextEdit()
        v.addWidget(self.cookie_edit)

        # 页码
        page_h = QHBoxLayout()
        page_h.addWidget(QLabel("起始页:"))
        self.start_spin = QSpinBox(); self.start_spin.setMinimum(1)
        page_h.addWidget(self.start_spin)
        page_h.addWidget(QLabel("结束页:"))
        self.end_spin = QSpinBox(); self.end_spin.setMinimum(1)
        page_h.addWidget(self.end_spin)
        v.addLayout(page_h)

        # 字段列表（可拖动排序）
        v.addWidget(QLabel("字段（勾选并可拖动排序）："))
        self.field_list = QListWidget()
        self.field_list.setDragDropMode(QListWidget.InternalMove)
        v.addWidget(self.field_list)

        # 上下移动 + 选中控制
        op_h = QHBoxLayout()
        self.up_btn = QPushButton("上移")
        self.down_btn = QPushButton("下移")
        self.select_all_btn = QPushButton("全选")
        self.invert_btn = QPushButton("反选")
        op_h.addWidget(self.up_btn); op_h.addWidget(self.down_btn)
        op_h.addWidget(self.select_all_btn); op_h.addWidget(self.invert_btn)
        v.addLayout(op_h)

        # 操作按钮
        btn_h = QHBoxLayout()
        self.detect_btn = QPushButton("检测字段 / 获取总页数")
        self.path_btn = QPushButton("选择保存路径")
        self.start_btn = QPushButton("开始爬取并导出 CSV")
        btn_h.addWidget(self.detect_btn); btn_h.addWidget(self.path_btn); btn_h.addWidget(self.start_btn)
        v.addLayout(btn_h)

        # 日志
        v.addWidget(QLabel("日志:"))
        self.log_text = QTextEdit(); self.log_text.setReadOnly(True)
        v.addWidget(self.log_text)

        self.setLayout(v)
        self.save_path = ""
        self.parsed_input = None

        # 绑定
        self.detect_btn.clicked.connect(self.detect_fields)
        self.path_btn.clicked.connect(self.choose_save_path)
        self.start_btn.clicked.connect(self.start_crawl)
        self.up_btn.clicked.connect(self.move_up)
        self.down_btn.clicked.connect(self.move_down)
        self.select_all_btn.clicked.connect(self.select_all)
        self.invert_btn.clicked.connect(self.invert_select)

    def log(self, s: str):
        self.log_text.append(s)
        self.log_text.ensureCursorVisible()

    def choose_save_path(self):
        default = "taobao_result.csv"
        fn, _ = QFileDialog.getSaveFileName(self, "选择保存路径", default, "CSV Files (*.csv)")
        if fn:
            if not fn.lower().endswith(".csv"):
                fn += ".csv"
            self.save_path = fn
            self.log(f"保存路径已选择: {self.save_path}")

    def detect_fields(self):
        try:
            inp = parse_input(self.input_edit.text())
            if not inp:
                QMessageBox.warning(self, "提示", "请输入关键词/URL/ID。")
                return
            cookies = parse_cookies(self.cookie_edit.toPlainText().strip())
            if not cookies:
                QMessageBox.warning(self, "提示", "请粘贴 Cookie（必须包含 _m_h5_tk）。")
                return
            mode = self.mode_combo.currentText()

            items = []
            total_page = 1
            if mode == "商品信息":
                if inp["type"] != "keyword":
                    self.log("商品信息模式建议输入关键词以获取搜索结果（接受 ID/URL 但不保证精确匹配）。")
                data = get_goods_data(inp["value"], cookies, 1)
                items = data.get("data", {}).get("itemsArray") or data.get("data", {}).get("resultList") or []
                total_page = extract_total_pages_for_goods(data, page_size=48)
            else:
                if inp["type"] != "id":
                    QMessageBox.warning(self, "提示", "评论模式请填写商品详情页 URL 或 auctionNumId（纯数字）。")
                    return
                data = get_comment_data(inp["value"], cookies, 1)
                items = extract_comment_items(data)
                total_page = extract_total_pages_for_comments(data, page_size=20)

            if not items:
                self.log("检测到 items 为空 — 可能是 Cookie 无效、被风控，或该页无数据。仍将列出空字段集。")

            fields = collect_fields(items)
            self.field_list.clear()
            for f in fields:
                it = QListWidgetItem(f)
                it.setFlags(it.flags() | Qt.ItemIsUserCheckable | Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsDragEnabled)
                it.setCheckState(Qt.Unchecked)
                self.field_list.addItem(it)

            # set page spinboxes
            self.start_spin.setMaximum(max(1, total_page))
            self.end_spin.setMaximum(max(1, total_page))
            if self.end_spin.value() > total_page:
                self.end_spin.setValue(total_page)

            self.parsed_input = inp
            self.log(f"字段检测完成：{len(fields)} 个字段，可用总页数：{total_page}")
        except Exception as e:
            QMessageBox.critical(self, "错误", f"检测失败：{e}")
            self.log(f"检测字段出错：{e}")

    def move_up(self):
        r = self.field_list.currentRow()
        if r > 0:
            it = self.field_list.takeItem(r)
            self.field_list.insertItem(r-1, it)
            self.field_list.setCurrentRow(r-1)

    def move_down(self):
        r = self.field_list.currentRow()
        if r >= 0 and r < self.field_list.count()-1:
            it = self.field_list.takeItem(r)
            self.field_list.insertItem(r+1, it)
            self.field_list.setCurrentRow(r+1)

    def select_all(self):
        for i in range(self.field_list.count()):
            self.field_list.item(i).setCheckState(Qt.Checked)

    def invert_select(self):
        for i in range(self.field_list.count()):
            it = self.field_list.item(i)
            it.setCheckState(Qt.Unchecked if it.checkState() == Qt.Checked else Qt.Checked)

    def start_crawl(self):
        try:
            if not self.save_path:
                QMessageBox.warning(self, "提示", "请先选择保存路径。")
                return
            cookies = parse_cookies(self.cookie_edit.toPlainText().strip())
            if not cookies:
                QMessageBox.warning(self, "提示", "请粘贴 Cookie（必须包含 _m_h5_tk）。")
                return
            inp = self.parsed_input or parse_input(self.input_edit.text())
            if not inp:
                QMessageBox.warning(self, "提示", "请输入关键词/URL/ID。")
                return
            mode = self.mode_combo.currentText()
            if mode == "评论" and inp["type"] != "id":
                QMessageBox.warning(self, "提示", "评论模式请填写商品详情页 URL 或 auctionNumId（纯数字）。")
                return

            # collect selected fields in current order
            fields = []
            for i in range(self.field_list.count()):
                it = self.field_list.item(i)
                if it.checkState() == Qt.Checked:
                    fields.append(it.text())
            if not fields:
                QMessageBox.warning(self, "提示", "请至少勾选一个字段。")
                return

            start_page = self.start_spin.value()
            end_page = self.end_spin.value()
            if start_page > end_page:
                QMessageBox.warning(self, "提示", "起始页不能大于结束页。")
                return

            # 启动 Worker 线程
            target_val = inp["value"]
            self.worker = Worker(mode, target_val, cookies, fields, start_page, end_page, self.save_path)
            self.worker.log_signal.connect(self.log)
            self.worker.start()
        except Exception as e:
            QMessageBox.critical(self, "错误", f"启动爬取失败：{e}")
            self.log(f"启动爬取失败：{e}")

# ------------------ main ------------------
if __name__ == "__main__":
    app = QApplication(sys.argv)
    w = MainWindow()
    w.show()
    sys.exit(app.exec_())
