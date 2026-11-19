#!/usr/bin/env python3
"""真实调用编排函数执行 7 个失败注入场景，核对响应终态、LTS 编排轨迹与 GeminiDB 三表数据，
并对成功场景重复触发一次验证 UUIDv5 幂等覆盖写，写 verification-result.json。

运行方式（部署后）：
    python scripts/verify_e2e.py
华为云 API 签名密钥读取 ../../credentials.csv 第 1 行（row[1]=AK、row[2]=SK）；
GeminiDB 数据面凭据经 GEMINIDB_* 环境变量注入（用户名固定 rwuser、密码为实例管理员密码）。
"""
import argparse
import csv
import hashlib
import hmac
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from urllib.parse import quote

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MODULE_DIR = os.path.dirname(SCRIPT_DIR)
FIXTURES_DIR = os.path.join(MODULE_DIR, "fixtures")
RESULT_FILE = os.path.join(MODULE_DIR, "verification-result.json")
CREDENTIALS_CSV = os.path.join(os.path.dirname(os.path.dirname(MODULE_DIR)), "credentials.csv")

TRANSACTION_NAMESPACE = uuid.UUID("8b03bc51-6db8-4d24-9e5b-0f8245f5f42e")


def transaction_id(order_id, transaction_type):
    """对齐源仓库 models.TransactionID：uuid5(namespace, order_id + "|" + transaction_type)。"""
    return str(uuid.uuid5(TRANSACTION_NAMESPACE, order_id + "|" + transaction_type))


def scenario_payload(index):
    with open(os.path.join(FIXTURES_DIR, "scenario-%d.json" % index), encoding="utf-8") as handle:
        return json.load(handle)


# 每个场景的期望编排轨迹：(eventType, step) 序列；SAGA_*/NOTIFIED 事件 step 为 None。
EXPECTED_TRACES = {
    1: [("SAGA_STARTED", None),
        ("STEP_STARTED", "order-new"), ("STEP_FAILED", "order-new"),
        ("COMPENSATING", "order-update"), ("COMPENSATED", "order-update"),
        ("SAGA_FAILED", None)],
    2: [("SAGA_STARTED", None),
        ("STEP_STARTED", "order-new"), ("STEP_FAILED", "order-new"),
        ("COMPENSATING", "order-update"), ("STEP_ERROR", "order-update"),
        ("NOTIFIED", None), ("SAGA_FAILED", None)],
    3: [("SAGA_STARTED", None),
        ("STEP_STARTED", "order-new"), ("STEP_SUCCEEDED", "order-new"),
        ("STEP_STARTED", "payment-debit"), ("STEP_FAILED", "payment-debit"),
        ("COMPENSATING", "payment-credit"), ("COMPENSATED", "payment-credit"),
        ("COMPENSATING", "order-update"), ("COMPENSATED", "order-update"),
        ("SAGA_FAILED", None)],
    4: [("SAGA_STARTED", None),
        ("STEP_STARTED", "order-new"), ("STEP_SUCCEEDED", "order-new"),
        ("STEP_STARTED", "payment-debit"), ("STEP_FAILED", "payment-debit"),
        ("COMPENSATING", "payment-credit"), ("STEP_ERROR", "payment-credit"),
        ("NOTIFIED", None), ("SAGA_FAILED", None)],
    5: [("SAGA_STARTED", None),
        ("STEP_STARTED", "order-new"), ("STEP_SUCCEEDED", "order-new"),
        ("STEP_STARTED", "payment-debit"), ("STEP_SUCCEEDED", "payment-debit"),
        ("STEP_STARTED", "inventory-reserve"), ("STEP_FAILED", "inventory-reserve"),
        ("COMPENSATING", "inventory-release"), ("COMPENSATED", "inventory-release"),
        ("COMPENSATING", "payment-credit"), ("COMPENSATED", "payment-credit"),
        ("COMPENSATING", "order-update"), ("COMPENSATED", "order-update"),
        ("SAGA_FAILED", None)],
    6: [("SAGA_STARTED", None),
        ("STEP_STARTED", "order-new"), ("STEP_SUCCEEDED", "order-new"),
        ("STEP_STARTED", "payment-debit"), ("STEP_SUCCEEDED", "payment-debit"),
        ("STEP_STARTED", "inventory-reserve"), ("STEP_FAILED", "inventory-reserve"),
        ("COMPENSATING", "inventory-release"), ("STEP_ERROR", "inventory-release"),
        ("NOTIFIED", None), ("SAGA_FAILED", None)],
    7: [("SAGA_STARTED", None),
        ("STEP_STARTED", "order-new"), ("STEP_SUCCEEDED", "order-new"),
        ("STEP_STARTED", "payment-debit"), ("STEP_SUCCEEDED", "payment-debit"),
        ("STEP_STARTED", "inventory-reserve"), ("STEP_SUCCEEDED", "inventory-reserve"),
        ("NOTIFIED", None), ("SAGA_SUCCEEDED", None)],
}

# 每个场景的期望响应终态（编排函数返回体）。
EXPECTED_RESPONSES = {
    1: {"status": "failed", "failedStep": "order-new", "errorType": "ErrProcessOrder",
        "completedSteps": [], "compensatedSteps": ["order-update"], "notified": None},
    2: {"status": "failed", "failedStep": "order-new", "errorType": "ErrProcessOrder",
        "completedSteps": [], "compensatedSteps": [],
        "compensationErrorType": "ErrUpdateOrderStatus", "notified": True},
    3: {"status": "failed", "failedStep": "payment-debit", "errorType": "ErrProcessPayment",
        "completedSteps": ["order-new"], "compensatedSteps": ["payment-credit", "order-update"], "notified": None},
    4: {"status": "failed", "failedStep": "payment-debit", "errorType": "ErrProcessPayment",
        "completedSteps": ["order-new"], "compensatedSteps": [],
        "compensationErrorType": "ErrProcessRefund", "notified": True},
    5: {"status": "failed", "failedStep": "inventory-reserve", "errorType": "ErrReserveInventory",
        "completedSteps": ["order-new", "payment-debit"],
        "compensatedSteps": ["inventory-release", "payment-credit", "order-update"], "notified": None},
    6: {"status": "failed", "failedStep": "inventory-reserve", "errorType": "ErrReserveInventory",
        "completedSteps": ["order-new", "payment-debit"], "compensatedSteps": [],
        "compensationErrorType": "ErrReleaseInventory", "notified": True},
    7: {"status": "success", "failedStep": None,
        "completedSteps": ["order-new", "payment-debit", "inventory-reserve"],
        "compensatedSteps": [], "notified": None},
}


def fail(message):
    print("[ERROR] %s" % message, file=sys.stderr)
    sys.exit(1)


def terraform(*tf_args):
    tf = shutil.which("terraform") or shutil.which("terraform.exe")
    if not tf:
        candidates = [
            "C:/Users/Administrator/AppData/Local/Temp/terraform.exe",
            "C:/terraform/terraform.exe",
            "/tmp/terraform.exe",
        ]
        tf = next((item for item in candidates if os.path.isfile(item)), None)
    if not tf:
        fail("未找到 terraform 可执行文件")
    result = subprocess.run([tf] + list(tf_args), cwd=MODULE_DIR, capture_output=True)
    return result.returncode, result.stdout.decode("utf-8", "replace"), result.stderr.decode("utf-8", "replace")


def terraform_outputs():
    code, out, err = terraform("output", "-json")
    if code != 0:
        fail("terraform output 失败: %s" % err[:300])
    return {key: value["value"] for key, value in json.loads(out).items()}


def load_account_credentials():
    """读 credentials.csv 第 1 行：row[1]=AK、row[2]=SK（华为云 API 签名用）。"""
    path = os.environ.get("HW_CREDENTIALS_CSV", CREDENTIALS_CSV)
    if not os.path.isfile(path):
        fail("缺少访问密钥文件 %s" % path)
    with open(path, newline="", encoding="utf-8-sig") as handle:
        row = next(csv.reader(handle), None)
    if not row or len(row) < 3 or not row[1] or not row[2]:
        fail("credentials.csv 第 1 行缺少 AK/SK")
    return row[1].strip(), row[2].strip()


def signed_request(method, host, path, payload, access_key, secret_key, project_id=None):
    body = json.dumps(payload).encode("utf-8") if payload is not None else b""
    sdk_date = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    signed = {"content-type": "application/json", "host": host, "x-sdk-date": sdk_date}
    if project_id:
        signed["x-project-id"] = project_id
    canonical_uri = "/" + "/".join(quote(part, safe="") for part in path.strip("/").split("/")) + "/"
    signed_headers = ";".join(sorted(signed))
    canonical_headers = "".join("%s:%s\n" % (key, signed[key]) for key in sorted(signed))
    canonical_request = "\n".join([
        method, canonical_uri, "", canonical_headers, signed_headers, hashlib.sha256(body).hexdigest(),
    ])
    string_to_sign = "\n".join([
        "SDK-HMAC-SHA256", sdk_date, hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
    ])
    signature = hmac.new(secret_key.encode("utf-8"), string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
    headers = {
        "Content-Type": "application/json",
        "X-Sdk-Date": sdk_date,
        "Authorization": "SDK-HMAC-SHA256 Access=%s, SignedHeaders=%s, Signature=%s" % (
            access_key, signed_headers, signature),
    }
    if project_id:
        headers["X-Project-Id"] = project_id
    request = urllib.request.Request("https://%s%s" % (host, path), data=body or None, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.status, response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode("utf-8", "replace")


def invoke(function_urn, event, access_key, secret_key):
    parts = function_urn.split(":")
    region, project_id = parts[2], parts[3]
    host = "functiongraph.%s.myhuaweicloud.com" % region
    path = "/v2/%s/fgs/functions/%s/invocations" % (project_id, function_urn)
    return signed_request("POST", host, path, event, access_key, secret_key, project_id)


def query_lts(region, project_id, group_id, stream_id, start_ms, access_key, secret_key):
    host = "lts.%s.myhuaweicloud.com" % region
    path = "/v2/%s/groups/%s/streams/%s/content/query" % (project_id, group_id, stream_id)
    payload = {
        "start_time": str(start_ms),
        "end_time": str(int(time.time() * 1000)),
        "limit": 1000,
        "is_desc": False,
    }
    status, text = signed_request("POST", host, path, payload, access_key, secret_key)
    if status != 200:
        print("[WARN] LTS 查询 HTTP %s: %s" % (status, text[:300]))
        return []
    return json.loads(text).get("logs", [])


def saga_records(logs):
    records = []
    marker = "[SAGA] "
    for item in logs:
        content = item.get("content", "")
        for line in content.splitlines():
            if marker not in line:
                continue
            raw = line.split(marker, 1)[1].strip()
            try:
                records.append(json.loads(raw))
            except json.JSONDecodeError:
                pass
    return records


def trace_for(records, order_id):
    return [item for item in records if item.get("orderId") == order_id]


# ---- GeminiDB（DynamoDB 兼容接口）数据面客户端：SigV4 静态凭据（rwuser + 实例管理员密码） ----
class GeminiDBError(RuntimeError):
    def __init__(self, operation, status, code, message=""):
        detail = " (%s)" % message if message else ""
        super().__init__("GeminiDB %s failed with HTTP %s: %s%s" % (operation, status, code, detail))
        self.code = code


class GeminiDBClient:
    def __init__(self):
        endpoint = os.environ.get("GEMINIDB_ENDPOINT", "")
        if not endpoint:
            fail("缺少 GEMINIDB_ENDPOINT 环境变量（外部预置的 GeminiDB 兼容 DynamoDB 实例内网地址）")
        protocol = "http"
        if "://" in endpoint:
            protocol, endpoint = endpoint.split("://", 1)
        host_part = endpoint.split("/", 1)[0]
        if ":" in host_part:
            host, port = host_part.rsplit(":", 1)
            port = int(port)
        else:
            host = host_part
            port = int(os.environ.get("GEMINIDB_PORT", "80"))
        if not host:
            fail("GEMINIDB_ENDPOINT 必须包含主机名")
        self.protocol = protocol
        self.host = host
        self.port = port
        self.host_header = "%s:%d" % (host, port)
        self.access_key = os.environ.get("GEMINIDB_ACCESS_KEY", "rwuser")
        self.secret_key = os.environ.get("GEMINIDB_SECRET_KEY", "")
        self.region = os.environ.get("GEMINIDB_REGION", "cn-north-4")
        if not self.secret_key:
            fail("缺少 GEMINIDB_SECRET_KEY 环境变量（实例管理员密码）")

    @staticmethod
    def _signing_key(secret_key, date, region):
        def sign(key, value):
            return hmac.new(key, value.encode("utf-8"), hashlib.sha256).digest()

        key = sign(("AWS4" + secret_key).encode("utf-8"), date)
        key = sign(key, region)
        key = sign(key, "dynamodb")
        return sign(key, "aws4_request")

    def _call(self, operation, payload):
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        payload_hash = hashlib.sha256(body).hexdigest()
        now = time.gmtime()
        date = time.strftime("%Y%m%d", now)
        amz_date = time.strftime("%Y%m%dT%H%M%SZ", now)
        headers = {
            "content-type": "application/x-amz-json-1.0",
            "host": self.host_header,
            "x-amz-content-sha256": payload_hash,
            "x-amz-date": amz_date,
            "x-amz-target": "DynamoDB_20120810.%s" % operation,
        }
        canonical_headers = "".join("%s:%s\n" % (key, headers[key]) for key in sorted(headers))
        signed_headers = ";".join(sorted(headers))
        canonical_request = "\n".join(["POST", "/", "", canonical_headers, signed_headers, payload_hash])
        scope = "%s/%s/dynamodb/aws4_request" % (date, self.region)
        string_to_sign = "\n".join(
            ["AWS4-HMAC-SHA256", amz_date, scope, hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()]
        )
        signature = hmac.new(
            self._signing_key(self.secret_key, date, self.region),
            string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
        authorization = ("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s"
                         % (self.access_key, scope, signed_headers, signature))
        request = urllib.request.Request(
            "%s://%s/" % (self.protocol, self.host_header), data=body, method="POST",
            headers={
                "Content-Type": headers["content-type"],
                "Host": self.host_header,
                "X-Amz-Content-Sha256": payload_hash,
                "X-Amz-Date": amz_date,
                "X-Amz-Target": headers["x-amz-target"],
                "Authorization": authorization,
            })
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                raw = response.read().decode("utf-8")
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as exc:
            raw = exc.read(1024).decode("utf-8", "replace")
            code = "HTTPError"
            message = ""
            try:
                error = json.loads(raw)
                code = str(error.get("__type") or error.get("code") or code).split("#")[-1]
                message = str(error.get("message") or "")
            except (TypeError, ValueError):
                pass
            raise GeminiDBError(operation, exc.code, code, message) from exc

    def ensure_table(self, table_name, key_name):
        """幂等建表（对齐源模板三张按需表的主键；GSI 不建，见 CONVERSION-NOTES）。"""
        try:
            self._call("CreateTable", {
                "TableName": table_name,
                "KeySchema": [{"AttributeName": key_name, "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": key_name, "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST",
            })
        except GeminiDBError as exc:
            if exc.code not in {"ResourceInUseException", "TableAlreadyExistsException", "TableInUseException"}:
                raise
            print("[DB] 表 %s 已存在，跳过建表" % table_name)

    @staticmethod
    def _plain(value):
        if isinstance(value, dict):
            if "S" in value:
                return value["S"]
            if "N" in value:
                return float(value["N"])
            if "BOOL" in value:
                return bool(value["BOOL"])
            if "NULL" in value:
                return None
            if "L" in value:
                return [GeminiDBClient._plain(item) for item in value["L"]]
            if "M" in value:
                return {key: GeminiDBClient._plain(item) for key, item in value["M"].items()}
        return value

    def scan_plains(self, table_name):
        payload = {"TableName": table_name}
        items = []
        while True:
            result = self._call("Scan", payload)
            items.extend(result.get("Items", []))
            last_key = result.get("LastEvaluatedKey")
            if not last_key:
                break
            payload["ExclusiveStartKey"] = last_key
        return [{key: self._plain(value) for key, value in item.items()} for item in items]

    def rows_for_order(self, table_name, order_id):
        return [row for row in self.scan_plains(table_name) if row.get("order_id") == order_id]


def assert_response(index, body):
    expected = EXPECTED_RESPONSES[index]
    if body.get("status") != expected["status"]:
        raise AssertionError("场景 %d 终态不符: %s" % (index, body))
    if body.get("failedStep") != expected["failedStep"]:
        raise AssertionError("场景 %d 失败步骤不符: %s" % (index, body.get("failedStep")))
    if body.get("completedSteps") != expected["completedSteps"]:
        raise AssertionError("场景 %d 已完成步骤不符: %s" % (index, body.get("completedSteps")))
    if body.get("compensatedSteps") != expected["compensatedSteps"]:
        raise AssertionError("场景 %d 补偿清单不符: %s" % (index, body.get("compensatedSteps")))
    error = body.get("error") or {}
    if expected.get("errorType") and error.get("errorType") != expected["errorType"]:
        raise AssertionError("场景 %d 失败类型不符: %s" % (index, error))
    comp_error = body.get("compensationError") or {}
    if expected.get("compensationErrorType") and comp_error.get("errorType") != expected["compensationErrorType"]:
        raise AssertionError("场景 %d 补偿失败类型不符: %s" % (index, comp_error))
    if expected.get("notified") is not None and bool(body.get("notified")) != expected["notified"]:
        raise AssertionError("场景 %d 通知标志不符: %s" % (index, body.get("notified")))


def assert_trace(index, trace):
    observed = [(item.get("eventType"), item.get("step")) for item in trace]
    expected = EXPECTED_TRACES[index]
    if observed != expected:
        raise AssertionError("场景 %d 轨迹不符，期望=%s，实际=%s" % (index, expected, observed))


def assert_payment_rows(index, rows, order_id, expected_types, total=None):
    """支付表断言：交易类型集合、确定性 UUIDv5、Debit/Credit 金额互补。"""
    by_type = {}
    for row in rows:
        payment_type = row.get("payment_type")
        if payment_type in by_type:
            raise AssertionError("场景 %d 支付表存在重复 %s 记录: %s" % (index, payment_type, rows))
        by_type[payment_type] = row
    if sorted(by_type.keys()) != sorted(expected_types):
        raise AssertionError("场景 %d 支付表记录类型不符，期望=%s，实际=%s" % (index, expected_types, sorted(by_type.keys())))
    for payment_type in expected_types:
        row = by_type[payment_type]
        if row.get("transaction_id") != transaction_id(order_id, payment_type):
            raise AssertionError("场景 %d 支付表 %s 事务 ID 与 UUIDv5 期望不符: %s" % (index, payment_type, row))
    if "Debit" in by_type and "Credit" in by_type:
        if by_type["Debit"].get("transaction_id") == by_type["Credit"].get("transaction_id"):
            raise AssertionError("场景 %d debit 与 credit 事务 ID 相同（对齐 TestDebitAndCreditIDsDiffer 应不同）" % index)
        if abs(by_type["Credit"].get("payment_amount", 0.0) + by_type["Debit"].get("payment_amount", 0.0)) > 1e-9:
            raise AssertionError("场景 %d credit 金额未取负抵消 debit: %s" % (index, by_type))
    if total is not None and "Debit" in by_type:
        if abs(by_type["Debit"].get("payment_amount", 0.0) - total) > 1e-9:
            raise AssertionError("场景 %d debit 金额与订单总额不符: %s" % (index, by_type["Debit"]))
    return by_type


def assert_inventory_rows(index, rows, order_id, expected_types, item_ids=None):
    by_type = {}
    for row in rows:
        transaction_type = row.get("transaction_type")
        if transaction_type in by_type:
            raise AssertionError("场景 %d 库存表存在重复 %s 记录: %s" % (index, transaction_type, rows))
        by_type[transaction_type] = row
    if sorted(by_type.keys()) != sorted(expected_types):
        raise AssertionError("场景 %d 库存表记录类型不符，期望=%s，实际=%s" % (index, expected_types, sorted(by_type.keys())))
    for transaction_type in expected_types:
        row = by_type[transaction_type]
        if row.get("transaction_id") != transaction_id(order_id, transaction_type):
            raise AssertionError("场景 %d 库存表 %s 事务 ID 与 UUIDv5 期望不符: %s" % (index, transaction_type, row))
    if "Reserve" in by_type and "Release" in by_type:
        if by_type["Reserve"].get("transaction_id") == by_type["Release"].get("transaction_id"):
            raise AssertionError("场景 %d reserve 与 release 事务 ID 相同（应不同）" % index)
    if item_ids is not None:
        for transaction_type in expected_types:
            if (by_type[transaction_type].get("items") or []) != item_ids:
                raise AssertionError("场景 %d 库存表 %s 商品清单不符: %s" % (index, transaction_type, by_type[transaction_type]))
    return by_type


def assert_database(index, client, tables, payload):
    """按场景断言三表落库结果（对齐源 testdata 语义与 guide.md 的失败注入规则）。"""
    order_id = payload["order_id"]
    total = sum(item["unit_price"] * item["qty"] for item in payload["items"])
    item_ids = [item["item_id"] for item in payload["items"]]

    order_rows = client.rows_for_order(tables["orders"], order_id)
    payment_rows = client.rows_for_order(tables["payments"], order_id)
    inventory_rows = client.rows_for_order(tables["inventory"], order_id)

    summary = {"orders": len(order_rows), "payments": len(payment_rows), "inventory": len(inventory_rows)}

    if len(order_rows) != 1:
        raise AssertionError("场景 %d 订单表应恰有 1 行，实际 %d 行" % (index, len(order_rows)))
    order_status = order_rows[0].get("order_status")

    if index == 7:
        if order_status != "New":
            raise AssertionError("场景 7 订单状态应为 New: %s" % order_rows[0])
        assert_payment_rows(index, payment_rows, order_id, ["Debit"], total=total)
        assert_inventory_rows(index, inventory_rows, order_id, ["Reserve"], item_ids=item_ids)
    elif index in (1, 2):
        # order-new 失败（写库后注入）→ order-update 补偿置 Pending；
        # 场景 2 补偿自身失败，但状态已在注入前写入，仍为 Pending。
        if order_status != "Pending":
            raise AssertionError("场景 %d 订单状态应为 Pending: %s" % (index, order_rows[0]))
        if payment_rows or inventory_rows:
            raise AssertionError("场景 %d 不应产生支付/库存记录: %s %s" % (index, payment_rows, inventory_rows))
    elif index == 3:
        if order_status != "Pending":
            raise AssertionError("场景 3 订单状态应为 Pending: %s" % order_rows[0])
        assert_payment_rows(index, payment_rows, order_id, ["Credit", "Debit"], total=total)
        if inventory_rows:
            raise AssertionError("场景 3 不应产生库存记录: %s" % inventory_rows)
    elif index == 4:
        # payment-debit 失败 → payment-credit 补偿写 Credit 后自身失败，order-update 被中止 → 订单仍为 New。
        if order_status != "New":
            raise AssertionError("场景 4 订单状态应为 New: %s" % order_rows[0])
        assert_payment_rows(index, payment_rows, order_id, ["Credit", "Debit"], total=total)
        if inventory_rows:
            raise AssertionError("场景 4 不应产生库存记录: %s" % inventory_rows)
    elif index == 5:
        if order_status != "Pending":
            raise AssertionError("场景 5 订单状态应为 Pending: %s" % order_rows[0])
        assert_payment_rows(index, payment_rows, order_id, ["Credit", "Debit"], total=total)
        assert_inventory_rows(index, inventory_rows, order_id, ["Release", "Reserve"], item_ids=item_ids)
    elif index == 6:
        # inventory-reserve 失败 → inventory-release 补偿写 Release 后自身失败，剩余补偿被中止 → 订单仍为 New、仅 Debit。
        if order_status != "New":
            raise AssertionError("场景 6 订单状态应为 New: %s" % order_rows[0])
        assert_payment_rows(index, payment_rows, order_id, ["Debit"], total=total)
        assert_inventory_rows(index, inventory_rows, order_id, ["Release", "Reserve"], item_ids=item_ids)

    summary["order_status"] = order_status
    summary["payment_types"] = sorted(row.get("payment_type") for row in payment_rows)
    summary["inventory_types"] = sorted(row.get("transaction_type") for row in inventory_rows)
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--interval", type=int, default=8)
    args = parser.parse_args()

    access_key, secret_key = load_account_credentials()
    outputs = terraform_outputs()
    urn = outputs["orchestrator_function_urn"]
    region, project_id = urn.split(":")[2:4]
    tables = outputs["saga_table_names"]
    topic_urn = outputs["smn_topic_urn"]

    client = GeminiDBClient()
    # 幂等确保三张业务表存在（主键对齐源模板；GSI 差异见 CONVERSION-NOTES）。
    client.ensure_table(tables["orders"], "order_id")
    client.ensure_table(tables["payments"], "transaction_id")
    client.ensure_table(tables["inventory"], "transaction_id")

    start_ms = int(time.time() * 1000) - 30_000
    cases = {}

    for index in range(1, 8):
        payload = scenario_payload(index)
        # 委托策略传播抖动时，内层步骤调用报 FSS.0403/user not permitted，退避重试。
        body = None
        for attempt in range(1, 11):
            status, text = invoke(urn, payload, access_key, secret_key)
            if "FSS.0403" in text or "user not permitted" in text:
                print("[RETRY-IAM] 场景 %d 第 %d 次策略未传播，15s 后重试" % (index, attempt))
                time.sleep(15)
                continue
            break
        print("[INVOKE] scenario=%d orderId=%s HTTP=%s" % (index, payload["order_id"], status))
        print("[RESPONSE] %s" % text.strip()[:500])
        if status != 200:
            raise AssertionError("场景 %d 调用 HTTP 状态异常: %s %s" % (index, status, text[:300]))
        body = json.loads(text)
        if body.get("errorMessage"):
            raise AssertionError("场景 %d 编排函数执行失败: %s" % (index, body))
        assert_response(index, body)
        cases[index] = {"payload": payload, "response": body}

    # 等待并核对 LTS 编排轨迹。
    deadline = time.time() + args.timeout
    traces = {}
    while time.time() < deadline:
        logs = query_lts(region, project_id, outputs["lts_log_group_id"],
                         outputs["lts_stream_ids"]["orchestrator"], start_ms, access_key, secret_key)
        records = saga_records(logs)
        traces = {index: trace_for(records, item["payload"]["order_id"]) for index, item in cases.items()}
        if all(traces.values()):
            break
        print("[WAIT-LTS] 已找到 %d/7 条 Saga 轨迹" % sum(bool(value) for value in traces.values()))
        time.sleep(args.interval)

    for index in range(1, 8):
        trace = traces.get(index) or []
        if not trace:
            raise AssertionError("LTS 未找到场景 %d 的 Saga 轨迹" % index)
        assert_trace(index, trace)
        print("[LTS-TRACE] scenario=%d orderId=%s" % (index, cases[index]["payload"]["order_id"]))
        for record in trace:
            details = {k: v for k, v in record.items() if k not in ("eventType", "requestId")}
            print("  %s | %s" % (record["eventType"], json.dumps(details, ensure_ascii=False)))

    # 数据库断言（直查 GeminiDB 三表，按 order_id 过滤）。
    db_summaries = {}
    for index in range(1, 8):
        db_summaries[index] = assert_database(index, client, tables, cases[index]["payload"])
        print("[DB-CHECK] scenario=%d %s" % (index, json.dumps(db_summaries[index], ensure_ascii=False)))

    # 幂等断言：成功场景同一 order_id 重复触发一次，三表行数不变（UUIDv5 覆盖写）。
    idempotency = {"scenario": 7}
    repeat_payload = cases[7]["payload"]
    order_id = repeat_payload["order_id"]
    before = {name: len(client.rows_for_order(table, order_id)) for name, table in tables.items()}
    status, text = invoke(urn, repeat_payload, access_key, secret_key)
    if status != 200:
        raise AssertionError("幂等复触发调用 HTTP 状态异常: %s" % status)
    repeat_body = json.loads(text)
    if repeat_body.get("errorMessage") or repeat_body.get("status") != "success":
        raise AssertionError("幂等复触发未再次成功: %s" % text[:300])
    after = {name: len(client.rows_for_order(table, order_id)) for name, table in tables.items()}
    if before != after:
        raise AssertionError("幂等复触发后三表行数变化: %s -> %s" % (before, after))
    debit = [row for row in client.rows_for_order(tables["payments"], order_id) if row.get("payment_type") == "Debit"]
    if len(debit) != 1 or debit[0].get("transaction_id") != transaction_id(order_id, "Debit"):
        raise AssertionError("幂等复触发后 Debit 记录数或确定性事务 ID 不符: %s" % debit)
    idempotency.update({"orderId": order_id, "rowsBefore": before, "rowsAfter": after,
                        "reinvokeStatus": repeat_body.get("status"), "verified": True})
    print("[IDEMPOTENCY] %s" % json.dumps(idempotency, ensure_ascii=False))

    result = {
        "verified_at": datetime.now(timezone.utc).isoformat(),
        "region": region,
        "project_id": project_id,
        "orchestrator_function_name": outputs["orchestrator_function_name"],
        "orchestrator_function_urn": urn,
        "smn_topic_urn": topic_urn,
        "function_spec": "步骤函数 128MB / 15s / Node.js18.15；编排函数 128MB / 180s / Node.js18.15",
        "lts_log_group_id": outputs["lts_log_group_id"],
        "lts_orchestrator_stream_id": outputs["lts_stream_ids"]["orchestrator"],
        "saga_table_names": tables,
        "agency_name": outputs["agency_name"],
        "orchestration_note": (
            "华为云 FunctionGraph 函数流服务在线在售，但 Terraform provider 暂无工作流资源，"
            "纯 IaC 交付采用函数内编排：编排函数在一次同步调用内按序调用 "
            "order-new → payment-debit → inventory-reserve 三个正向事务步骤函数"
            "（函数间使用委托 token 经 FunctionGraph invocations 接口同步调用，"
            "瞬态错误按 FULL jitter 退避重试），任一步骤业务失败按失败步骤及其之前步骤的逆序执行补偿"
            "（inventory-release → payment-credit → order-update）；补偿自身失败时发布失败通知并中止剩余补偿，"
            "对齐源状态机的 Catch → Notify → OrderFailed 语义；正向业务失败但补偿全部成功时不发通知，"
            "对齐源状态机 UpdateOrderStatus → OrderFailed 的直连语义。"
            "完整编排轨迹以 [SAGA] 结构化日志写入显式绑定的 LTS 日志流核对。"
        ),
        "invoke_cases": [
            {
                "case": "scenario-%d" % index,
                "orderId": cases[index]["payload"]["order_id"],
                "http_status": 200,
                "response_status": cases[index]["response"].get("status"),
                "failedStep": cases[index]["response"].get("failedStep"),
                "error": cases[index]["response"].get("error"),
                "compensationError": cases[index]["response"].get("compensationError"),
                "completedSteps": cases[index]["response"].get("completedSteps"),
                "compensatedSteps": cases[index]["response"].get("compensatedSteps"),
                "notified": bool(cases[index]["response"].get("notified")),
                "lts_trace_events": len(EXPECTED_TRACES[index]),
                "lts_trace_verified": True,
                "database": db_summaries[index],
            }
            for index in range(1, 8)
        ],
        "lts_trace_check": (
            "scripts/verify_e2e.py 按 orderId 定位七次执行，从 LTS 还原的 (eventType, step) 序列"
            "与期望轨迹逐项相等（场景 1 六条、场景 2 七条、场景 3 十条、场景 4 九条、"
            "场景 5 十四条、场景 6 十一条、场景 7 九条）。"
        ),
        "database_check": (
            "GeminiDB 兼容 DynamoDB 数据面按 SigV4 静态凭据直查三表：成功场景三表记录齐且状态为 New；"
            "payment-debit 失败场景 Debit 与 Credit 两条记录并存且 transaction_id 为不同的确定性 UUIDv5"
            "（对齐 models/transaction_id_test.go 的 TestDebitAndCreditIDsDiffer）；"
            "inventory-reserve 失败场景 Release+Credit+订单 Pending 为逆序补偿痕迹；"
            "order-new 失败场景仅订单记录且状态 Pending；补偿失败场景按注入位置核对中止边界。"
        ),
        "idempotency_check": (
            "成功场景同一 order_id 重复触发一次：三表行数不变（UUIDv5 覆盖写）、"
            "复触发仍返回 success、Debit 事务 ID 保持 uuid5(namespace, order_id|Debit) 期望值。"
        ),
        "evidence_files": ["verification-result.json"],
        "passed": True,
    }
    with open(RESULT_FILE, "w", encoding="utf-8") as handle:
        json.dump(result, handle, ensure_ascii=False, indent=2)

    print("[SUCCESS] 7 个场景的响应终态、LTS 编排轨迹、数据库落库与幂等复触发全部验证通过，"
          "结果已写入 verification-result.json。")


if __name__ == "__main__":
    main()
