#!/usr/bin/env python3
"""本地静态校验：node --check 全部 src/*.js、密钥字面量扫描、UUIDv5 跨语言一致性核对，
留痕 static-check-result.txt。terraform validate 由部署流程手动执行，不在本脚本内。"""
import json
import os
import re
import subprocess
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT / "src"
RESULT_FILE = ROOT / "static-check-result.txt"

TRANSACTION_NAMESPACE = uuid.UUID("8b03bc51-6db8-4d24-9e5b-0f8245f5f42e")
TEST_ORDER_ID = "40063fe3-56d9-4c51-b91f-71929834ce03"
# 对齐 models/transaction_id_test.go 的期望：同名幂等、Debit/Credit 不同、Reserve/Release 不同。
UUID_CHECK_TARGETS = [
    ("payment-debit.js", "Debit"),
    ("payment-credit.js", "Credit"),
    ("inventory-reserve.js", "Reserve"),
    ("inventory-release.js", "Release"),
]

# 供密钥字面量扫描的文件集合：源码与 Terraform 全量扫描，文档只查赋值型密钥字面量。
SCAN_GLOBS_FULL = ["src/*.js", "*.tf"]
SCAN_GLOBS_ASSIGNED_ONLY = ["*.md", "fixtures/*.json"]

# 可疑字面量模式（允许 <占位符>、环境变量引用与文档示例值）。
ASSIGNED_SECRET = re.compile(
    r"(?i)\b(access[_-]?key|secret[_-]?key|password|passwd|token)\b[^=\n]{0,12}=\s*[\"']([^\"'$<>]{8,})[\"']")
RAW_BLOB = re.compile(r"\b(?:[A-Fa-f0-9]{40,}|[A-Za-z0-9+/]{40,}={0,2})\b")
HEX_KEY_LIKE = re.compile(r"\b[A-Fa-f0-9]{32,}\b")

ALLOWED_LITERAL_HINTS = ("<", "$", "var.", "process.env", "os.environ", "TF_VAR", "GEMINIDB_", "HW_")


def node_available():
    return subprocess.run(["node", "--version"], capture_output=True).returncode == 0


def check_syntax():
    results = []
    for source in sorted(SRC_DIR.glob("*.js")):
        completed = subprocess.run(["node", "--check", str(source)], capture_output=True)
        results.append((source.name, completed.returncode == 0,
                        completed.stderr.decode("utf-8", "replace").strip()))
    return results


def check_uuid_v5():
    """Node 实现与 Python 标准库 uuid5 逐个核对（同名幂等 + 四类事务 ID 互不相同）。"""
    results = []
    computed = {}
    for filename, transaction_type in UUID_CHECK_TARGETS:
        source = str(SRC_DIR / filename).replace("\\", "/")
        script = "const m = require(%s); process.stdout.write(m.transactionId(%s, %s));" % (
            json.dumps(source), json.dumps(TEST_ORDER_ID), json.dumps(transaction_type))
        completed = subprocess.run(["node", "-e", script], capture_output=True)
        actual = completed.stdout.decode("utf-8", "replace").strip()
        expected = str(uuid.uuid5(TRANSACTION_NAMESPACE, TEST_ORDER_ID + "|" + transaction_type))
        ok = completed.returncode == 0 and actual == expected
        results.append((filename, transaction_type, expected, actual, ok))
        computed[transaction_type] = actual
    idempotent = (
        len(set(computed.values())) == len(computed)
        and computed.get("Debit") != computed.get("Credit")
        and computed.get("Reserve") != computed.get("Release")
    )
    return results, idempotent


def scan_file(path, patterns):
    findings = []
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return findings
    for match in ASSIGNED_SECRET.finditer(text):
        literal = match.group(2)
        if literal.startswith(ALLOWED_LITERAL_HINTS):
            continue
        findings.append({"file": str(path.relative_to(ROOT)), "line": text.count("\n", 0, match.start()) + 1,
                         "kind": "assigned-secret-literal", "value": literal[:12] + "..."})
    if patterns:
        for regex, kind in patterns:
            for match in regex.finditer(text):
                findings.append({"file": str(path.relative_to(ROOT)), "line": text.count("\n", 0, match.start()) + 1,
                                 "kind": kind, "value": match.group(0)[:12] + "..."})
    return findings


def scan_secrets():
    findings = []
    blob_patterns = [(RAW_BLOB, "raw-blob"), (HEX_KEY_LIKE, "hex-key-like")]
    for pattern in SCAN_GLOBS_FULL:
        for path in sorted(ROOT.glob(pattern)):
            findings.extend(scan_file(path, blob_patterns))
    for pattern in SCAN_GLOBS_ASSIGNED_ONLY:
        for path in sorted(ROOT.glob(pattern)):
            findings.extend(scan_file(path, None))
    return findings


def main():
    lines = []
    passed = True

    if not node_available():
        lines.append("node --check: SKIP (node 不可用)")
        passed = False
    else:
        syntax_results = check_syntax()
        syntax_ok = all(ok for _, ok, _ in syntax_results)
        lines.append("node --check src/*.js: %s (%d 个文件)" % ("PASS" if syntax_ok else "FAIL", len(syntax_results)))
        for name, ok, error in syntax_results:
            lines.append("  %s %s%s" % (name, "OK" if ok else "FAIL", (" | " + error[:200]) if error and not ok else ""))
        passed = passed and syntax_ok

    if node_available():
        uuid_results, idempotent = check_uuid_v5()
        uuid_ok = all(item[4] for item in uuid_results) and idempotent
        lines.append("UUIDv5 cross-check (node vs python uuid5): %s" % ("PASS" if uuid_ok else "FAIL"))
        for filename, transaction_type, expected, actual, ok in uuid_results:
            lines.append("  %s %s: expected=%s actual=%s %s" % (
                filename, transaction_type, expected, actual, "OK" if ok else "MISMATCH"))
        lines.append("  idempotency + Debit!=Credit + Reserve!=Release: %s" % ("OK" if idempotent else "FAIL"))
        passed = passed and uuid_ok

    findings = scan_secrets()
    secrets_ok = not findings
    lines.append("secret literal scan (src/tf full + md/fixtures assigned-only): %s (发现 %d 处)" % (
        "PASS" if secrets_ok else "FAIL", len(findings)))
    for finding in findings:
        lines.append("  %s:%s %s %s" % (finding["file"], finding["line"], finding["kind"], finding["value"]))
    passed = passed and secrets_ok

    lines.append("terraform validate: 由部署流程手动执行（见 README.md 部署步骤）")
    lines.append("overall: %s" % ("PASS" if passed else "FAIL"))
    RESULT_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))
    if not passed:
        sys.exit(1)


if __name__ == "__main__":
    main()
