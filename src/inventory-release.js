// 库存释放补偿事务：找到订单的 Reserve 交易并写入 Release 交易（对齐源仓库 inventory-release/main.go）。
// 事务 ID 为确定性 UUIDv5(order_id|Release)，与 Reserve 不同（重试幂等、不覆盖原预留记录）。
// 失败注入语义：写库成功之后，order_id 前两位为 "33" 时返回 ErrReleaseInventory。
const crypto = require("crypto");
const http = require("http");
const https = require("https");

// ---- UUIDv5（RFC 4122）：对齐 models/inventory.go 的 TransactionID 派生 ----
// 命名空间与派生输入照抄源仓库：uuid5(8b03bc51-6db8-4d24-9e5b-0f8245f5f42e, order_id + "|" + 事务类型)
const TRANSACTION_NAMESPACE = "8b03bc51-6db8-4d24-9e5b-0f8245f5f42e";

function uuidV5(name, namespace) {
  const nsBytes = Buffer.from(String(namespace).replace(/-/g, ""), "hex");
  const digest = crypto.createHash("sha1").update(nsBytes).update(Buffer.from(String(name), "utf8")).digest();
  const bytes = digest.slice(0, 16);
  bytes[6] = (bytes[6] & 0x0f) | 0x50;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = bytes.toString("hex");
  return [hex.slice(0, 8), hex.slice(8, 12), hex.slice(12, 16), hex.slice(16, 20), hex.slice(20, 32)].join("-");
}

function transactionId(orderId, transactionType) {
  return uuidV5(orderId + "|" + transactionType, TRANSACTION_NAMESPACE);
}

// ---- GeminiDB（DynamoDB 兼容接口）客户端：SigV4 静态凭据，零外部依赖 ----
// 数据面只认实例账号：用户名固定 rwuser（GEMINIDB_ACCESS_KEY）、密码为实例管理员密码
// （GEMINIDB_SECRET_KEY）；集群形态数据面端口为 80，不接受 IAM/委托临时凭据。
function geminidbConfig() {
  const endpoint = process.env.GEMINIDB_ENDPOINT || "";
  if (!endpoint) {
    throw new Error("GEMINIDB_ENDPOINT is not configured");
  }
  const withScheme = endpoint.indexOf("://") >= 0 ? endpoint : "http://" + endpoint;
  const schemeIndex = withScheme.indexOf("://");
  const protocol = withScheme.slice(0, schemeIndex).toLowerCase();
  if (protocol !== "http" && protocol !== "https") {
    throw new Error("GeminiDB protocol must be http or https");
  }
  let rest = withScheme.slice(schemeIndex + 3);
  const slashIndex = rest.indexOf("/");
  const hostPart = slashIndex >= 0 ? rest.slice(0, slashIndex) : rest;
  const colonIndex = hostPart.indexOf(":");
  const host = colonIndex >= 0 ? hostPart.slice(0, colonIndex) : hostPart;
  if (!host) {
    throw new Error("GeminiDB endpoint must contain a host");
  }
  const explicitPort = colonIndex >= 0 ? hostPart.slice(colonIndex + 1) : "";
  const port = explicitPort ? parseInt(explicitPort, 10) : parseInt(process.env.GEMINIDB_PORT || "80", 10);
  const hostForHeader = host.indexOf(":") >= 0 ? "[" + host + "]" : host;
  return {
    protocol: protocol,
    host: host,
    port: port,
    hostHeader: hostForHeader + ":" + port,
    accessKey: process.env.GEMINIDB_ACCESS_KEY || "",
    secretKey: process.env.GEMINIDB_SECRET_KEY || "",
    region: process.env.GEMINIDB_REGION || "cn-north-4",
    tableName: process.env.TABLE_NAME || "",
  };
}

function sha256Hex(value) {
  return crypto.createHash("sha256").update(value, "utf8").digest("hex");
}

function hmacSha256(key, value) {
  return crypto.createHmac("sha256", key).update(value, "utf8").digest();
}

// 签名派生密钥：AWS4+SK → date → region → dynamodb → aws4_request
function signingKey(secretKey, date, region) {
  let key = hmacSha256(Buffer.from("AWS4" + secretKey, "utf8"), date);
  key = hmacSha256(key, region);
  key = hmacSha256(key, "dynamodb");
  return hmacSha256(key, "aws4_request");
}

function amzTimestamp(date) {
  return date.toISOString().replace(/[-:]/g, "").replace(/\.\d{3}/, "");
}

function httpRequest(protocol, options, body) {
  const transport = protocol === "https" ? https : http;
  return new Promise((resolve, reject) => {
    const req = transport.request(options, (res) => {
      const chunks = [];
      res.on("data", (chunk) => chunks.push(chunk));
      res.on("end", () => resolve({ statusCode: res.statusCode, body: Buffer.concat(chunks).toString("utf8") }));
    });
    req.on("error", reject);
    req.setTimeout(10000, () => {
      req.destroy(new Error("GeminiDB request timeout"));
    });
    if (body != null) {
      req.write(body);
    }
    req.end();
  });
}

// 对 DynamoDB JSON API 发起 SigV4 签名调用（X-Amz-Target 指定操作）。
async function dynamodbCall(operation, payload) {
  const cfg = geminidbConfig();
  if (!cfg.accessKey || !cfg.secretKey) {
    throw new Error("GeminiDB access key and secret key are required");
  }
  if (!cfg.tableName) {
    throw new Error("TABLE_NAME is not configured");
  }
  const body = JSON.stringify(payload);
  const payloadHash = sha256Hex(body);
  const now = new Date();
  const amzDate = amzTimestamp(now);
  const date = amzDate.slice(0, 8);
  const headers = {
    "content-type": "application/x-amz-json-1.0",
    "host": cfg.hostHeader,
    "x-amz-content-sha256": payloadHash,
    "x-amz-date": amzDate,
    "x-amz-target": "DynamoDB_20120810." + operation,
  };
  const headerNames = Object.keys(headers).sort();
  const canonicalHeaders = headerNames.map((name) => name + ":" + headers[name] + "\n").join("");
  const signedHeaders = headerNames.join(";");
  const canonicalRequest = ["POST", "/", "", canonicalHeaders, signedHeaders, payloadHash].join("\n");
  const scope = date + "/" + cfg.region + "/dynamodb/aws4_request";
  const stringToSign = ["AWS4-HMAC-SHA256", amzDate, scope, sha256Hex(canonicalRequest)].join("\n");
  const signature = crypto.createHmac("sha256", signingKey(cfg.secretKey, date, cfg.region))
    .update(stringToSign, "utf8").digest("hex");
  const authorization = "AWS4-HMAC-SHA256 Credential=" + cfg.accessKey + "/" + scope +
    ", SignedHeaders=" + signedHeaders + ", Signature=" + signature;
  const resp = await httpRequest(cfg.protocol, {
    method: "POST",
    host: cfg.host,
    port: cfg.port,
    path: "/",
    headers: {
      "Content-Type": headers["content-type"],
      "Host": cfg.hostHeader,
      "X-Amz-Content-Sha256": payloadHash,
      "X-Amz-Date": amzDate,
      "X-Amz-Target": headers["x-amz-target"],
      "Authorization": authorization,
      "Content-Length": Buffer.byteLength(body),
    },
  }, body);
  if (resp.statusCode < 200 || resp.statusCode >= 300) {
    let code = "HTTPError";
    let message = "";
    try {
      const parsedError = JSON.parse(resp.body);
      code = String(parsedError.__type || parsedError.code || code).split("#").pop();
      message = String(parsedError.message || "");
    } catch (error) {
      // 保留默认错误信息
    }
    throw new Error("GeminiDB " + operation + " failed with HTTP " + resp.statusCode + ": " + code +
      (message ? " (" + message + ")" : ""));
  }
  if (!resp.body) {
    return {};
  }
  let parsedResult;
  try {
    parsedResult = JSON.parse(resp.body);
  } catch (error) {
    throw new Error("GeminiDB " + operation + " returned invalid JSON: " + resp.body.slice(0, 200));
  }
  if (parsedResult === null || typeof parsedResult !== "object") {
    throw new Error("GeminiDB " + operation + " returned a non-object response");
  }
  return parsedResult;
}

async function putRecord(item) {
  const cfg = geminidbConfig();
  await dynamodbCall("PutItem", { TableName: cfg.tableName, Item: item });
}

// 源实现经 GSI orderIDIndex（order_id HASH + transaction_type RANGE）查询 Reserve 交易；
// GeminiDB 兼容接口按需建表、无二级索引，这里 Scan 全表取回后在客户端按属性过滤。
async function scanRecords() {
  const cfg = geminidbConfig();
  const items = [];
  let exclusiveStartKey;
  do {
    const payload = { TableName: cfg.tableName };
    if (exclusiveStartKey) {
      payload.ExclusiveStartKey = exclusiveStartKey;
    }
    const result = await dynamodbCall("Scan", payload);
    const batch = result.Items || [];
    for (let i = 0; i < batch.length; i++) {
      items.push(batch[i]);
    }
    exclusiveStartKey = result.LastEvaluatedKey;
  } while (exclusiveStartKey);
  return items;
}

// ---- DynamoDB 属性值编解码（对齐 Go dynamodbav omitempty 语义） ----
function avPlain(value) {
  if (!value || typeof value !== "object") {
    return value;
  }
  if ("S" in value) return value.S;
  if ("N" in value) return parseFloat(value.N);
  if ("BOOL" in value) return value.BOOL;
  if ("NULL" in value) return null;
  if ("L" in value) return value.L.map(avPlain);
  if ("M" in value) {
    const out = {};
    const keys = Object.keys(value.M);
    for (let i = 0; i < keys.length; i++) {
      out[keys[i]] = avPlain(value.M[keys[i]]);
    }
    return out;
  }
  return value;
}

function plainItem(item) {
  const out = {};
  const keys = Object.keys(item || {});
  for (let i = 0; i < keys.length; i++) {
    out[keys[i]] = avPlain(item[keys[i]]);
  }
  return out;
}

function inventoryItemAV(inv) {
  const av = {};
  if (inv.transaction_id) av.transaction_id = { S: inv.transaction_id };
  if (inv.transaction_date) av.transaction_date = { S: inv.transaction_date };
  if (inv.order_id) av.order_id = { S: inv.order_id };
  const items = Array.isArray(inv.items) ? inv.items : [];
  if (items.length > 0) av.items = { L: items.map((id) => ({ S: String(id) })) };
  if (inv.transaction_type) av.transaction_type = { S: inv.transaction_type };
  return av;
}

// ---- 函数返回载荷（对齐 Go JSON omitempty 语义） ----
function lineItemPayload(it) {
  const out = {};
  if (it.item_id) out.item_id = it.item_id;
  if (typeof it.qty === "number" && it.qty !== 0) out.qty = it.qty;
  if (it.description) out.description = it.description;
  if (typeof it.unit_price === "number" && it.unit_price !== 0) out.unit_price = it.unit_price;
  return out;
}

function paymentPayload(p) {
  const out = {};
  if (p.merchant_id) out.merchant_id = p.merchant_id;
  if (typeof p.payment_amount === "number" && p.payment_amount !== 0) out.payment_amount = p.payment_amount;
  if (p.transaction_id) out.transaction_id = p.transaction_id;
  if (p.transaction_date) out.transaction_date = p.transaction_date;
  if (p.order_id) out.order_id = p.order_id;
  if (p.payment_type) out.payment_type = p.payment_type;
  return out;
}

function inventoryPayload(inv) {
  const out = {};
  if (inv.transaction_id) out.transaction_id = inv.transaction_id;
  if (inv.transaction_date) out.transaction_date = inv.transaction_date;
  if (inv.order_id) out.order_id = inv.order_id;
  const items = Array.isArray(inv.items) ? inv.items : [];
  if (items.length > 0) out.items = items.map(String);
  if (inv.transaction_type) out.transaction_type = inv.transaction_type;
  return out;
}

function orderPayload(ord) {
  const out = {};
  if (ord.order_id) out.order_id = ord.order_id;
  if (ord.order_date) out.order_date = ord.order_date;
  if (ord.customer_id) out.customer_id = ord.customer_id;
  if (ord.order_status) out.order_status = ord.order_status;
  const items = Array.isArray(ord.items) ? ord.items : [];
  if (items.length > 0) out.items = items.map(lineItemPayload);
  if (ord.payment) out.payment = paymentPayload(ord.payment);
  if (ord.inventory) out.inventory = inventoryPayload(ord.inventory);
  return out;
}

// ---- 业务语义 ----
function rfc3339Now() {
  return new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
}

function logStep(event, fields) {
  console.log("[SAGA-STEP] " + JSON.stringify(Object.assign({ service: "inventory-release", event: event }, fields)));
}

exports.handler = async (event, context) => {
  const ord = event || {};
  const orderId = ord.order_id || "";
  logStep("STEP_INVOKED", { orderId, message: "processing inventory release" });

  // 找到该订单的 Reserve 交易记录（源语义：取查询结果的第一条）
  let inventory = null;
  let scanError = null;
  try {
    const items = await scanRecords();
    for (let i = 0; i < items.length; i++) {
      const candidate = plainItem(items[i]);
      if (candidate.order_id === orderId && candidate.transaction_type === "Reserve") {
        inventory = candidate;
        break;
      }
    }
  } catch (error) {
    scanError = error;
  }
  if (scanError) {
    logStep("STEP_FAILED", { orderId, error: scanError.message });
    return { businessError: { errorType: "ErrReleaseInventory", errorMessage: scanError.message } };
  }
  if (!inventory) {
    const message = "no reserve transaction found for order " + orderId;
    logStep("STEP_FAILED", { orderId, error: message });
    return { businessError: { errorType: "ErrReleaseInventory", errorMessage: message } };
  }

  // Release()：Release 事务 ID 与 Reserve 不同，商品清单保持不变，类型置为 Release
  inventory.transaction_id = transactionId(orderId, "Release");
  inventory.transaction_date = rfc3339Now();
  inventory.transaction_type = "Release";

  try {
    await putRecord(inventoryItemAV(inventory));
  } catch (error) {
    logStep("STEP_FAILED", { orderId, error: error.message });
    return { businessError: { errorType: "ErrReleaseInventory", errorMessage: "failed to put record to GeminiDB: " + error.message } };
  }

  ord.inventory = inventory;

  // 失败注入：order_id 前两位为 "33"（源语义：写库成功后触发）
  if (orderId.slice(0, 2) === "33") {
    const message = "Unable to release inventory for order " + orderId;
    logStep("STEP_FAILED", { orderId, error: message });
    return { businessError: { errorType: "ErrReleaseInventory", errorMessage: message } };
  }

  logStep("STEP_SUCCEEDED", { orderId, message: "inventory release processed", transactionId: inventory.transaction_id });
  return orderPayload(ord);
};

// 供本地静态校验交叉验证 UUIDv5（对齐 models/transaction_id_test.go 的期望）
exports.transactionId = transactionId;
