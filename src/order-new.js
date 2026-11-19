// 订单创建事务：保存订单并把 order_status 置为 "New"（对齐源仓库 order-new/main.go）。
// 失败注入语义：写库成功之后，order_id 首字符为 "1" 时返回 ErrProcessOrder。
const crypto = require("crypto");
const http = require("http");
const https = require("https");

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

// ---- DynamoDB 属性值编解码（对齐 Go dynamodbav omitempty 语义） ----
function orderLineItemAV(it) {
  const av = {};
  if (it.item_id) av.item_id = { S: it.item_id };
  if (typeof it.qty === "number" && it.qty !== 0) av.qty = { N: String(it.qty) };
  if (it.description) av.description = { S: it.description };
  if (typeof it.unit_price === "number" && it.unit_price !== 0) av.unit_price = { N: String(it.unit_price) };
  return av;
}

function paymentItemAV(payment) {
  const av = {};
  if (payment.merchant_id) av.merchant_id = { S: payment.merchant_id };
  if (typeof payment.payment_amount === "number" && payment.payment_amount !== 0) {
    av.payment_amount = { N: String(payment.payment_amount) };
  }
  if (payment.transaction_id) av.transaction_id = { S: payment.transaction_id };
  if (payment.transaction_date) av.transaction_date = { S: payment.transaction_date };
  if (payment.order_id) av.order_id = { S: payment.order_id };
  if (payment.payment_type) av.payment_type = { S: payment.payment_type };
  return av;
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

// 订单表记录（嵌套 payment / inventory 以 M 存储，字段 omitempty）。
function orderItemAV(ord) {
  const item = {};
  if (ord.order_id) item.order_id = { S: ord.order_id };
  if (ord.order_date) item.order_date = { S: ord.order_date };
  if (ord.customer_id) item.customer_id = { S: ord.customer_id };
  if (ord.order_status) item.order_status = { S: ord.order_status };
  const items = Array.isArray(ord.items) ? ord.items : [];
  if (items.length > 0) item.items = { L: items.map((it) => ({ M: orderLineItemAV(it) })) };
  if (ord.payment) item.payment = { M: paymentItemAV(ord.payment) };
  if (ord.inventory) item.inventory = { M: inventoryItemAV(ord.inventory) };
  return item;
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

function logStep(event, fields) {
  console.log("[SAGA-STEP] " + JSON.stringify(Object.assign({ service: "order-new", event: event }, fields)));
}

exports.handler = async (event, context) => {
  const ord = event || {};
  const orderId = ord.order_id || "";
  logStep("STEP_INVOKED", { orderId, message: "received new order" });

  // 保存订单数据，状态置为 New
  ord.order_status = "New";
  try {
    await putRecord(orderItemAV(ord));
  } catch (error) {
    logStep("STEP_FAILED", { orderId, error: error.message });
    return { businessError: { errorType: "ErrProcessOrder", errorMessage: "failed to put record to GeminiDB: " + error.message } };
  }

  // 失败注入：order_id 首字符为 "1"（源语义：写库成功后触发）
  if (orderId.slice(0, 1) === "1") {
    const message = "Unable to process order " + orderId;
    logStep("STEP_FAILED", { orderId, error: message });
    return { businessError: { errorType: "ErrProcessOrder", errorMessage: message } };
  }

  logStep("STEP_SUCCEEDED", { orderId, message: "order status set to new" });
  return orderPayload(ord);
};
