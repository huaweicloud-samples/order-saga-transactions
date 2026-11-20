// Saga 编排函数：按 llt.asl.yaml 的状态机语义实现电商下单长事务。
// 正向链 order-new → payment-debit → inventory-reserve；任一步骤业务失败时，
// 按失败步骤及其之前各步骤补偿动作的逆序执行（inventory-release → payment-credit → order-update）；
// 补偿自身失败时发布失败通知并中止剩余补偿（ASL 的 Catch → Notify → OrderFailed 语义）。
// 编排轨迹以 [SAGA] 结构化日志写入 LTS。
const https = require("https");
const crypto = require("crypto");

const FORWARD_STEPS = [
  { name: "order-new", urn: process.env.ORDER_NEW_FUNCTION_URN, errorType: "ErrProcessOrder" },
  { name: "payment-debit", urn: process.env.PAYMENT_DEBIT_FUNCTION_URN, errorType: "ErrProcessPayment" },
  { name: "inventory-reserve", urn: process.env.INVENTORY_RESERVE_FUNCTION_URN, errorType: "ErrReserveInventory" },
];

// 每个正向步骤的补偿动作；失败于第 i 步时执行 FORWARD[0..i] 的补偿逆序。
const COMPENSATION_OF = {
  "order-new": { name: "order-update", urn: process.env.ORDER_UPDATE_FUNCTION_URN, errorType: "ErrUpdateOrderStatus" },
  "payment-debit": { name: "payment-credit", urn: process.env.PAYMENT_CREDIT_FUNCTION_URN, errorType: "ErrProcessRefund" },
  "inventory-reserve": { name: "inventory-release", urn: process.env.INVENTORY_RELEASE_FUNCTION_URN, errorType: "ErrReleaseInventory" },
};

const SMN_TOPIC_URN = process.env.SMN_TOPIC_URN;
const SMN_REGION = process.env.SMN_REGION;

// 重试策略对齐 ASL：Interval 2s / MaxAttempts 3（不含首次）/ BackoffRate 2 / MaxDelay 8s / FULL jitter。
const RETRY_INTERVAL_SECONDS = 2;
const RETRY_MAX_ATTEMPTS = 3;
const RETRY_BACKOFF_RATE = 2;
const RETRY_MAX_DELAY_SECONDS = 8;
const STEP_INVOKE_TIMEOUT_MS = 10000;

function trace(eventType, fields) {
  console.log("[SAGA] " + JSON.stringify(Object.assign({ eventType: eventType }, fields)));
}

function httpsRequest(options, body) {
  return new Promise((resolve, reject) => {
    const req = https.request(options, (res) => {
      const chunks = [];
      res.on("data", (chunk) => chunks.push(chunk));
      res.on("end", () => resolve({ statusCode: res.statusCode, body: Buffer.concat(chunks).toString("utf8") }));
    });
    req.on("error", reject);
    req.setTimeout(STEP_INVOKE_TIMEOUT_MS, () => {
      req.destroy(new Error("request timeout"));
    });
    if (body != null) {
      req.write(body);
    }
    req.end();
  });
}

// 业务错误：步骤函数显式返回 businessError（对应源函数返回的 Err* 类型错误），不重试、直接走补偿。
function BusinessError(errorType, errorMessage) {
  const error = new Error(errorMessage);
  error.kind = "business";
  error.errorType = errorType;
  return error;
}

// 瞬态错误：传输/平台异常（对应 ASL Retry 的 Lambda 服务端异常），按抖动退避重试。
function TransientError(message) {
  const error = new Error(message);
  error.kind = "transient";
  return error;
}

// 使用委托注入的项目级 token 同步调用步骤函数。
async function invokeStep(urn, payload, token) {
  const parts = urn.split(":");
  if (parts.length < 4) {
    throw new TransientError("invalid step function URN: " + urn);
  }
  const region = parts[2];
  const projectId = parts[3];
  const resp = await httpsRequest({
    method: "POST",
    host: `functiongraph.${region}.myhuaweicloud.com`,
    path: `/v2/${projectId}/fgs/functions/${urn}/invocations`,
    headers: {
      "Content-Type": "application/json",
      "X-Auth-Token": token,
      "X-Project-Id": projectId,
    },
  }, JSON.stringify(payload));
  if (resp.statusCode < 200 || resp.statusCode >= 300) {
    throw new TransientError(`step invoke HTTP ${resp.statusCode}: ${resp.body.slice(0, 300)}`);
  }
  let parsed;
  try {
    parsed = JSON.parse(resp.body);
  } catch (e) {
    throw new TransientError(`step invoke returned non-JSON body: ${resp.body.slice(0, 200)}`);
  }
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new TransientError("step invoke returned an unexpected body");
  }
  if (parsed.businessError && parsed.businessError.errorType) {
    throw new BusinessError(parsed.businessError.errorType, parsed.businessError.errorMessage || "");
  }
  // 步骤函数未捕获崩溃时平台仍可能返回 HTTP 200，错误在 errorMessage 字段（瞬态，可重试）。
  if (parsed.errorMessage) {
    throw new TransientError(String(parsed.errorMessage));
  }
  return parsed;
}

function agencyCredentials(context) {
  const ak = context.getSecurityAccessKey && context.getSecurityAccessKey();
  const sk = context.getSecuritySecretKey && context.getSecuritySecretKey();
  const token = context.getSecurityToken && context.getSecurityToken();
  if (!ak || !sk || !token) {
    throw new Error("agency temporary credentials are unavailable");
  }
  return { ak, sk, token };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// FULL jitter：第 n 次重试（n 从 0 起）前的等待为 random(0, min(2 * 2^n, 8)) 秒。
function fullJitterDelayMs(retryIndex) {
  const cap = Math.min(RETRY_INTERVAL_SECONDS * Math.pow(RETRY_BACKOFF_RATE, retryIndex), RETRY_MAX_DELAY_SECONDS);
  return Math.random() * cap * 1000;
}

// 首次调用 + 至多 RETRY_MAX_ATTEMPTS 次重试；业务错误不重试，瞬态错误抖动退避重试。
async function invokeStepWithRetry(step, payload, token) {
  const totalAttempts = RETRY_MAX_ATTEMPTS + 1;
  let lastError;
  for (let attempt = 1; attempt <= totalAttempts; attempt++) {
    try {
      const result = await invokeStep(step.urn, payload, token);
      return { result, attempt };
    } catch (error) {
      error.attempt = attempt;
      lastError = error;
      if (error.kind === "business") {
        throw error;
      }
      if (attempt >= totalAttempts) {
        break;
      }
      await sleep(fullJitterDelayMs(attempt - 1));
    }
  }
  throw lastError;
}

// 发布 saga 终态通知到 SMN 主题（委托 token，同 saga 先例的 httpsRequest 风格）。
async function publishNotification(token, message) {
  const parts = String(SMN_TOPIC_URN || "").split(":");
  if (parts.length < 5 || parts[1] !== "smn") {
    throw new Error("invalid SMN topic URN: " + SMN_TOPIC_URN);
  }
  const projectId = parts[3];
  const body = JSON.stringify({ message: JSON.stringify(message) });
  const resp = await httpsRequest({
    method: "POST",
    host: `smn.${SMN_REGION}.myhuaweicloud.com`,
    path: `/v2/${projectId}/notifications/topics/${encodeURIComponent(SMN_TOPIC_URN)}/publish`,
    headers: {
      "Content-Type": "application/json",
      "X-Auth-Token": token,
      "Content-Length": Buffer.byteLength(body),
    },
  }, body);
  if (resp.statusCode < 200 || resp.statusCode >= 300) {
    throw new Error(`SMN publish failed: HTTP ${resp.statusCode} ${resp.body.slice(0, 300)}`);
  }
}

exports.handler = async (event, context) => {
  const requestId = context.getRequestID ? context.getRequestID() : "unknown";
  const orderId = event && event.order_id;
  if (!orderId) {
    throw new Error("order_id is required");
  }
  // 前置校验委托临时凭据可用（跨函数调用与 SMN 发布均依赖该委托），不可用时立即失败。
  agencyCredentials(context);
  const token = context.getToken && context.getToken();
  if (!token) {
    throw new Error("agency token is unavailable");
  }

  trace("SAGA_STARTED", { orderId, step: null, attempt: 1, requestId });

  // 对应 ASL 工作流变量 $order：初始为输入载荷，每个步骤成功后更新为该步输出；
  // 补偿动作始终收到"最后一次成功的 order 载荷"，不携带错误数据。
  let order = event;
  const completedSteps = [];
  const compensatedSteps = [];
  let failure = null;

  for (const step of FORWARD_STEPS) {
    trace("STEP_STARTED", { orderId, step: step.name, attempt: 1, requestId });
    try {
      const invocation = await invokeStepWithRetry(step, order, token);
      order = invocation.result;
      completedSteps.push(step.name);
      trace("STEP_SUCCEEDED", { orderId, step: step.name, attempt: invocation.attempt, requestId });
    } catch (error) {
      failure = { step: step.name, error: error };
      break;
    }
  }

  if (!failure) {
    // ASL：ReserveInventory 成功 → sns:NotifySuccess → OrderSucceeded
    const message = { order_id: order.order_id, status: "success" };
    await publishNotification(token, message);
    trace("NOTIFIED", { orderId, step: null, attempt: 1, status: "success", requestId });
    trace("SAGA_SUCCEEDED", { orderId, step: null, attempt: 1, requestId, completedSteps });
    return {
      order_id: orderId,
      status: "success",
      completedSteps: completedSteps,
      compensatedSteps: compensatedSteps,
      order: order,
    };
  }

  const failedStep = failure.step;
  const failedAttempt = failure.error.attempt || 1;
  trace("STEP_FAILED", { orderId, step: failedStep, attempt: failedAttempt, error: failure.error.message, requestId });

  const result = {
    order_id: orderId,
    status: "failed",
    failedStep: failedStep,
    completedSteps: completedSteps,
    compensatedSteps: compensatedSteps,
    order: order,
  };

  // ASL 语义：瞬态错误重试耗尽后以未捕获错误直接失败，不执行补偿、不发通知。
  if (failure.error.kind !== "business") {
    result.error = { errorType: "TransientStepFailure", errorMessage: failure.error.message };
    trace("SAGA_FAILED", { orderId, step: null, attempt: failedAttempt, requestId, error: failure.error.message, compensatedSteps });
    return result;
  }
  result.error = { errorType: failure.error.errorType, errorMessage: failure.error.message };

  // 补偿链：失败步骤自身及其之前各正向步骤的补偿动作，按正向链逆序执行。
  const failedIndex = FORWARD_STEPS.findIndex((s) => s.name === failedStep);
  const compensationChain = FORWARD_STEPS.slice(0, failedIndex + 1)
    .map((s) => Object.assign({ compensates: s.name }, COMPENSATION_OF[s.name]))
    .reverse();

  for (const comp of compensationChain) {
    trace("COMPENSATING", { orderId, step: comp.name, attempt: 1, compensates: comp.compensates, requestId });
    try {
      const invocation = await invokeStepWithRetry(comp, order, token);
      order = invocation.result;
      compensatedSteps.push(comp.name);
      trace("COMPENSATED", { orderId, step: comp.name, attempt: invocation.attempt, requestId });
    } catch (error) {
      // ASL 语义：补偿失败 → 发布失败通知并中止剩余补偿（Catch → Notify → OrderFailed）。
      trace("STEP_ERROR", { orderId, step: comp.name, attempt: error.attempt || 1, error: error.message, requestId });
      result.compensationError = error.kind === "business"
        ? { errorType: error.errorType, errorMessage: error.message }
        : { errorType: "TransientStepFailure", errorMessage: error.message };
      if (error.kind === "business") {
        const message = { order_id: order.order_id, status: "failed", error: result.compensationError };
        await publishNotification(token, message);
        trace("NOTIFIED", { orderId, step: null, attempt: 1, status: "failed", requestId });
        result.notified = true;
      }
      trace("SAGA_FAILED", { orderId, step: null, attempt: error.attempt || 1, requestId, error: result.error.errorMessage, compensatedSteps });
      return result;
    }
  }

  // ASL 语义：正向业务失败但补偿全部成功 → OrderFailed（不发通知）。
  trace("SAGA_FAILED", { orderId, step: null, attempt: 1, requestId, error: result.error.errorMessage, compensatedSteps });
  return result;
};
