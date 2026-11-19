# 订单长事务 Saga：基于 FunctionGraph 的分布式事务示例

**简体中文** | [English](README.md)

在华为云 `cn-north-4` 上实现电商下单长事务（Saga 模式）：一个编排函数驱动正向事务链
`order-new → payment-debit → inventory-reserve`；任一步骤业务失败时，按逆序补偿已完成的事务
（`inventory-release → payment-credit → order-update`），终态发布到 SMN 主题，完整编排轨迹写入 LTS。
业务数据存放在外部预置的 GeminiDB 兼容 DynamoDB 实例的三张表中，交易 ID 为确定性 UUIDv5，
步骤重试时覆盖写而非产生重复记录。

## 架构

```text
验证脚本 / 客户端
  │ 同步调用（事件体 = 订单载荷，场景由 order_id 前缀注入）
  ▼
FunctionGraph 编排函数（Node.js18.15，128 MB / 180 s，委托 op_svc_cff）
  │ 函数间同步调用：委托 token + /v2/{project_id}/fgs/functions/{urn}/invocations
  │ 瞬态错误：FULL jitter 重试（间隔 2 s、最多 3 次、退避倍率 2、上限 8 s）
  │ 业务错误：不重试，直接进入补偿
  │--------------- 正向链 -------------------|--------------- 补偿链（逆序） -----------------|
  │-- order-new          （orders 表）       │-- inventory-release   （inventory 表）          |
  │-- payment-debit      （payments 表）     │-- payment-credit     （payments 表）            |
  │-- inventory-reserve  （inventory 表）    │-- order-update       （orders 表）              │
  │
  ├─ 编排轨迹：[SAGA] 结构化日志 → LTS 日志组 + 每函数独立日志流（共 7 条）
  └─ 终态通知：{order_id, status: "success" | "failed", error} → SMN 主题（REST 发布，委托 token）
  ▼
GeminiDB（兼容 DynamoDB 接口，外部预置）
  ├─ {app_name}-orders     主键 order_id
  ├─ {app_name}-payments   主键 transaction_id
  └─ {app_name}-inventory  主键 transaction_id
```

- 编排函数在内存中保存"最后一次成功的 order 载荷"，补偿动作始终收到干净的业务载荷，
  对齐源状态机工作流变量语义（补偿不携带错误数据）。
- 补偿自身失败时发布失败通知并中止剩余补偿；正向业务失败但补偿全部成功时直接终态失败、
  不发通知——两条语义均逐状态对齐源状态机。
- 支付/库存交易 ID 由 `order_id + "|" + 事务类型` 派生确定性 UUIDv5，重试覆盖写同一条记录。

## 目录结构

```text
.
├── main.tf / variables.tf / outputs.tf / versions.tf
├── src/
│   ├── orchestrator.js        # Saga 编排函数（正向链、重试、补偿、通知）
│   ├── order-new.js           # 保存订单，order_status 置 "New"
│   ├── order-update.js        # 补偿：order_status 置 "Pending"
│   ├── payment-debit.js       # 写入 "Debit" 支付交易
│   ├── payment-credit.js      # 补偿：写入金额取负的 "Credit" 退款
│   ├── inventory-reserve.js   # 写入 "Reserve" 库存交易
│   └── inventory-release.js   # 补偿：写入 "Release" 库存交易
├── fixtures/scenario-1..7.json   # 失败注入载荷（按 order_id 前缀选择场景）
├── scripts/verify_e2e.py          # 部署后端到端验证（响应 + LTS 轨迹 + 数据库 + 幂等）
└── scripts/static_check.py        # node --check、密钥字面量扫描、UUIDv5 跨语言校验
```

## 前置条件

1. 具备 FunctionGraph、SMN、LTS 与 IAM 委托管理权限的华为云账号；访问密钥只经环境变量提供。
2. Terraform >= 1.5.0，`huaweicloud` provider >= 1.95.0。
3. **预先创建接口类型为兼容 DynamoDB 的 GeminiDB 实例**（控制台路径：数据库 > GeminiDB，购买时选择
   DynamoDB 兼容接口）。规格建议：集群形态、**3 节点起步**（数据面高可用）；实例按需计费
   （以控制台为准），验证后请在控制台及时销毁。本目录不创建该实例、不声明其资源。
4. GeminiDB 兼容 DynamoDB 接口的数据面只认实例自带数据库账号：用户名固定 **rwuser**、密码为
   **实例管理员密码**（建实例时设置）；**不接受** IAM AK/SK，也不接受委托临时凭据。集群形态数据面端口为
   **80**（单节点形态为 8000），`geminidb_port` 默认取 80。
5. `geminidb_access_key` 与 `geminidb_secret_key` 永不写入变量文件、源码、fixtures 或日志，只经
   Terraform sensitive 变量环境变量注入。连接地址为实例负载均衡内网 IP；FunctionGraph 运行子网可达
   时无需为函数挂接网络配置。

## 资源清单

| 资源 | 数量 | 命名 | 计费 |
|---|---|---|---|
| FunctionGraph 函数 | 7 | `${app_name}-orchestrator` 与 `${app_name}-{步骤}` | 按量；样例调用量费用可忽略 |
| IAM 委托（委托方 op_svc_cff） | 1 | `${app_name}-agency-<随机后缀>` | 免费 |
| SMN 主题 | 1 | `${app_name}-saga-topic` | 按发布消息条数；每次 saga 至多 1 条 |
| LTS 日志组 | 1 | `${app_name}-logs`（保留 7 天） | 按日志存储量；量极小 |
| LTS 日志流 | 7 | 每函数一条 | 随 LTS 存储 |
| GeminiDB 业务表 | 3 | `${app_name}-orders/payments/inventory` | 随实例 |

不创建 APIG、OBS、VPC、网关、消息队列、容器或虚拟机；GeminiDB 实例为外部预置。

## Terraform 配置与部署

敏感值只经 `TF_VAR_...` 环境变量注入，不要写入 `.tfvars` 文件。

```bash
cd Repositories/order-saga-transactions
unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy ALL_PROXY all_proxy

terraform init -plugin-dir=../../.terraform-plugin-cache
terraform validate

export TF_VAR_geminidb_endpoint="<geminidb负载均衡内网IP>"
export TF_VAR_geminidb_port="80"
export TF_VAR_geminidb_access_key="rwuser"
export TF_VAR_geminidb_secret_key="$(secret-provider read geminidb-admin-password)"
terraform plan
# 确认费用与账号权限后：
terraform apply
```

`secret-provider` 占位符代表租户现成的安全注入工具，请勿把真实凭据替换进本目录任何文件。
`geminidb_*` 为 sensitive 变量，Terraform state 含敏感配置，须使用受保护的后端与访问控制；
`outputs.tf` 不输出任何凭据。Terraform 把数据面参数注入 FunctionGraph `user_data`：每个步骤函数
拿到自己的 `TABLE_NAME`，编排函数拿到六个步骤函数 URN、SMN 主题 URN 与区域。

## 验证

`scripts/verify_e2e.py` 依次调用 7 个场景，断言 saga 终态、LTS `(eventType, step)` 轨迹、三表落库
结果与 UUIDv5 幂等（同一订单重复触发行数不变），结果写入 `verification-result.json`。

```bash
# 华为云 API 签名密钥：读 ../../credentials.csv 第 1 行（row[1]=AK、row[2]=SK）
# GeminiDB 数据面凭据（数据库直查断言用）：
export GEMINIDB_ENDPOINT="<geminidb负载均衡内网IP>"
export GEMINIDB_PORT="80"
export GEMINIDB_ACCESS_KEY="rwuser"
export GEMINIDB_SECRET_KEY="$(secret-provider read geminidb-admin-password)"
export GEMINIDB_REGION="cn-north-4"

E:/code/huawei-samples/.tmp-gemini-key-venv/Scripts/python.exe scripts/verify_e2e.py
```

脚本先幂等确保三张业务表存在（CreateTable，主键对齐源模板），再逐场景执行。各场景期望结果
（源 testdata 语义，`order_id` 前缀即注入开关）：

| 场景 | order_id 前缀 | 注入错误 | 期望路径 |
|---|---|---|---|
| 1 | `1` | ErrProcessOrder | order-new 失败 → order-update 补偿 → saga 失败 |
| 2 | `11` | ErrProcessOrder + ErrUpdateOrderStatus | order-new 失败 → order-update 也失败 → 通知 → 中止 |
| 3 | `2` | ErrProcessPayment | payment-debit 失败 → payment-credit + order-update 补偿 |
| 4 | `22` | ErrProcessPayment + ErrProcessRefund | payment-debit 失败 → payment-credit 也失败 → 通知 → 中止 |
| 5 | `3` | ErrReserveInventory | inventory-reserve 失败 → release + credit + order-update 逆序补偿 |
| 6 | `33` | ErrReserveInventory + ErrReleaseInventory | inventory-reserve 失败 → inventory-release 也失败 → 通知 → 中止 |
| 7 | `4`–`9` | 无 | 正向链全部成功 → 成功通知 → saga 成功 |

数据库断言：场景 7 三表记录齐（订单状态 `New`、一条 `Debit`、一条 `Reserve`）；场景 3 的 `Debit`
与 `Credit` 两条记录并存且 transaction_id 为**不同的**确定性 UUIDv5（对齐源单测
`TestDebitAndCreditIDsDiffer`）；场景 5 留下逆序补偿痕迹（`Release` + `Credit` + 订单 `Pending`）；
场景 1/2 仅订单记录且状态 `Pending`；场景 4/6 因补偿中止，订单保持 `New`，只落库中止前已执行的步骤。
所有支付/库存交易 ID 都必须等于 `uuid5(8b03bc51-…, order_id + "|" + 类型)` 的确定性期望值。

本地静态校验（无云访问）：

```bash
python scripts/static_check.py   # node --check 全部 src/*.js、密钥字面量扫描、UUIDv5 对照 RFC 4122
```

## 清理

```bash
unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy ALL_PROXY all_proxy
terraform destroy
```

`terraform destroy` 删除本目录的函数、委托、SMN 主题与 LTS 组及日志流。GeminiDB 实例为外部资源，
本目录不管理其生命周期；不再需要时请在控制台销毁。实例内表数据可删除 `${app_name}-*` 三张表或重置实例。

## 运行机制与设计说明

- **编排模式**：本实践采用函数内同步编排架构，主编排函数在一次同步调用周期内完成订单、支付与库存服务的协同（默认超时预算 180 秒）。
- **重试策略**：针对瞬态网络或下游服务抖动采用指数退避与 Full Jitter 策略；业务逻辑错误不触发重试，直接进入分布式回滚流程。
- **补偿机制**：编排器维护局部状态上下文，当某一步骤失败时，按照反向拓扑依次触发前序已成功服务的补偿操作（如退款、释放库存锁定）。
- **消息通知**：通过消息通知服务 SMN 发布关键事务失败事件，通知仅在业务补偿执行失败等需要人工介入的异常场景下触发。
- **持久化接口**：数据存储采用 GeminiDB 兼容接口，服务内集成 SigV4 认证客户端直连数据库实例。
- **可观测性**：各微服务函数分别绑定专属 LTS 云日志流，实现分布式调用的全链路日志检索与排障。
