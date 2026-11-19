# 电商下单长事务（Saga 模式）：编排函数 + 3 组正向/补偿步骤函数 + SMN 通知主题。
# 数据面为外部预置的 GeminiDB 兼容 DynamoDB 实例（rwuser + 实例管理员密码，SigV4 静态凭据），
# 本目录不创建数据库实例、VPC、APIG 与 OBS。

resource "random_string" "suffix" {
  length  = 6
  lower   = true
  upper   = false
  special = false
}

locals {
  orders_table_name    = "${var.app_name}-orders"
  payments_table_name  = "${var.app_name}-payments"
  inventory_table_name = "${var.app_name}-inventory"

  # 步骤函数访问 GeminiDB 数据面的静态凭据与环境参数（敏感值只来自 TF_VAR 环境变量）。
  geminidb_env = {
    GEMINIDB_ENDPOINT   = var.geminidb_endpoint
    GEMINIDB_PORT       = tostring(var.geminidb_port)
    GEMINIDB_ACCESS_KEY = var.geminidb_access_key
    GEMINIDB_SECRET_KEY = var.geminidb_secret_key
    GEMINIDB_REGION     = var.region
  }
}

resource "huaweicloud_identity_agency" "orchestrator" {
  name                  = "${var.app_name}-agency-${random_string.suffix.result}"
  description           = "Allow the saga orchestrator function to invoke step functions and publish SMN notifications"
  delegated_domain_name = "op_svc_cff"
  duration              = "FOREVER"

  project_role {
    project = var.region
    roles   = ["FunctionGraph FullAccess", "SMN FullAccess"]
  }

  # 服务端会将 delegated_domain_name 读回到 delegated_service_name，
  # 两者表示同一委托主体；忽略该只读归一化，避免刷新后永久差异。
  lifecycle {
    ignore_changes = [delegated_service_name]
  }
}

resource "huaweicloud_smn_topic" "saga" {
  name         = "${var.app_name}-saga-topic"
  display_name = "Order saga outcome notifications"
}

resource "huaweicloud_lts_group" "this" {
  group_name  = "${var.app_name}-logs"
  ttl_in_days = 7
}

resource "huaweicloud_lts_stream" "orchestrator" {
  group_id    = huaweicloud_lts_group.this.id
  stream_name = "${var.app_name}-orchestrator"
}

resource "huaweicloud_lts_stream" "order_new" {
  group_id    = huaweicloud_lts_group.this.id
  stream_name = "${var.app_name}-order-new"
}

resource "huaweicloud_lts_stream" "order_update" {
  group_id    = huaweicloud_lts_group.this.id
  stream_name = "${var.app_name}-order-update"
}

resource "huaweicloud_lts_stream" "payment_debit" {
  group_id    = huaweicloud_lts_group.this.id
  stream_name = "${var.app_name}-payment-debit"
}

resource "huaweicloud_lts_stream" "payment_credit" {
  group_id    = huaweicloud_lts_group.this.id
  stream_name = "${var.app_name}-payment-credit"
}

resource "huaweicloud_lts_stream" "inventory_reserve" {
  group_id    = huaweicloud_lts_group.this.id
  stream_name = "${var.app_name}-inventory-reserve"
}

resource "huaweicloud_lts_stream" "inventory_release" {
  group_id    = huaweicloud_lts_group.this.id
  stream_name = "${var.app_name}-inventory-release"
}

resource "huaweicloud_fgs_function" "order_new" {
  name        = "${var.app_name}-order-new"
  app         = "default"
  handler     = "order-new.handler"
  memory_size = 128
  timeout     = 15
  runtime     = "Node.js18.15"
  code_type   = "inline"
  func_code   = base64encode(file("${path.module}/src/order-new.js"))

  user_data = jsonencode(merge(local.geminidb_env, {
    TABLE_NAME = local.orders_table_name
  }))

  enable_lts_log  = true
  log_group_id    = huaweicloud_lts_group.this.id
  log_group_name  = huaweicloud_lts_group.this.group_name
  log_stream_id   = huaweicloud_lts_stream.order_new.id
  log_stream_name = huaweicloud_lts_stream.order_new.stream_name
}

resource "huaweicloud_fgs_function" "order_update" {
  name        = "${var.app_name}-order-update"
  app         = "default"
  handler     = "order-update.handler"
  memory_size = 128
  timeout     = 15
  runtime     = "Node.js18.15"
  code_type   = "inline"
  func_code   = base64encode(file("${path.module}/src/order-update.js"))

  user_data = jsonencode(merge(local.geminidb_env, {
    TABLE_NAME = local.orders_table_name
  }))

  enable_lts_log  = true
  log_group_id    = huaweicloud_lts_group.this.id
  log_group_name  = huaweicloud_lts_group.this.group_name
  log_stream_id   = huaweicloud_lts_stream.order_update.id
  log_stream_name = huaweicloud_lts_stream.order_update.stream_name
}

resource "huaweicloud_fgs_function" "payment_debit" {
  name        = "${var.app_name}-payment-debit"
  app         = "default"
  handler     = "payment-debit.handler"
  memory_size = 128
  timeout     = 15
  runtime     = "Node.js18.15"
  code_type   = "inline"
  func_code   = base64encode(file("${path.module}/src/payment-debit.js"))

  user_data = jsonencode(merge(local.geminidb_env, {
    TABLE_NAME = local.payments_table_name
  }))

  enable_lts_log  = true
  log_group_id    = huaweicloud_lts_group.this.id
  log_group_name  = huaweicloud_lts_group.this.group_name
  log_stream_id   = huaweicloud_lts_stream.payment_debit.id
  log_stream_name = huaweicloud_lts_stream.payment_debit.stream_name
}

resource "huaweicloud_fgs_function" "payment_credit" {
  name        = "${var.app_name}-payment-credit"
  app         = "default"
  handler     = "payment-credit.handler"
  memory_size = 128
  timeout     = 15
  runtime     = "Node.js18.15"
  code_type   = "inline"
  func_code   = base64encode(file("${path.module}/src/payment-credit.js"))

  user_data = jsonencode(merge(local.geminidb_env, {
    TABLE_NAME = local.payments_table_name
  }))

  enable_lts_log  = true
  log_group_id    = huaweicloud_lts_group.this.id
  log_group_name  = huaweicloud_lts_group.this.group_name
  log_stream_id   = huaweicloud_lts_stream.payment_credit.id
  log_stream_name = huaweicloud_lts_stream.payment_credit.stream_name
}

resource "huaweicloud_fgs_function" "inventory_reserve" {
  name        = "${var.app_name}-inventory-reserve"
  app         = "default"
  handler     = "inventory-reserve.handler"
  memory_size = 128
  timeout     = 15
  runtime     = "Node.js18.15"
  code_type   = "inline"
  func_code   = base64encode(file("${path.module}/src/inventory-reserve.js"))

  user_data = jsonencode(merge(local.geminidb_env, {
    TABLE_NAME = local.inventory_table_name
  }))

  enable_lts_log  = true
  log_group_id    = huaweicloud_lts_group.this.id
  log_group_name  = huaweicloud_lts_group.this.group_name
  log_stream_id   = huaweicloud_lts_stream.inventory_reserve.id
  log_stream_name = huaweicloud_lts_stream.inventory_reserve.stream_name
}

resource "huaweicloud_fgs_function" "inventory_release" {
  name        = "${var.app_name}-inventory-release"
  app         = "default"
  handler     = "inventory-release.handler"
  memory_size = 128
  timeout     = 15
  runtime     = "Node.js18.15"
  code_type   = "inline"
  func_code   = base64encode(file("${path.module}/src/inventory-release.js"))

  user_data = jsonencode(merge(local.geminidb_env, {
    TABLE_NAME = local.inventory_table_name
  }))

  enable_lts_log  = true
  log_group_id    = huaweicloud_lts_group.this.id
  log_group_name  = huaweicloud_lts_group.this.group_name
  log_stream_id   = huaweicloud_lts_stream.inventory_release.id
  log_stream_name = huaweicloud_lts_stream.inventory_release.stream_name
}

resource "huaweicloud_fgs_function" "orchestrator" {
  depends_on = [huaweicloud_identity_agency.orchestrator]

  name        = "${var.app_name}-orchestrator"
  app         = "default"
  handler     = "orchestrator.handler"
  memory_size = 128
  timeout     = 180
  runtime     = "Node.js18.15"
  code_type   = "inline"
  func_code   = base64encode(file("${path.module}/src/orchestrator.js"))
  agency      = huaweicloud_identity_agency.orchestrator.name

  user_data = jsonencode({
    ORDER_NEW_FUNCTION_URN         = "${trimsuffix(huaweicloud_fgs_function.order_new.urn, ":latest")}:latest"
    ORDER_UPDATE_FUNCTION_URN      = "${trimsuffix(huaweicloud_fgs_function.order_update.urn, ":latest")}:latest"
    PAYMENT_DEBIT_FUNCTION_URN     = "${trimsuffix(huaweicloud_fgs_function.payment_debit.urn, ":latest")}:latest"
    PAYMENT_CREDIT_FUNCTION_URN    = "${trimsuffix(huaweicloud_fgs_function.payment_credit.urn, ":latest")}:latest"
    INVENTORY_RESERVE_FUNCTION_URN = "${trimsuffix(huaweicloud_fgs_function.inventory_reserve.urn, ":latest")}:latest"
    INVENTORY_RELEASE_FUNCTION_URN = "${trimsuffix(huaweicloud_fgs_function.inventory_release.urn, ":latest")}:latest"
    SMN_TOPIC_URN                  = huaweicloud_smn_topic.saga.topic_urn
    SMN_REGION                     = var.region
  })

  enable_lts_log  = true
  log_group_id    = huaweicloud_lts_group.this.id
  log_group_name  = huaweicloud_lts_group.this.group_name
  log_stream_id   = huaweicloud_lts_stream.orchestrator.id
  log_stream_name = huaweicloud_lts_stream.orchestrator.stream_name
}
