output "orchestrator_function_name" {
  description = "编排函数名称"
  value       = huaweicloud_fgs_function.orchestrator.name
}

output "orchestrator_function_urn" {
  description = "编排函数 URN"
  value       = "${trimsuffix(huaweicloud_fgs_function.orchestrator.urn, ":latest")}:latest"
}

output "step_function_urns" {
  description = "六个事务步骤函数 URN（正向与补偿）"
  value = {
    "order-new"         = "${trimsuffix(huaweicloud_fgs_function.order_new.urn, ":latest")}:latest"
    "order-update"      = "${trimsuffix(huaweicloud_fgs_function.order_update.urn, ":latest")}:latest"
    "payment-debit"     = "${trimsuffix(huaweicloud_fgs_function.payment_debit.urn, ":latest")}:latest"
    "payment-credit"    = "${trimsuffix(huaweicloud_fgs_function.payment_credit.urn, ":latest")}:latest"
    "inventory-reserve" = "${trimsuffix(huaweicloud_fgs_function.inventory_reserve.urn, ":latest")}:latest"
    "inventory-release" = "${trimsuffix(huaweicloud_fgs_function.inventory_release.urn, ":latest")}:latest"
  }
}

output "smn_topic_urn" {
  description = "SMN 通知主题 URN"
  value       = huaweicloud_smn_topic.saga.topic_urn
}

output "lts_log_group_id" {
  description = "LTS 日志组 ID"
  value       = huaweicloud_lts_group.this.id
}

output "lts_log_group_name" {
  description = "LTS 日志组名称"
  value       = huaweicloud_lts_group.this.group_name
}

output "lts_stream_ids" {
  description = "各函数的 LTS 日志流 ID"
  value = {
    orchestrator        = huaweicloud_lts_stream.orchestrator.id
    "order-new"         = huaweicloud_lts_stream.order_new.id
    "order-update"      = huaweicloud_lts_stream.order_update.id
    "payment-debit"     = huaweicloud_lts_stream.payment_debit.id
    "payment-credit"    = huaweicloud_lts_stream.payment_credit.id
    "inventory-reserve" = huaweicloud_lts_stream.inventory_reserve.id
    "inventory-release" = huaweicloud_lts_stream.inventory_release.id
  }
}

output "saga_table_names" {
  description = "三张业务表名称（GeminiDB 兼容 DynamoDB）"
  value = {
    orders    = local.orders_table_name
    payments  = local.payments_table_name
    inventory = local.inventory_table_name
  }
}

output "agency_name" {
  description = "编排函数 IAM 委托名称"
  value       = huaweicloud_identity_agency.orchestrator.name
}

output "region" {
  description = "部署区域"
  value       = var.region
}

output "resource_count" {
  description = "Terraform 管理的云资源数量"
  value       = 17
}
