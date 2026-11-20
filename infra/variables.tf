variable "region" {
  description = "华为云区域"
  type        = string
  default     = "cn-north-4"
}

variable "app_name" {
  description = "应用前缀；函数、日志组/流、SMN 主题与数据表名称均由该前缀派生"
  type        = string
  default     = "order-saga"

  validation {
    condition     = can(regex("^[a-zA-Z][a-zA-Z0-9-]{2,30}$", var.app_name))
    error_message = "app_name 必须以字母开头，仅含字母、数字、连字符，长度为 3~31。"
  }
}

variable "geminidb_endpoint" {
  description = "来自外部预置的 GeminiDB 兼容 DynamoDB 实例连接地址（负载均衡内网 IP 或主机名，可带协议与端口）；本目录不创建该实例"
  type        = string
  sensitive   = true
}

variable "geminidb_port" {
  description = "来自外部预置的 GeminiDB 兼容 DynamoDB 实例数据面端口；集群形态为 80，单节点形态为 8000"
  type        = number
  default     = 80
  sensitive   = true
}

variable "geminidb_access_key" {
  description = "来自外部预置的 GeminiDB 兼容 DynamoDB 实例数据面用户名（固定 rwuser）；数据面不接受 IAM AK/SK 或委托临时凭据"
  type        = string
  sensitive   = true
}

variable "geminidb_secret_key" {
  description = "来自外部预置的 GeminiDB 兼容 DynamoDB 实例的管理员密码（创建实例时设置）；只经 TF_VAR 环境变量注入，不写入任何文件"
  type        = string
  sensitive   = true
}
