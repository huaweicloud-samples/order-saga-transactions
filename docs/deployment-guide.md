# 部署与运维指南

本文说明分布式 Saga 事务处理示例（order-saga-transactions）的前置准备与部署流程。
架构设计详见 [architecture.md](architecture.md)。

## 1. 前置准备

- 华为云账号，具备 FunctionGraph、GeminiDB、SMN、LTS 及 IAM 权限。
- 本地安装 Terraform ≥ 1.5、Python 3.9+、Node.js 18+。
- 配置认证环境变量：
  ```bash
  export HW_ACCESS_KEY="<your-ak>"
  export HW_SECRET_KEY="<your-sk>"
  export HW_REGION_NAME="cn-north-4"
  ```

## 2. 部署步骤

```bash
cd infra
terraform init
terraform plan -out=tfplan
terraform apply tfplan
```

## 3. 功能验证

执行测试套件模拟正常流程与各类补偿分支：
```bash
python scripts/test_saga.py
```

## 4. 资源清理

```bash
cd infra
terraform destroy -auto-approve
```
