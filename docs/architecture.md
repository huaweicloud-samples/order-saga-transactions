# 架构与技术设计

本文说明分布式 Saga 事务处理示例（order-saga-transactions）的系统架构、事务流转与错误补偿设计。
部署指南见 [deployment-guide.md](deployment-guide.md)，功能概览见根目录 `README.md`。

## 1. 架构总览

```
Client / Caller
   │ (同步 HTTP 调用)
   ▼
Saga Orchestrator (FunctionGraph)
   │
   ├── 步骤 1: create-order-status (创建待处理订单)
   ├── 步骤 2: process-payment (扣减支付金额)
   │           └── 失败时 ──► update-order-status(FAILED)
   ├── 步骤 3: update-inventory (锁定并扣减商品库存)
   │           └── 失败时 ──► refund-payment (退还金额)
   │                       └── update-order-status(FAILED)
   └── 步骤 4: complete-order (将订单标记为完成)
               └── 补偿失败 ──► 发布告警至 SMN 主题
```

## 2. 核心组件与职责

| 组件 | 服务 | 说明 |
|---|---|---|
| 主编排器 | FunctionGraph | 同步协调订单全流程事务与补偿执行 |
| 订单微服务 | FunctionGraph | 订单记录创建与状态更新 |
| 支付微服务 | FunctionGraph | 支付处理与退款补偿操作 |
| 库存微服务 | FunctionGraph | 库存锁定校验与库存释放补偿操作 |
| 异常告警 | SMN | 关键补偿执行失败时发送通知通知运维 |
| 数据持久化 | GeminiDB | 存储订单表、支付记录表与库存记录表 |
| 可观测性 | LTS | 编排器及微服务日志实时检索与追踪 |
