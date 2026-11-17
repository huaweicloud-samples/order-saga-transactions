# order-saga-transactions

[![Status](https://img.shields.io/badge/Status-Incubating-blue)]()
[![Huawei Cloud](https://img.shields.io/badge/Huawei%20Cloud-Samples-red)]()
[![Scenario](https://img.shields.io/badge/Scenario-finance-success)](https://3ms.huawei.com/docs/docinfo/1300901382590492672?bookstackId=866760814559879168&gid=3591759&l=zh-cn&documentkind=&attachmentIdx=5)

An e-commerce order saga on Huawei Cloud `cn-north-4`: one orchestrator function drives the
forward transaction chain `order-new → payment-debit → inventory-reserve`; when any step fails
with a business error, the already-completed work is compensated in reverse order
(`inventory-release → payment-credit → order-update`), the saga outcome is published to an SMN
topic, and the full orchestration trace is written to LTS. Business state is stored in three
tables of an externally provisioned GeminiDB DynamoDB-compatible instance, with deterministic
UUIDv5 transaction IDs so task retries overwrite instead of duplicating records.

## Overview

This repository is created from the huaweicloud-samples automated repository request workflow.

## Getting Started

Add setup, deployment, and verification steps here.

## Contributing

Please use pull requests and follow the repository review rules.

## License

This project is licensed under the MIT-0 license.

## Maintainers

CODEOWNERS: @Ferguson2211

## Feedback

Please use GitHub Issues: https://github.com/huaweicloud-samples/order-saga-transactions/issues
