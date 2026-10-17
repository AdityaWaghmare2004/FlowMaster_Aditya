# FlowMaster

The Orchestrator loads its Saga steps from `orchestrator/workflow.yaml`. Workflow state changes and outgoing Kafka messages are committed together in Postgres; the Outbox relay publishes committed events to Kafka with at-least-once delivery. The Verification and Credit workers use atomic Redis claims and 24-hour completion markers to ignore duplicate workflow events. Redis is exposed by Docker Compose on `127.0.0.1:6380` to avoid conflicts with a host Redis service; set `REDIS_URL` to override the worker connection URL.

## Saga Workflow

```mermaid
sequenceDiagram
    autonumber
    participant API as Client API
    participant O as Orchestrator
    participant DB as Postgres (State & Outbox)
    participant K as Kafka Broker
    participant V as Verification Service
    participant C as Credit Service

    API->>O: POST /workflows
    O->>DB: Atomic Transaction: Insert PENDING state & Outbox [start_verification]
    O->>K: Outbox Relay: Publish start_verification
    K->>V: Consume start_verification
    Note over V: Redis Idempotency Check -> Verify ID
    V->>K: Publish verification_result (SUCCESS)
    K->>O: Consume verification_result
    O->>DB: Atomic Transaction: Update State & Outbox [start_credit_check]
    O->>K: Outbox Relay: Publish start_credit_check
    K->>C: Consume start_credit_check
    Note over C: Redis Idempotency Check -> Deliberate Rejection
    C->>K: Publish credit_result (FAILED)
    K->>O: Consume credit_result (Detect Failure)
    Note over O: Saga Compensation Triggered
    O->>DB: Atomic Transaction: Mark ROLLED_BACK & Outbox [compensate_verification]
    O->>K: Outbox Relay: Publish compensate_verification
    K->>V: Consume compensate_verification
    Note over V: Redis Idempotency Check -> Undo Verification
```
