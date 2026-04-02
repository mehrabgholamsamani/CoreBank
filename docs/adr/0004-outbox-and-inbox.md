# ADR 0004: Reliable Kafka effects use local outbox and inbox records

Producing services persist state and outbox records in one local transaction. Consumers record message IDs, apply work, write resulting outbox records, and mark the inbox item processed atomically. Direct Kafka publication from domain methods is prohibited.
