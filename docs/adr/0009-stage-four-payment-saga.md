# ADR 0009: Internal payments use an asynchronous reservation saga

Payment owns the aggregate and starts an internal-transfer saga by writing a `ledger.reserve-funds.v1` command to its transactional outbox. Ledger remains the sole authority for available funds, reservations, and postings. A successful reservation causes Payment to request an internal posting; the final ledger event marks the payment `SETTLED`. A rejected reservation transitions it to `REJECTED` without changing money. Each command has a deterministic per-payment idempotency key, and consumers deduplicate envelope `messageId` values in their local inbox.

Rail submission, external acceptance, refunds, and reversals remain Stage 5 work.
