# ADR 0008: Ledger posting locks referenced accounts

Ledger posting and reservation checks run in a local `QueryRunner` transaction and lock referenced ledger accounts with `FOR UPDATE`. Idempotency keys are unique and request hashes prevent a reused key from representing a different posting. This targets one business effect despite at-least-once delivery without distributed transactions.
