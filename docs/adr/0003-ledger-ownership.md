# ADR 0003: Ledger is the sole monetary authority

Only the Ledger Service will create immutable monetary journal entries and calculate authoritative balances. Account and Payment services may hold metadata and workflow state but cannot update ledger tables.
