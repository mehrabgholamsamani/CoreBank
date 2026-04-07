# ADR 0010: Rail outcomes are deterministic, asynchronous simulator events

Rail Simulator owns simulated external lifecycle only. Payment sends `rail.submit-payment.v1` after funds are reserved. Rail persists the requested deterministic scenario and emits acceptance, settlement, rejection, or timeout events through its local outbox. Payment never treats acceptance as settlement; only a settlement event triggers Ledger posting. Duplicates and out-of-order events are deliberate scenarios and must have one effective Payment transition through its inbox.
