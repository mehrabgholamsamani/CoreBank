# Monitoring and tracing

Prometheus is available at `http://localhost:9090`, Grafana at `http://localhost:3008`, and Tempo at `http://localhost:3200` in the local sandbox stack.

1. Check Prometheus Targets; Gateway and Reconciliation must be `up` before investigating application data.
2. Use the CoreBank dashboard for service availability. An unavailable target indicates a process, network, or `/metrics` failure.
3. Search Grafana Tempo by correlation ID when the ID is included as trace metadata. Follow the request from Gateway to Reconciliation.
4. Do not paste authorization headers, tokens, passwords, or simulated customer data into Grafana annotations, logs, or incident notes.
5. If telemetry storage is unavailable, financial processing continues; use the durable service databases and outbox/inbox records for investigation.
