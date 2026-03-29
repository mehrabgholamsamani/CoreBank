# ADR 0001: pnpm monorepo and database-per-service

We use pnpm workspaces for the eight independently deployable service shells and the deliberately narrow shared libraries. Each service owns one PostgreSQL database; cross-service changes will be communicated by Kafka events rather than shared tables or entities.
