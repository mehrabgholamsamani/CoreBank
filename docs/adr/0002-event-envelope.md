# ADR 0002: Versioned event envelopes

All domain events and commands use `MessageEnvelope<TPayload>`. Message types include a `v1` suffix, commands describe requested actions, and events use past tense. Consumers must tolerate additive payload fields within a version.
