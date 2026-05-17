# Changelog

All notable changes to `kafka-request-reply` will be documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

---

## [1.0.0] — Initial Release

### Added

- `@KafkaConsumer(topic)` decorator — marks a method as a Kafka message handler
  - Works with both legacy (`experimentalDecorators`) and modern TC39 TypeScript decorators
  - No `reflect-metadata` required — uses a `WeakMap` internally
- `KafkaClient` — top-level class for plain Node.js usage
  - Wires producer, consumer manager, and registry together
  - `connect()` — connects producer + consumer, subscribes all registered topics, starts consuming
  - `disconnect()` — gracefully flushes producer, commits offsets, leaves consumer group
- `KafkaProducer`
  - `emit<TRequest>(topic, payload)` — fire-and-forget, no reply expected
  - `request<TRequest, TResponse>(requestTopic, replyTopic, payload, options?)` — request-reply, awaits correlated response
  - Auto-subscribes to reply topics on first use — no separate consumer class needed
  - Per-request timeout with meaningful error message on expiry
  - Error propagation — if the handler throws, the error is sent back and the Promise rejects
- `KafkaConsumerManager` — manages subscribe, run, and late topic subscription
- `KafkaConsumerRegistry` — routes incoming messages to the correct handler
  - Reply path: no registered handler + `correlationId` header → resolves pending Promise
  - Request path: registered handler → calls handler → auto-replies if routing headers present
- NestJS support via `kafka-request-reply/nestjs` subpath
  - `KafkaModule.register()` — synchronous registration
  - `KafkaModule.registerAsync()` — async registration (ConfigService, etc.)
  - `KafkaConsumerExplorer` — auto-discovers `@KafkaConsumer` methods across all NestJS providers
  - `OnApplicationShutdown` — automatic graceful disconnect when NestJS shuts down
- `kafkajs` bundled as a direct dependency — users install one package only
- All `kafkajs` types (`EachMessagePayload`, `KafkaMessage`, `IHeaders`, etc.) re-exported — no direct `kafkajs` import needed
- Full TypeScript support with declaration files for all exports
- Dual CJS + ESM build output

### Multi-pod support

- Per-pod reply topic pattern documented — use `POD_NAME` from Kubernetes downward API
- `SIGTERM` handling documented for graceful Kubernetes pod termination

[1.0.0]: https://github.com/YOUR_USERNAME/kafka-request-reply/releases/tag/v1.0.0