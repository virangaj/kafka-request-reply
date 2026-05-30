# kafka-request-reply

`kafka-request-reply` is a lightweight KafkaJS library for implementing fire-and-forget and request-reply messaging patterns in Node.js and NestJS. It simplifies correlation IDs, reply topics, and timeout handling for building scalable event-driven microservices.

Works with plain **Node.js** and **NestJS**. Full TypeScript support. No `reflect-metadata` required.

---

## Install

```bash
npm install kafka-request-reply
```

`kafkajs` is included — no separate installs needed.

---

## Plain Node.js

### Project structure

```
src/
├── index.ts                       ← bootstrap
├── app.ts                         ← express setup
├── kafka/
│   ├── kafka.client.ts            ← KafkaClient singleton
│   ├── kafka.constants.ts         ← topic names, group id
│   └── register-consumers.ts     ← register all @KafkaConsumer classes
├── consumers/
│   ├── user-update.consumer.ts   ← request-reply handler
│   └── order-create.consumer.ts  ← fire-and-forget handler
└── routes/
    ├── user.routes.ts
    └── order.routes.ts
```

### 1. Kafka constants

```ts
// src/kafka/kafka.constants.ts
export const KAFKA_GROUP_ID = "my-app-group";

export const KAFKA_REQUEST_TOPICS = {
  USER_UPDATE_REQUEST: "user.update.request",
  ORDER_CREATE_REQUEST: "order.create.request",
} as const;

export const KAFKA_REPLY_TOPICS = {
  USER_UPDATE_REPLY: "user.update.reply",
} as const;
```

### 2. Kafka client

```ts
// src/kafka/kafka.client.ts
import { KafkaClient } from "kafka-request-reply";
import { KAFKA_GROUP_ID } from "./kafka.constants";

export const kafkaClient = new KafkaClient({
  kafka: {
    clientId: "my-app",
    brokers: ["localhost:9092"],
  },
  groupId: KAFKA_GROUP_ID,
  consumers: [],
});
```

### 3. Register consumers

```ts
// src/kafka/register-consumers.ts
import { kafkaClient } from "./kafka.client";
import { UserUpdateConsumer } from "../consumers/user-update.consumer";
import { OrderCreateConsumer } from "../consumers/order-create.consumer";

export function registerConsumers(): void {
  kafkaClient.registry.register(new UserUpdateConsumer());
  kafkaClient.registry.register(new OrderCreateConsumer());
}
```

### 4. Define consumers

```ts
// src/consumers/user-update.consumer.ts
import { KafkaConsumer, EachMessagePayload } from "kafka-request-reply";
import { KAFKA_REQUEST_TOPICS } from "../kafka/kafka.constants";

export interface UpdateUserInput {
  id: number;
  name: string;
}

export interface UpdateUserOutput {
  success: boolean;
  updatedAt: string;
  user: UpdateUserInput;
}

export class UserUpdateConsumer {
  // Request-reply — return value is automatically sent back to the caller
  @KafkaConsumer(KAFKA_REQUEST_TOPICS.USER_UPDATE_REQUEST)
  async handle(payload: EachMessagePayload): Promise<UpdateUserOutput> {
    const { data } = JSON.parse(payload.message.value!.toString()) as {
      data: UpdateUserInput;
    };

    // your business logic here...

    return {
      success: true,
      updatedAt: new Date().toISOString(),
      user: data,
    };
  }
}
```

```ts
// src/consumers/order-create.consumer.ts
import { KafkaConsumer, EachMessagePayload } from "kafka-request-reply";
import { KAFKA_REQUEST_TOPICS } from "../kafka/kafka.constants";

export interface CreateOrderInput {
  userId: number;
  items: string[];
}

export class OrderCreateConsumer {
  // Fire-and-forget — no return value needed
  @KafkaConsumer(KAFKA_REQUEST_TOPICS.ORDER_CREATE_REQUEST)
  async handle(payload: EachMessagePayload): Promise<void> {
    const { data } = JSON.parse(payload.message.value!.toString()) as {
      data: CreateOrderInput;
    };

    // your business logic here...
  }
}
```

### 5. Bootstrap

```ts
// src/index.ts
import app from "./app";
import { kafkaClient } from "./kafka/kafka.client";
import { registerConsumers } from "./kafka/register-consumers";

const PORT = 3000;

async function bootstrap() {
  // 1. Register all @KafkaConsumer decorated classes
  registerConsumers();

  // 2. Connect Kafka and start consuming
  await kafkaClient.connect();

  // 3. Start HTTP server
  app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
  });

  // 4. Graceful shutdown — handles both Ctrl+C and Kubernetes pod termination
  async function shutdown() {
    console.log("Shutting down...");
    await kafkaClient.disconnect();
    process.exit(0);
  }

  process.on("SIGINT", shutdown);   // local dev
  process.on("SIGTERM", shutdown);  // Kubernetes
}

bootstrap().catch(console.error);
```

### 6. Routes

```ts
// src/routes/user.routes.ts — request-reply
import { Router, Request, Response } from "express";
import { kafkaClient } from "../kafka/kafka.client";
import { KAFKA_REQUEST_TOPICS, KAFKA_REPLY_TOPICS } from "../kafka/kafka.constants";
import { UpdateUserInput, UpdateUserOutput } from "../consumers/user-update.consumer";

const router = Router();

router.post("/:id", async (req: Request, res: Response) => {
  try {
    const result = await kafkaClient.producer.request<UpdateUserInput, UpdateUserOutput>(
      KAFKA_REQUEST_TOPICS.USER_UPDATE_REQUEST,
      KAFKA_REPLY_TOPICS.USER_UPDATE_REPLY,
      { id: Number(req.params.id), name: req.body.name },
    );
    res.json({ data: result });
  } catch (err) {
    res.status(504).json({ error: (err as Error).message });
  }
});

export default router;
```

```ts
// src/routes/order.routes.ts — fire-and-forget
import { Router, Request, Response } from "express";
import { kafkaClient } from "../kafka/kafka.client";
import { KAFKA_REQUEST_TOPICS } from "../kafka/kafka.constants";
import { CreateOrderInput } from "../consumers/order-create.consumer";

const router = Router();

router.post("/", async (req: Request, res: Response) => {
  await kafkaClient.producer.emit<CreateOrderInput>(
    KAFKA_REQUEST_TOPICS.ORDER_CREATE_REQUEST,
    { userId: req.body.userId, items: req.body.items },
  );
  res.json({ message: "Order event emitted" });
});

export default router;
```

---

## NestJS

### 1. Register the module

```ts
// app.module.ts
import { Module } from "@nestjs/common";
import { KafkaModule } from "kafka-request-reply/nestjs";

@Module({
  imports: [
    KafkaModule.register({
      clientId: "my-app",
      brokers: ["localhost:9092"],
      groupId: "my-app-group",
    }),
  ],
})
export class AppModule {}
```

Async registration with `ConfigService`:

```ts
KafkaModule.registerAsync({
  useFactory: (config: ConfigService) => ({
    clientId: config.get("KAFKA_CLIENT_ID"),
    brokers: [config.get("KAFKA_BROKER")],
    groupId: config.get("KAFKA_GROUP_ID"),
  }),
  inject: [ConfigService],
})
```

### 2. Define a consumer service

```ts
// order.consumer.ts
import { Injectable } from "@nestjs/common";
import { KafkaConsumer, EachMessagePayload } from "kafka-request-reply/nestjs";

@Injectable()
export class OrderConsumer {
  @KafkaConsumer("orders.create.request")
  async handle(payload: EachMessagePayload) {
    const { data } = JSON.parse(payload.message.value!.toString());
    return { orderId: "123", status: "created" };
  }
}
```

`KafkaConsumerExplorer` (included in `KafkaModule`) automatically discovers all `@KafkaConsumer` methods across your providers — no manual registration needed.

### 3. Inject the producer

```ts
// order.controller.ts
import { Controller, Post, Body } from "@nestjs/common";
import { KafkaProducer } from "kafka-request-reply/nestjs";

@Controller("orders")
export class OrderController {
  constructor(private readonly producer: KafkaProducer) {}

  @Post("notify")
  async notify(@Body() body: { userId: number; items: string[] }) {
    await this.producer.emit("orders.created", body);
    return { message: "Event emitted" };
  }

  @Post()
  async create(@Body() body: { items: string[] }) {
    const result = await this.producer.request(
      "orders.create.request",
      "orders.create.reply",
      body,
    );
    return result;
  }
}
```

### 4. Graceful shutdown (NestJS)

`KafkaModule` implements `OnApplicationShutdown` and disconnects automatically.  
Just enable shutdown hooks in `main.ts`:

```ts
// main.ts
const app = await NestFactory.create(AppModule);
app.enableShutdownHooks(); // handles SIGTERM and SIGINT automatically
await app.listen(3000);
```

---

## API

### `@KafkaConsumer(topic: string)`

Method decorator. Marks a method as the handler for incoming messages on `topic`.

- Receives `EachMessagePayload`
- Return value is automatically sent back as the reply for `request()` callers
- Return value is discarded for `emit()` callers
- Works with both legacy (`experimentalDecorators`) and modern TypeScript decorators
- No `reflect-metadata` required

### `producer.emit<TRequest>(topic, payload)`

Fire-and-forget. Sends the message and returns immediately.

```ts
await kafkaClient.producer.emit("orders.created", { orderId: "123" });
```

### `producer.request<TRequest, TResponse>(requestTopic, replyTopic, payload, options?)`

Sends a request and returns a `Promise<TResponse>` that resolves when the consumer replies.

```ts
const result = await kafkaClient.producer.request<Input, Output>(
  "orders.create.request",
  "orders.create.reply",
  { items: ["item-1"] },
  { timeoutMs: 10000 },
);
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `timeoutMs` | `number` | `60000` | Reject after this many ms |
| `key` | `string` | — | Kafka message key |

### `KafkaClient` (Node.js only)

```ts
const client = new KafkaClient({
  kafka: { clientId: "my-app", brokers: ["localhost:9092"] },
  groupId: "my-app-group",
  consumers: [],
  producer: {
    defaultTimeoutMs: 60000,
  },
});

await client.connect();    // connect, subscribe all topics, start consuming
await client.disconnect(); // flush producer, commit offsets, leave group
```

---

## Error handling

If a `@KafkaConsumer` handler throws, the error propagates back to the `request()` caller — no silent timeouts.

```ts
try {
  const result = await producer.request("req.topic", "reply.topic", payload);
} catch (err) {
  // handler threw  → err.message = whatever the handler threw
  // timeout        → err.message = "Request to ... timed out after 60000ms"
}
```

---

## Scalability summary

| Scenario | Works | Notes |
|---|---|---|
| Single pod, fire-and-forget | ✅ | No config needed |
| Single pod, request-reply | ✅ | No config needed |
| Multiple pods, fire-and-forget | ✅ | Kafka group balancing handles it |
| Graceful shutdown (SIGTERM) | ✅ | Handle both `SIGINT` and `SIGTERM` |
| Kubernetes pod termination | ✅ | `disconnect()` sends LeaveGroup immediately |