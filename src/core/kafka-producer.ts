import { Kafka, Producer } from "kafkajs";
import { KafkaConsumerManager } from "./kafka-consumer.manager";
import { KafkaConsumerRegistry } from "../registry/kafka-consumer.registry";
import { PendingRequest, KafkaProducerConfig, KafkaRequestReplyOptions } from "../types";
import { generateCorrelationId } from "./utils";

export class KafkaProducer {
  private producer: Producer;
  private consumerManager: KafkaConsumerManager;
  private pendingRequests = new Map<string, PendingRequest>();
  private subscribedReplyTopics = new Set<string>();
  private config: Required<KafkaProducerConfig>;

  constructor(
    kafka: Kafka,
    consumerManager: KafkaConsumerManager,
    registry: KafkaConsumerRegistry,
    config: KafkaProducerConfig = {},
  ) {
    this.producer = kafka.producer();
    this.consumerManager = consumerManager;
    this.config = {
      defaultTimeoutMs: config.defaultTimeoutMs ?? 10_000,
    };

    // Bind reply resolution into the registry so it can resolve Promises
    // without creating a circular import between producer ↔ registry
    registry.bindProducer({
      resolveReply: this.resolveReply.bind(this),
      rejectReply: this.rejectReply.bind(this),
      sendRaw: this.sendRaw.bind(this),
    });
  }

  async connect(): Promise<void> {
    await this.producer.connect();
    console.log("[KafkaProducer] Connected");
  }

  async disconnect(): Promise<void> {
    await this.producer.disconnect();
    console.log("[KafkaProducer] Disconnected");
  }

  async sendRaw(payload: unknown): Promise<void> {
    await this.producer.send(payload as Parameters<Producer["send"]>[0]);
  }

  /**
   * Fire-and-forget: send a message with no reply expected.
   *
   * @example
   * await producer.emit('orders.created', { orderId: '123' });
   */
  async emit<TRequest>(topic: string, payload: TRequest): Promise<void> {
    await this.sendRaw({
      topic,
      messages: [
        {
          value: JSON.stringify({ data: payload }),
        },
      ],
    });
  }

  /**
   * Request-reply: send a message and await the correlated response.
   *
   * Automatically subscribes to the replyTopic on first use — no separate
   * consumer class needed.
   *
   * @example
   * const result = await producer.request<CreateOrderInput, CreateOrderOutput>(
   *   'orders.create.request',
   *   'orders.create.reply',
   *   { items: ['item-1'] },
   * );
   */
  async request<TRequest, TResponse>(
    requestTopic: string,
    replyTopic: string,
    payload: TRequest,
    options: KafkaRequestReplyOptions = {},
  ): Promise<TResponse> {
    const timeoutMs = options.timeoutMs ?? this.config.defaultTimeoutMs;

    // Auto-subscribe to reply topic on first use
    if (!this.subscribedReplyTopics.has(replyTopic)) {
      this.subscribedReplyTopics.add(replyTopic);
      await this.consumerManager.subscribeReplyTopic(replyTopic);
    }

    const correlationId = generateCorrelationId();
    console.log(`[KafkaProducer] [${correlationId}] Sending request → ${requestTopic}`);

    await this.sendRaw({
      topic: requestTopic,
      messages: [
        {
          key: options.key,
          value: JSON.stringify({ data: payload }),
          headers: {
            correlationId,
            replyTopic,
          },
        },
      ],
    });

    return new Promise<TResponse>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pendingRequests.delete(correlationId);
        reject(
          new Error(
            `[KafkaProducer] [${correlationId}] Request to "${requestTopic}" timed out after ${timeoutMs}ms`,
          ),
        );
      }, timeoutMs);

      this.pendingRequests.set(correlationId, {
        resolve: resolve as (value: unknown) => void,
        reject,
        timeout,
      });
    });
  }

  /** @internal — called by the registry when a reply arrives */
  resolveReply(correlationId: string, data: unknown): void {
    console.log(`[KafkaProducer] [${correlationId}] Reply received ✓`);
    const pending = this.pendingRequests.get(correlationId);
    if (!pending) return;

    clearTimeout(pending.timeout);
    pending.resolve(data);
    this.pendingRequests.delete(correlationId);
  }

  /** @internal — called by the registry when the remote handler throws */
  rejectReply(correlationId: string, error: Error): void {
    console.error(`[KafkaProducer] [${correlationId}] Reply error:`, error.message);
    const pending = this.pendingRequests.get(correlationId);
    if (!pending) return;

    clearTimeout(pending.timeout);
    pending.reject(error);
    this.pendingRequests.delete(correlationId);
  }

  getInstance(): Producer {
    return this.producer;
  }
}