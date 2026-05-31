import { Kafka, KafkaConfig } from "kafkajs";
import { KafkaConsumerManager } from "./kafka-consumer.manager";
import { KafkaProducer } from "./kafka-producer";
import { KafkaConsumerRegistry } from "../registry/kafka-consumer.registry";
import { KafkaProducerConfig } from "../types";

export interface KafkaClientConfig {
  /** KafkaJS client config (brokers, clientId, ssl, sasl, etc.) */
  kafka: KafkaConfig;
  /** Consumer group ID */
  groupId: string;
  /** Consumer classes decorated with @KafkaConsumer to register */
  consumers?: object[];
  producer?: KafkaProducerConfig;

  /**
   * Reply topics to subscribe to before run() is called.
   * This prevents consumer group rebalancing caused by late topic subscription
   * when the first request() call triggers subscribeReplyTopic().
   *
   * Always pass all reply topics your service uses here.
   *
   * @example
   * replyTopics: Object.values(KAFKA_REPLY_TOPICS)
   */
  replyTopics?: string[];
}

/**
 * @example
 * const client = new KafkaClient({
 *   kafka: { clientId: 'my-app', brokers: ['localhost:9092'] },
 *   groupId: 'my-app-group',
 *   consumers: [new OrderConsumer()],
 * });
 *
 * await client.connect();
 *
 * // Fire-and-forget
 * await client.producer.emit('orders.created', { orderId: '123' });
 *
 * // Request-reply
 * const result = await client.producer.request(
 *   'orders.create.request',
 *   'orders.create.reply',
 *   { items: ['item-1'] },
 * );
 */
export class KafkaClient {
  readonly kafka: Kafka;
  readonly registry: KafkaConsumerRegistry;
  readonly consumerManager: KafkaConsumerManager;
  readonly producer: KafkaProducer;
  private replyTopics: string[];

  constructor(config: KafkaClientConfig) {
    this.kafka = new Kafka(config.kafka);
    this.registry = new KafkaConsumerRegistry();
    this.consumerManager = new KafkaConsumerManager(
      this.kafka,
      config.groupId,
      this.registry,
    );
    this.producer = new KafkaProducer(
      this.kafka,
      this.consumerManager,
      this.registry,
      config.producer,
    );

    this.replyTopics = config.replyTopics ?? [];
    // Register all provided consumer instances
    for (const consumer of config.consumers ?? []) {
      this.registry.register(consumer);
    }
  }

  /**
   * Connect producer + consumer, subscribe to all registered topics, and start
   * consuming. Call this once during app bootstrap.
   */
  async connect(): Promise<void> {
    await this.producer.connect();
    await this.consumerManager.connect();

    // Subscribe to @KafkaConsumer request topics
    const requestTopics = this.registry.getRegisteredTopics();
    for (const topic of requestTopics) {
      await this.consumerManager.subscribe(topic, false);
    }

    // Subscribe to reply topics BEFORE run() — prevents rebalancing
    for (const topic of this.replyTopics) {
      await this.consumerManager.subscribe(topic, false);
      console.log(`[KafkaConsumerManager] Subscribed to reply topic: ${topic}`);
    }

    await this.consumerManager.run();
  }

  /**
   * Gracefully disconnect producer and consumer.
   */
  async disconnect(): Promise<void> {
    await this.producer.disconnect();
    await this.consumerManager.disconnect();
  }
}
