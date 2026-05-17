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

    // Subscribe to all topics that have a @KafkaConsumer handler
    const topics = this.registry.getRegisteredTopics();
    for (const topic of topics) {
      await this.consumerManager.subscribe(topic, false);
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