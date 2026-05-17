import { Consumer, EachMessagePayload, Kafka } from "kafkajs";
import { KafkaConsumerRegistry } from "../registry/kafka-consumer.registry";

export class KafkaConsumerManager {
  private consumer: Consumer;
  private registry: KafkaConsumerRegistry;
  private subscribedTopics = new Set<string>();
  private running = false;

  constructor(kafka: Kafka, groupId: string, registry: KafkaConsumerRegistry) {
    this.consumer = kafka.consumer({ groupId });
    this.registry = registry;
  }

  async connect(): Promise<void> {
    await this.consumer.connect();
    console.log("[KafkaConsumerManager] Connected");
  }

  async disconnect(): Promise<void> {
    await this.consumer.disconnect();
    console.log("[KafkaConsumerManager] Disconnected");
  }

  async subscribe(topic: string, fromBeginning = false): Promise<void> {
    if (this.subscribedTopics.has(topic)) return;

    this.subscribedTopics.add(topic);
    await this.consumer.subscribe({ topic, fromBeginning });
    console.log(`[KafkaConsumerManager] Subscribed to: ${topic}`);
  }

  /**
   * Subscribe to a reply topic at runtime (after run() has been called).
   * KafkaJS requires a consumer restart to pick up new topics.
   */
  async subscribeReplyTopic(topic: string): Promise<void> {
    if (this.subscribedTopics.has(topic)) return;

    console.log(`[KafkaConsumerManager] Auto-subscribing to reply topic: ${topic}`);

    if (this.running) {
      await this.consumer.stop();
    }

    this.subscribedTopics.add(topic);
    await this.consumer.subscribe({ topic, fromBeginning: false });
    await this.run();
  }

  async run(): Promise<void> {
    this.running = true;
    await this.consumer.run({
      eachMessage: async (payload: EachMessagePayload) => {
        await this.handleMessage(payload);
      },
    });
  }

  private async handleMessage(payload: EachMessagePayload): Promise<void> {
    try {
      await this.registry.execute(payload.topic, payload);
    } catch (error) {
      console.error("[KafkaConsumerManager] Unhandled error:", error);
    }
  }
}