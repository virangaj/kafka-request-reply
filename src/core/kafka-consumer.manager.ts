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
   * Subscribe to a reply topic.
   *
   * Must be called BEFORE run() to avoid consumer group rebalancing.
   * KafkaClient.connect() handles this automatically when replyTopics
   * is passed in the config — use that instead of calling this directly.
   *
   * If called after run() (late subscription), a warn is logged and the
   * consumer is restarted — this will cause a brief rebalance.
   */
  async subscribeReplyTopic(topic: string): Promise<void> {
    if (this.subscribedTopics.has(topic)) return;

    if (this.running) {
      console.warn(
        `[KafkaConsumerManager] Late subscription to reply topic "${topic}". ` +
          `Pass it in replyTopics config to avoid rebalancing.`,
      );
      await this.consumer.stop();
      this.running = false;
      this.subscribedTopics.add(topic);
      await this.consumer.subscribe({ topic, fromBeginning: false });
      await this.run();
    } else {
      this.subscribedTopics.add(topic);
      await this.consumer.subscribe({ topic, fromBeginning: false });
    }
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
