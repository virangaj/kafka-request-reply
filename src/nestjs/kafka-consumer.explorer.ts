import { Injectable, OnModuleInit } from "@nestjs/common";
import { DiscoveryService, MetadataScanner } from "@nestjs/core";
import { KafkaConsumerRegistry } from "../registry/kafka-consumer.registry";
import { KafkaConsumerManager } from "../core/kafka-consumer.manager";
import { KafkaProducer } from "../core/kafka-producer";
import { getKafkaConsumerTopic } from "../decorators/kafka-consumer.decorator";

@Injectable()
export class KafkaConsumerExplorer implements OnModuleInit {
  constructor(
    private readonly discovery: DiscoveryService,
    private readonly metadataScanner: MetadataScanner,
    private readonly registry: KafkaConsumerRegistry,
    private readonly consumerManager: KafkaConsumerManager,
    private readonly producer: KafkaProducer,
  ) {}

  async onModuleInit(): Promise<void> {
    const wrappers = this.discovery.getProviders();

    for (const wrapper of wrappers) {
      const { instance } = wrapper;
      if (!instance || typeof instance !== "object") continue;

      const proto = Object.getPrototypeOf(instance);

      this.metadataScanner.scanFromPrototype(instance, proto, (key: string) => {
        const method = proto[key];
        if (typeof method !== "function") return;

        // Use WeakMap lookup — no reflect-metadata needed
        const topic = getKafkaConsumerTopic(method);
        if (!topic) return;

        this.registry.register(instance);
      });
    }

    await this.producer.connect();
    await this.consumerManager.connect();

    const topics = this.registry.getRegisteredTopics();
    for (const topic of topics) {
      await this.consumerManager.subscribe(topic, false);
    }

    if (topics.length > 0) {
      await this.consumerManager.run();
    }
  }
}