export { KafkaClient, KafkaClientConfig } from "./core/kafka-client";
export { KafkaProducer } from "./core/kafka-producer";
export { KafkaConsumerManager } from "./core/kafka-consumer.manager";
export { KafkaConsumerRegistry } from "./registry/kafka-consumer.registry";
export { KafkaConsumer } from "./decorators/kafka-consumer.decorator";
export type {
  PendingRequest,
  KafkaMessageEnvelope,
  KafkaReplyEnvelope,
  KafkaRequestReplyOptions,
  KafkaProducerConfig,
} from "./types";

// import from kafkajs directly
export type {
  EachMessagePayload,
  KafkaMessage,
  IHeaders,
  Producer,
  Consumer,
  Kafka,
} from "kafkajs";
