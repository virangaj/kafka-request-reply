export { KafkaModule, KafkaModuleOptions, KafkaModuleAsyncOptions } from "./kafka.module";
export { KafkaConsumerExplorer } from "./kafka-consumer.explorer";
export { KAFKA_MODULE_OPTIONS } from "./kafka.constants"

export { KafkaProducer } from "../core/kafka-producer";
export { KafkaConsumerRegistry } from "../registry/kafka-consumer.registry";
export { KafkaConsumer } from "../decorators/kafka-consumer.decorator";
export type { KafkaRequestReplyOptions, KafkaProducerConfig } from "../types";

export type { EachMessagePayload, KafkaMessage, IHeaders } from "kafkajs";