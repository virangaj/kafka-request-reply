import { DynamicModule, Global, Module, OnApplicationShutdown, Provider } from "@nestjs/common";
import { ModuleRef } from "@nestjs/core";
import { Kafka } from "kafkajs";
import { KafkaConsumerRegistry } from "../registry/kafka-consumer.registry";
import { KafkaConsumerManager } from "../core/kafka-consumer.manager";
import { KafkaProducer } from "../core/kafka-producer";
import { KafkaProducerConfig } from "../types";
import { KAFKA_MODULE_OPTIONS, KAFKA_CONSUMER_CLASSES } from "./kafka.constants"

export interface KafkaModuleOptions {
  clientId: string;
  brokers: string[];
  groupId: string;
  /** Any extra KafkaJS options (ssl, sasl, retry, etc.) */
  kafkaOptions?: Record<string, unknown>;
  producer?: KafkaProducerConfig;
}

export interface KafkaModuleAsyncOptions {
  useFactory: (...args: unknown[]) => Promise<KafkaModuleOptions> | KafkaModuleOptions;
  inject?: string[];
  imports?: unknown[];
}

@Global()
@Module({})
export class KafkaModule implements OnApplicationShutdown {
  constructor(private moduleRef: ModuleRef) {}

  async onApplicationShutdown(): Promise<void> {
    const producer = this.moduleRef.get(KafkaProducer, { strict: false });
    const consumer = this.moduleRef.get(KafkaConsumerManager, { strict: false });
    await producer?.disconnect().catch(() => undefined);
    await consumer?.disconnect().catch(() => undefined);
  }

  /**
   * Synchronous registration.
   *
   * @example
   * KafkaModule.register({
   *   clientId: 'my-app',
   *   brokers: ['localhost:9092'],
   *   groupId: 'my-app-group',
   * })
   */
  static register(options: KafkaModuleOptions): DynamicModule {
    const providers = KafkaModule.buildProviders();
    return {
      module: KafkaModule,
      providers: [
        { provide: KAFKA_MODULE_OPTIONS, useValue: options },
        ...providers,
      ],
      exports: [KafkaProducer, KafkaConsumerRegistry],
    };
  }

  /**
   * Async registration — use when options come from ConfigService or similar.
   *
   * @example
   * KafkaModule.registerAsync({
   *   useFactory: (config: ConfigService) => ({
   *     clientId: config.get('KAFKA_CLIENT_ID'),
   *     brokers: [config.get('KAFKA_BROKER')],
   *     groupId: config.get('KAFKA_GROUP_ID'),
   *   }),
   *   inject: [ConfigService],
   * })
   */
  static registerAsync(asyncOptions: KafkaModuleAsyncOptions): DynamicModule {
    const providers = KafkaModule.buildProviders();
    return {
      module: KafkaModule,
      imports: (asyncOptions.imports as DynamicModule["imports"]) ?? [],
      providers: [
        {
          provide: KAFKA_MODULE_OPTIONS,
          useFactory: asyncOptions.useFactory,
          inject: (asyncOptions.inject as string[]) ?? [],
        },
        ...providers,
      ],
      exports: [KafkaProducer, KafkaConsumerRegistry],
    };
  }

  private static buildProviders(): Provider[] {
    return [
      {
        provide: KafkaConsumerRegistry,
        useFactory: () => new KafkaConsumerRegistry(),
      },
      {
        provide: KafkaConsumerManager,
        useFactory: (options: KafkaModuleOptions, registry: KafkaConsumerRegistry) => {
          const kafka = new Kafka({
            clientId: options.clientId,
            brokers: options.brokers,
            ...(options.kafkaOptions ?? {}),
          });
          return new KafkaConsumerManager(kafka, options.groupId, registry);
        },
        inject: [KAFKA_MODULE_OPTIONS, KafkaConsumerRegistry],
      },
      {
        provide: KafkaProducer,
        useFactory: (
          options: KafkaModuleOptions,
          consumerManager: KafkaConsumerManager,
          registry: KafkaConsumerRegistry,
        ) => {
          const kafka = new Kafka({
            clientId: options.clientId,
            brokers: options.brokers,
            ...(options.kafkaOptions ?? {}),
          });
          return new KafkaProducer(kafka, consumerManager, registry, options.producer);
        },
        inject: [KAFKA_MODULE_OPTIONS, KafkaConsumerManager, KafkaConsumerRegistry],
      },
    ];
  }
}