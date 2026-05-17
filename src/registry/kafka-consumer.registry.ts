import { EachMessagePayload } from "kafkajs";
import { getKafkaConsumerTopic } from "../decorators/kafka-consumer.decorator";
import { KafkaReplyEnvelope } from "../types";
import { parseEnvelope } from "../core/utils";

type HandlerFn = (payload: EachMessagePayload) => Promise<unknown>;

interface RegisteredHandler {
  fn: HandlerFn;
}

export class KafkaConsumerRegistry {
  private handlers = new Map<string, RegisteredHandler>();

  private resolveReply?: (correlationId: string, data: unknown) => void;
  private rejectReply?: (correlationId: string, error: Error) => void;
  private sendRaw?: (payload: unknown) => Promise<void>;

  bindProducer(options: {
    resolveReply: (correlationId: string, data: unknown) => void;
    rejectReply: (correlationId: string, error: Error) => void;
    sendRaw: (payload: unknown) => Promise<void>;
  }): void {
    this.resolveReply = options.resolveReply;
    this.rejectReply = options.rejectReply;
    this.sendRaw = options.sendRaw;
  }

  register(instance: object): void {
    const proto = Object.getPrototypeOf(instance);

    for (const key of Object.getOwnPropertyNames(proto)) {
      const method = (proto as Record<string, unknown>)[key];
      if (typeof method !== "function") continue;

      // Use WeakMap lookup instead of Reflect.getMetadata
      const topic = getKafkaConsumerTopic(method);
      if (!topic) continue;

      const bound = (method as HandlerFn).bind(instance);
      this.handlers.set(topic, { fn: bound });

      console.log(`[KafkaRegistry] Registered handler for topic: ${topic}`);
    }
  }

  getRegisteredTopics(): string[] {
    return Array.from(this.handlers.keys());
  }

  async execute(topic: string, payload: EachMessagePayload): Promise<void> {
    const headers = payload.message.headers ?? {};
    const correlationId = headers["correlationId"]?.toString();
    const entry = this.handlers.get(topic);

    // Reply path
    if (!entry) {
      if (correlationId) {
        const raw = payload.message.value?.toString();
        if (!raw) return;

        const envelope = parseEnvelope<KafkaReplyEnvelope>(raw);

        if (envelope.error) {
          this.rejectReply?.(correlationId, new Error(envelope.error));
        } else {
          this.resolveReply?.(correlationId, envelope.data);
        }
      } else {
        console.warn(
          `[KafkaRegistry] No handler registered for topic: ${topic}`,
        );
      }
      return;
    }

    // Request path
    const replyTopic = headers["replyTopic"]?.toString();

    try {
      const result = await entry.fn(payload);

      if (replyTopic && correlationId && this.sendRaw) {
        await this.sendRaw({
          topic: replyTopic,
          messages: [
            {
              value: JSON.stringify({
                data: result,
              } satisfies KafkaReplyEnvelope),
              headers: { correlationId },
            },
          ],
        });
      }
    } catch (error) {
      console.error(`[KafkaRegistry] Handler error on topic ${topic}:`, error);

      if (replyTopic && correlationId && this.sendRaw) {
        await this.sendRaw({
          topic: replyTopic,
          messages: [
            {
              value: JSON.stringify({
                data: null,
                error: error instanceof Error ? error.message : String(error),
              } satisfies KafkaReplyEnvelope),
              headers: { correlationId },
            },
          ],
        });
      }
    }
  }
}

export const kafkaConsumerRegistry = new KafkaConsumerRegistry();
