// No reflect-metadata needed — uses a WeakMap instead.
// Works with both legacy (experimentalDecorators) and modern TC39 decorators.

export const KAFKA_CONSUMER_TOPIC = "KAFKA_CONSUMER_TOPIC";

// Internal map: method function → topic string
const topicMap = new WeakMap<Function, string>();

export function getKafkaConsumerTopic(fn: Function): string | undefined {
  return topicMap.get(fn);
}

/**
 * Marks a method as a Kafka request consumer for the given topic.
 *
 * Works with both legacy and modern TypeScript decorators —
 * no `experimentalDecorators` or `reflect-metadata` required.
 *
 * @example
 * class OrderConsumer {
 *   @KafkaConsumer('orders.create.request')
 *   async handle(payload: EachMessagePayload) {
 *     return { orderId: '123' };
 *   }
 * }
 */
export function KafkaConsumer(topic: string) {
  // Legacy decorator (experimentalDecorators: true)
  function legacyDecorator(
    _target: any,
    _propertyKey: string | symbol,
    descriptor: PropertyDescriptor,
  ): void {
    topicMap.set(descriptor.value, topic);
  }

  // Modern TC39 decorator (TypeScript 5+ default)
  function modernDecorator(
    value: Function,
    _context: ClassMethodDecoratorContext,
  ): void {
    topicMap.set(value, topic);
  }

  // Return a function that handles both signatures
  return function (
    targetOrValue: any,
    propertyKeyOrContext: any,
    descriptor?: PropertyDescriptor,
  ): void {
    if (descriptor !== undefined) {
      // Called with 3 args → legacy decorator
      legacyDecorator(targetOrValue, propertyKeyOrContext, descriptor);
    } else {
      // Called with 2 args → modern decorator
      modernDecorator(targetOrValue, propertyKeyOrContext);
    }
  };
}