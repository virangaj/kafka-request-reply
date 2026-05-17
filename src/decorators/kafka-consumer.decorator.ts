export const KAFKA_CONSUMER_TOPIC = "KAFKA_CONSUMER_TOPIC";

// Internal map: method function -> topic string
const topicMap = new WeakMap<Function, string>();

export function getKafkaConsumerTopic(fn: Function): string | undefined {
  return topicMap.get(fn);
}

/**
 * Marks a method as a Kafka request consumer for the given topic.
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
  function legacyDecorator(
    _target: any,
    _propertyKey: string | symbol,
    descriptor: PropertyDescriptor,
  ): void {
    topicMap.set(descriptor.value, topic);
  }

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
      legacyDecorator(targetOrValue, propertyKeyOrContext, descriptor);
    } else {
      modernDecorator(targetOrValue, propertyKeyOrContext);
    }
  };
}