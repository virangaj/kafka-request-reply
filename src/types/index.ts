export interface PendingRequest {
  resolve: (value: unknown) => void;
  reject: (reason?: unknown) => void;
  timeout: ReturnType<typeof setTimeout>;
}

export interface KafkaMessageEnvelope<T = unknown> {
  data: T;
}

export interface KafkaReplyEnvelope<T = unknown> {
  data: T | null;
  error?: string;
}

export interface KafkaRequestReplyOptions {
  /** How long to wait for a reply before rejecting. Default: 10000 ms */
  timeoutMs?: number;
  /** Optional Kafka message key */
  key?: string;
}

export interface KafkaProducerConfig {
  /** Default timeout for all request() calls. Default: 10000 ms */
  defaultTimeoutMs?: number;
}