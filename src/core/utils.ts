import { randomUUID } from "crypto";

export function generateCorrelationId(): string {
  return randomUUID();
}

export function parseEnvelope<T>(raw: string): T {
  try {
    return JSON.parse(raw) as T;
  } catch {
    throw new Error(`[kafkajs-request-reply] Failed to parse message envelope: ${raw}`);
  }
}