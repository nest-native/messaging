import { Injectable } from '@nestjs/common';
import type { RabbitConsumeResult } from '@nest-native/messaging/rabbitmq';

/**
 * What happened to each delivery: processed, duplicate, dead-lettered or
 * requeued. In production this is where metrics and logs come from; the smoke
 * test reads it to assert each path.
 */
@Injectable()
export class DeliveryLog {
  readonly entries: RabbitConsumeResult[] = [];

  record(result: RabbitConsumeResult): void {
    this.entries.push(result);
  }

  count(outcome: RabbitConsumeResult['outcome'], dedupKey: string): number {
    return this.entries.filter(
      entry => entry.outcome === outcome && entry.dedupKey === dedupKey,
    ).length;
  }
}
