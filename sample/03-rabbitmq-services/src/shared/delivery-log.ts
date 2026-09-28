import { Injectable } from '@nestjs/common';
import type { RabbitConsumeResult } from '@nest-native/messaging/rabbitmq';

type Listener = (result: RabbitConsumeResult) => void;

/**
 * What the inbox did with each delivery: processed, duplicate, dead-lettered
 * or requeued. A real service turns this into metrics and logs; here the
 * process also reports it to the smoke test that started it.
 */
@Injectable()
export class DeliveryLog {
  private readonly listeners: Listener[] = [];

  record(result: RabbitConsumeResult): void {
    for (const listener of this.listeners) {
      listener(result);
    }
  }

  listen(listener: Listener): void {
    this.listeners.push(listener);
  }
}
