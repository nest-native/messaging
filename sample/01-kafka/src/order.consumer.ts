import { Inject, Injectable } from '@nestjs/common';
import {
  KafkaConsumer,
  KafkaContext,
  KafkaCtx,
  KafkaHandler,
  KafkaHeaders,
  KafkaMessage,
} from '@nest-native/kafka';
import { type WireHeaderValue } from '@nest-native/messaging';
import { KafkaInboxConsumer } from '@nest-native/messaging/kafka';
import { OrderAuditService } from './order-audit.service';

export const ORDER_TOPIC = 'order.placed';
const GROUP_ID = 'orders-service';
const DLQ_TOPIC = `${ORDER_TOPIC}.DLQ`;
// `source` scopes dedup keys to this topic+group in the shared inbox table.
const SOURCE = `${ORDER_TOPIC}:${GROUP_ID}`;

interface OrderPlaced {
  id: string;
  item: string;
}
function isOrderPlaced(value: unknown): value is OrderPlaced {
  return (
    typeof value === 'object' &&
    value !== null &&
    typeof (value as OrderPlaced).id === 'string' &&
    typeof (value as OrderPlaced).item === 'string'
  );
}

/**
 * A thin `@KafkaConsumer` shell over the library's {@link KafkaInboxConsumer}
 * engine: it owns the static topic/group/DLQ and supplies the concrete payload
 * validator + the exactly-once side effect (the audit write). The engine does
 * all async broker work (parse, ack, dead-letter) OUTSIDE the dedup transaction
 * and runs `InboxService.runOnce` INSIDE it — so a redelivery is deduplicated.
 */
@Injectable()
@KafkaConsumer(ORDER_TOPIC, { groupId: GROUP_ID })
export class OrderConsumer {
  constructor(
    @Inject(KafkaInboxConsumer) private readonly inbox: KafkaInboxConsumer,
    @Inject(OrderAuditService) private readonly audit: OrderAuditService,
  ) {}

  @KafkaHandler()
  async handle(
    @KafkaMessage() payload: unknown,
    @KafkaHeaders() headers: Record<string, WireHeaderValue>,
    @KafkaCtx() context: KafkaContext,
  ): Promise<void> {
    await this.inbox.consume<OrderPlaced>({
      source: SOURCE,
      context,
      headers,
      payload,
      validate: isOrderPlaced,
      sideEffect: (order, dedupKey) => this.audit.record(dedupKey, order.item),
      dlqTopic: DLQ_TOPIC,
    });
  }
}
