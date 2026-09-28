import { randomUUID } from 'node:crypto';
import { Injectable } from '@nestjs/common';
import { InjectTransaction } from '@nestjs-cls/transactional';
import { OutboxProducer } from '@nest-native/messaging';
import type { SqliteOutboxStore } from '@nest-native/messaging/sqlite';
import {
  EVENTS,
  type OrderPlaced,
  type ShipmentScheduled,
} from '../shared/contracts';
import { shipments, type ShippingDatabase } from './schema';

const CARRIER = 'acme-freight';

/**
 * Books a shipment and announces it. It runs as the inbox's side effect, inside
 * the transaction the inbox opened for this delivery: the dedup row, the
 * shipment, and the outgoing `shipment.scheduled` event commit together or not
 * at all. Consume once and publish once, in a single step — a crash between the
 * two cannot lose the event or book a second shipment.
 */
@Injectable()
export class ShipmentService {
  constructor(
    @InjectTransaction() private readonly db: ShippingDatabase,
    private readonly producer: OutboxProducer<SqliteOutboxStore>,
  ) {}

  schedule(order: OrderPlaced): void {
    const shipmentId = `shp-${randomUUID().slice(0, 8)}`;
    this.db
      .insert(shipments)
      .values({ id: shipmentId, orderId: order.orderId, carrier: CARRIER })
      .run();
    this.producer.enqueue<ShipmentScheduled>({
      topic: EVENTS.shipmentScheduled,
      payload: { orderId: order.orderId, shipmentId, carrier: CARRIER },
      idempotencyKey: `shipment-scheduled:${order.orderId}`,
    });
  }
}
