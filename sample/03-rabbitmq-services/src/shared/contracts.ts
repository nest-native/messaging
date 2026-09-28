/**
 * The contract between the two services: the events they exchange and the
 * shape of each payload. It is the only code both sides share — each service
 * validates what it receives against it rather than trusting the sender.
 */
export const EVENTS = {
  orderPlaced: 'order.placed',
  shipmentScheduled: 'shipment.scheduled',
} as const;

export interface OrderPlaced {
  orderId: string;
  address: string;
}

export interface ShipmentScheduled {
  orderId: string;
  shipmentId: string;
  carrier: string;
}

export function isOrderPlaced(value: unknown): value is OrderPlaced {
  const event = value as OrderPlaced;
  return (
    typeof value === 'object' &&
    value !== null &&
    typeof event.orderId === 'string' &&
    typeof event.address === 'string'
  );
}

export function isShipmentScheduled(value: unknown): value is ShipmentScheduled {
  const event = value as ShipmentScheduled;
  return (
    typeof value === 'object' &&
    value !== null &&
    typeof event.orderId === 'string' &&
    typeof event.shipmentId === 'string' &&
    typeof event.carrier === 'string'
  );
}
