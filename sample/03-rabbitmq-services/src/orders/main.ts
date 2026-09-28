import 'reflect-metadata';
import { requiredEnv, runService } from '../shared/service-process';
import { OrderService } from './order.service';
import { OrdersModule } from './orders.module';
import { createOrdersDatabase } from './schema';

const { db, sqlite } = createOrdersDatabase(requiredEnv('ORDERS_DB'));

runService({
  module: OrdersModule.register(db, requiredEnv('RABBITMQ_URL')),
  commands: {
    // What an HTTP `POST /orders` would do.
    place: (app, { orderId, address }) =>
      app.get(OrderService).placeOrder(orderId, address),
  },
  close: () => sqlite.close(),
}).catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
