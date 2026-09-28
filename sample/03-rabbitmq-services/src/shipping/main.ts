import 'reflect-metadata';
import { requiredEnv, runService } from '../shared/service-process';
import { createShippingDatabase } from './schema';
import { ShippingModule } from './shipping.module';

const { db, sqlite } = createShippingDatabase(requiredEnv('SHIPPING_DB'));

// Shipping takes no commands: it only reacts to the events it consumes.
runService({
  module: ShippingModule.register(db, requiredEnv('RABBITMQ_URL')),
  close: () => sqlite.close(),
}).catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
