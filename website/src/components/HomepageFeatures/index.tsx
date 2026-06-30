import type {ReactNode} from 'react';
import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

type FeatureItem = {
  title: string;
  icon: string;
  description: ReactNode;
};

const FeatureList: FeatureItem[] = [
  {
    title: 'Transactional Outbox',
    icon: 'Out',
    description: (
      <>
        <code>enqueue()</code> writes the event row inside your business
        transaction via nestjs-cls, so the event and your writes commit
        atomically. A background claimer relays committed rows to the broker.
      </>
    ),
  },
  {
    title: 'Idempotent Inbox',
    icon: 'In',
    description: (
      <>
        <code>runOnce()</code> dedups redeliveries on a unique
        <code> (source, message_key)</code> row written in the same transaction
        as the side effect — effective exactly-once processing.
      </>
    ),
  },
  {
    title: 'Drizzle-Native Stores',
    icon: 'DB',
    description: (
      <>
        Per-dialect stores for better-sqlite3 (sync) and Postgres (async), with
        table factories you add to your schema and migrate with drizzle-kit. The
        core engine stays dialect-agnostic.
      </>
    ),
  },
  {
    title: 'Pluggable Transport',
    icon: 'Bus',
    description: (
      <>
        The claimer publishes through a dependency-free
        <code> OutboxTransport</code> seam. Ship to Kafka via
        <code> @nest-native/kafka</code>, or stay in-process — the core never
        imports a broker client.
      </>
    ),
  },
  {
    title: 'Zero Runtime Dependencies',
    icon: 'Zero',
    description: (
      <>
        The published package keeps runtime dependencies empty. Nest, Drizzle,
        your driver, and the optional Kafka client stay under the host
        application's control as peer dependencies.
      </>
    ),
  },
  {
    title: '100% Tested',
    icon: 'Test',
    description: (
      <>
        The core engine is covered to 100% across branches, functions, lines,
        and statements — both dialects and the Kafka path, with a gated
        real-broker run proving exactly-once under redelivery.
      </>
    ),
  },
];

function Feature({title, icon, description}: FeatureItem) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center padding-horiz--md feature-card">
        <div className={styles.featureIcon}>{icon}</div>
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures(): ReactNode {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
