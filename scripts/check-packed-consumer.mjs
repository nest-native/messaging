import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const npmExecutable = process.platform === 'win32' ? 'npm.cmd' : 'npm';
const nodeExecutable = process.execPath;
const repoRoot = process.cwd();
const tempRoot = fs.mkdtempSync(
  path.join(os.tmpdir(), 'nest-native-messaging-consumer-'),
);
const consumerRoot = path.join(tempRoot, 'consumer');
const npmCache = path.join(tempRoot, 'npm-cache');

try {
  fs.mkdirSync(consumerRoot);

  const tarballPath = packTarball();
  writeConsumerPackage(tarballPath);
  writeConsumerSmoke();

  execFileSync(
    npmExecutable,
    [
      'install',
      '--package-lock=false',
      '--no-audit',
      '--fund=false',
      '--ignore-scripts',
    ],
    {
      cwd: consumerRoot,
      stdio: 'inherit',
      env: {
        ...process.env,
        npm_config_cache: npmCache,
      },
    },
  );
  execFileSync(nodeExecutable, ['smoke.cjs'], {
    cwd: consumerRoot,
    stdio: 'inherit',
  });

  console.log('Packed consumer validation OK.');
} finally {
  fs.rmSync(tempRoot, { recursive: true, force: true });
}

function packTarball() {
  const rawOutput = execFileSync(
    npmExecutable,
    [
      'pack',
      '--json',
      '--workspace',
      '@nest-native/messaging',
      '--pack-destination',
      tempRoot,
    ],
    {
      cwd: repoRoot,
      encoding: 'utf8',
      env: {
        ...process.env,
        npm_config_cache: npmCache,
      },
    },
  );
  const [packResult] = JSON.parse(rawOutput);

  if (!packResult?.filename) {
    throw new Error('npm pack did not produce a tarball filename.');
  }

  return path.join(tempRoot, packResult.filename);
}

function writeConsumerPackage(tarballPath) {
  const rootPackage = JSON.parse(
    fs.readFileSync(path.join(repoRoot, 'package.json'), 'utf8'),
  );
  const devDependencies = rootPackage.devDependencies ?? {};
  const dependencies = {
    '@nestjs/common': devDependencies['@nestjs/common'],
    '@nestjs/core': devDependencies['@nestjs/core'],
    '@nestjs-cls/transactional': devDependencies['@nestjs-cls/transactional'],
    'drizzle-orm': devDependencies['drizzle-orm'],
    '@nest-native/messaging': `file:${tarballPath}`,
    'reflect-metadata': devDependencies['reflect-metadata'],
    rxjs: devDependencies.rxjs,
  };
  const missingDependencies = Object.entries(dependencies)
    .filter(([, version]) => !version)
    .map(([name]) => name);

  if (missingDependencies.length > 0) {
    throw new Error(
      `Consumer smoke is missing dependency versions: ${missingDependencies.join(', ')}`,
    );
  }

  fs.writeFileSync(
    path.join(consumerRoot, 'package.json'),
    `${JSON.stringify(
      {
        name: 'nest-native-messaging-packed-consumer',
        private: true,
        type: 'commonjs',
        dependencies,
      },
      null,
      2,
    )}\n`,
  );
}

function writeConsumerSmoke() {
  fs.writeFileSync(
    path.join(consumerRoot, 'smoke.cjs'),
    `'use strict';

require('reflect-metadata');

const assert = require('node:assert/strict');
const core = require('@nest-native/messaging');
const sqlite = require('@nest-native/messaging/sqlite');
const postgres = require('@nest-native/messaging/postgres');
const testing = require('@nest-native/messaging/testing');
const packageJson = require('@nest-native/messaging/package.json');

// Every public entry point resolves from the packed tarball and exports its
// documented surface.
for (const name of [
  'MessagingModule', 'OutboxProducer', 'OutboxClaimer', 'InboxService',
  'runWorkerLoop', 'RetryableError', 'PermanentError', 'OUTBOX_TRANSPORT',
  'deriveDedupKey', 'encodeWireValue', 'decodeWireValue',
]) {
  assert.ok(name in core, 'missing core export: ' + name);
}
for (const name of ['SqliteOutboxStore', 'SqliteInboxStore', 'outboxEvents', 'inboxEvents']) {
  assert.ok(name in sqlite, 'missing sqlite export: ' + name);
}
for (const name of ['PostgresOutboxStore', 'PostgresInboxStore', 'outboxEvents', 'inboxEvents']) {
  assert.ok(name in postgres, 'missing postgres export: ' + name);
}
assert.ok('InMemoryOutboxTransport' in testing, 'missing testing export');
assert.ok(packageJson.exports['./kafka'], 'missing ./kafka subpath export');

// The published package declares zero runtime dependencies (consumers only pull
// the peers they actually use).
assert.equal(
  Object.keys(packageJson.dependencies ?? {}).length,
  0,
  'The packed package must not declare runtime dependencies.',
);

// Functional smoke: the in-memory transport records a publish (no broker, no DB).
(async () => {
  const transport = new testing.InMemoryOutboxTransport();
  await transport.publish({ id: 'e1', topic: 'demo', payload: { ok: true } });
  assert.equal(transport.list().length, 1);
  assert.equal(transport.list()[0].topic, 'demo');
  assert.ok(new core.RetryableError('x', 100).delayMs === 100);
})().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
`,
  );
}
