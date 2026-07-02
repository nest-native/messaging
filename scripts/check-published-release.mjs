#!/usr/bin/env node

import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const npmExecutable = process.platform === 'win32' ? 'npm.cmd' : 'npm';
const nodeExecutable = process.execPath;
const repoRoot = process.cwd();
const packagePath = path.join(repoRoot, 'packages/messaging/package.json');
const packageJson = JSON.parse(fs.readFileSync(packagePath, 'utf8'));
const packageName = packageJson.name;
const version = process.argv[2] ?? packageJson.version;
const tempRoot = fs.mkdtempSync(
  path.join(os.tmpdir(), 'nest-native-messaging-published-release-'),
);
const npmCache = path.join(tempRoot, 'npm-cache');

try {
  verifyRegistryVersion();
  packPublishedArtifact();
  runPublishedConsumerSmoke();
  runPublishedSampleSmoke();

  console.log(`Published release validation OK for ${packageName}@${version}.`);
} finally {
  fs.rmSync(tempRoot, { recursive: true, force: true });
}

function verifyRegistryVersion() {
  const rawOutput = execFileSync(
    npmExecutable,
    ['view', `${packageName}@${version}`, 'version', 'dist-tags.latest', 'dist.tarball', '--json'],
    {
      cwd: repoRoot,
      encoding: 'utf8',
      env: npmEnv(),
    },
  );
  const registryInfo = JSON.parse(rawOutput);

  if (registryInfo.version !== version) {
    throw new Error(
      `Expected npm version ${version}, but registry returned ${registryInfo.version}.`,
    );
  }

  if (registryInfo['dist-tags.latest'] !== version) {
    throw new Error(
      `Expected npm latest tag ${version}, but registry returned ${registryInfo['dist-tags.latest']}.`,
    );
  }

  if (!registryInfo['dist.tarball']) {
    throw new Error('Registry response is missing the published tarball URL.');
  }

  console.log(`Registry validation OK: ${packageName}@${version} is latest.`);
}

function packPublishedArtifact() {
  const rawOutput = execFileSync(
    npmExecutable,
    ['pack', `${packageName}@${version}`, '--json', '--pack-destination', tempRoot],
    {
      cwd: repoRoot,
      encoding: 'utf8',
      env: npmEnv(),
    },
  );
  const [packResult] = JSON.parse(rawOutput);

  if (!packResult?.filename || !Array.isArray(packResult.files)) {
    throw new Error('npm pack did not return the expected published artifact payload.');
  }

  const filePaths = packResult.files.map(file => file.path);
  for (const requiredFile of ['README.md', 'package.json', 'dist/index.js', 'dist/index.d.ts']) {
    if (!filePaths.includes(requiredFile)) {
      throw new Error(`Published artifact is missing ${requiredFile}.`);
    }
  }

  console.log(
    `Published artifact validation OK: ${packResult.filename} contains ${packResult.entryCount} files.`,
  );
}

function runPublishedConsumerSmoke() {
  const consumerRoot = path.join(tempRoot, 'consumer');
  fs.mkdirSync(consumerRoot);
  writeConsumerPackage(consumerRoot);
  writeConsumerSmoke(consumerRoot);

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
      env: npmEnv(),
    },
  );
  execFileSync(nodeExecutable, ['smoke.cjs'], {
    cwd: consumerRoot,
    stdio: 'inherit',
  });

  console.log('Published consumer validation OK.');
}

function writeConsumerPackage(consumerRoot) {
  const rootPackage = JSON.parse(
    fs.readFileSync(path.join(repoRoot, 'package.json'), 'utf8'),
  );
  const devDependencies = rootPackage.devDependencies ?? {};
  const dependencies = {
    '@nestjs/common': devDependencies['@nestjs/common'],
    '@nestjs/core': devDependencies['@nestjs/core'],
    '@nestjs-cls/transactional': devDependencies['@nestjs-cls/transactional'],
    'drizzle-orm': devDependencies['drizzle-orm'],
    [packageName]: version,
    'reflect-metadata': devDependencies['reflect-metadata'],
    rxjs: devDependencies.rxjs,
  };
  const missingDependencies = Object.entries(dependencies)
    .filter(([, dependencyVersion]) => !dependencyVersion)
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
        name: 'nest-native-messaging-published-consumer',
        private: true,
        type: 'commonjs',
        dependencies,
      },
      null,
      2,
    )}\n`,
  );
}

function writeConsumerSmoke(consumerRoot) {
  fs.writeFileSync(
    path.join(consumerRoot, 'smoke.cjs'),
    `'use strict';

require('reflect-metadata');

const assert = require('node:assert/strict');
const core = require('${packageName}');
const inProcess = require('${packageName}/in-process');
const sqlite = require('${packageName}/sqlite');
const postgres = require('${packageName}/postgres');
const mysql = require('${packageName}/mysql');
const testing = require('${packageName}/testing');
const packageJson = require('${packageName}/package.json');

// Every public entry point resolves from the registry install and exports its
// documented surface.
for (const name of [
  'MessagingModule', 'OutboxProducer', 'OutboxClaimer', 'InboxService',
  'runWorkerLoop', 'RetryableError', 'PermanentError', 'OUTBOX_TRANSPORT',
  'deriveDedupKey', 'encodeWireValue', 'decodeWireValue',
]) {
  assert.ok(name in core, 'missing core export: ' + name);
}
for (const name of ['OutboxRegistry', 'InProcessOutboxTransport']) {
  assert.ok(name in inProcess, 'missing in-process export: ' + name);
}
for (const name of ['SqliteOutboxStore', 'SqliteInboxStore', 'outboxEvents', 'inboxEvents']) {
  assert.ok(name in sqlite, 'missing sqlite export: ' + name);
}
for (const name of ['PostgresOutboxStore', 'PostgresInboxStore', 'outboxEvents', 'inboxEvents']) {
  assert.ok(name in postgres, 'missing postgres export: ' + name);
}
for (const name of ['MysqlOutboxStore', 'MysqlInboxStore', 'outboxEvents', 'inboxEvents']) {
  assert.ok(name in mysql, 'missing mysql export: ' + name);
}
assert.ok('InMemoryOutboxTransport' in testing, 'missing testing export');
assert.ok(packageJson.exports['./kafka'], 'missing ./kafka subpath export');

// The published package declares zero runtime dependencies (consumers only pull
// the peers they actually use).
assert.equal(
  Object.keys(packageJson.dependencies ?? {}).length,
  0,
  'The published package must not declare runtime dependencies.',
);

// Functional smoke: the in-memory transport records a publish, and the
// in-process transport dispatches to a registered handler (no broker, no DB).
(async () => {
  const transport = new testing.InMemoryOutboxTransport();
  await transport.publish({ id: 'e1', topic: 'demo', payload: { ok: true } });
  assert.equal(transport.list().length, 1);
  assert.equal(transport.list()[0].topic, 'demo');
  assert.ok(new core.RetryableError('x', 100).delayMs === 100);

  const registry = new inProcess.OutboxRegistry();
  let handled;
  registry.register('demo', payload => {
    handled = payload;
    return 'completed';
  });
  await new inProcess.InProcessOutboxTransport(registry).publish({
    id: 'e2', topic: 'demo', payload: { ok: 1 },
  });
  assert.equal(handled.ok, 1);
  await assert.rejects(
    () => new inProcess.InProcessOutboxTransport(registry).publish({
      id: 'e3', topic: 'unrouted', payload: {},
    }),
    core.PermanentError,
  );
})().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
`,
  );
}

function runPublishedSampleSmoke() {
  const sampleWorkspace = path.join(tempRoot, 'samples');
  fs.mkdirSync(sampleWorkspace);

  writeSampleWorkspacePackage(sampleWorkspace);
  fs.cpSync(path.join(repoRoot, 'sample'), path.join(sampleWorkspace, 'sample'), {
    recursive: true,
  });
  const pinnedSamples = pinSampleDependencies(
    path.join(sampleWorkspace, 'sample'),
  );
  fs.cpSync(path.join(repoRoot, 'scripts'), path.join(sampleWorkspace, 'scripts'), {
    recursive: true,
    filter: source => !source.includes(`${path.sep}node_modules${path.sep}`),
  });
  fs.copyFileSync(
    path.join(repoRoot, 'tsconfig.base.json'),
    path.join(sampleWorkspace, 'tsconfig.base.json'),
  );

  installPublishedSampleWorkspace(sampleWorkspace);

  const resolutionOutput = execFileSync(
    npmExecutable,
    ['ls', packageName, '--workspaces', '--depth=0'],
    {
      cwd: sampleWorkspace,
      encoding: 'utf8',
      env: npmEnv(),
    },
  );

  const localPackageLinkPattern = new RegExp(
    `${escapeRegExp(packageName)}@[^\\s]+\\s+->`,
  );
  const requestedVersionPattern = new RegExp(
    `${escapeRegExp(packageName)}@${escapeRegExp(version)}\\b`,
  );

  if (localPackageLinkPattern.test(resolutionOutput)) {
    throw new Error(
      `Sample workspace resolved ${packageName} through a local workspace link:\n${resolutionOutput}`,
    );
  }

  if (!requestedVersionPattern.test(resolutionOutput)) {
    throw new Error(
      `Sample workspace did not resolve ${packageName}@${version}:\n${resolutionOutput}`,
    );
  }

  execFileSync(npmExecutable, ['run', 'ci:sample'], {
    cwd: sampleWorkspace,
    stdio: 'inherit',
    env: npmEnv(),
  });

  console.log(
    `Published sample validation OK: pinned ${pinnedSamples} sample workspaces to ${packageName}@${version}.`,
  );
}

function pinSampleDependencies(sampleRoot) {
  const packageFiles = fs
    .readdirSync(sampleRoot, { withFileTypes: true })
    .filter(entry => entry.isDirectory())
    .map(entry => path.join(sampleRoot, entry.name, 'package.json'))
    .filter(packageFile => fs.existsSync(packageFile));
  let pinnedSamples = 0;

  for (const packageFile of packageFiles) {
    const packageJson = JSON.parse(fs.readFileSync(packageFile, 'utf8'));
    const pinned = pinPackageDependency(packageJson);

    if (pinned) {
      pinnedSamples += 1;
      fs.writeFileSync(packageFile, `${JSON.stringify(packageJson, null, 2)}\n`);
    }
  }

  if (pinnedSamples === 0) {
    throw new Error(
      `No sample package.json files declare ${packageName}; post-publish sample validation cannot prove registry consumption.`,
    );
  }

  return pinnedSamples;
}

function pinPackageDependency(packageJson) {
  let pinned = false;

  for (const field of [
    'dependencies',
    'devDependencies',
    'peerDependencies',
    'optionalDependencies',
  ]) {
    if (packageJson[field]?.[packageName]) {
      packageJson[field][packageName] = version;
      pinned = true;
    }
  }

  return pinned;
}

function installPublishedSampleWorkspace(sampleWorkspace) {
  try {
    execFileSync(npmExecutable, ['install', '--package-lock=false'], {
      cwd: sampleWorkspace,
      stdio: 'inherit',
      env: npmEnv(),
    });
  } catch (error) {
    throw new Error(
      [
        `Sample installation failed after pinning samples to ${packageName}@${version}.`,
        'This usually means the current sample workspace expects package peer dependency metadata that differs from the published version being checked.',
        'Run this check from the matching release commit, or after publishing the version that matches the current repository state.',
      ].join(' '),
      { cause: error },
    );
  }
}

function writeSampleWorkspacePackage(sampleWorkspace) {
  fs.writeFileSync(
    path.join(sampleWorkspace, 'package.json'),
    `${JSON.stringify(
      {
        name: 'nest-native-messaging-published-samples',
        private: true,
        workspaces: ['sample/*'],
        scripts: {
          showcase:
            'node scripts/run-optional-workspace.cjs nest-native-messaging-showcase test',
          'sample:focused': 'node scripts/run-focused-samples.mjs',
          sample: 'npm run showcase && npm run sample:focused',
          'ci:sample': 'npm run sample',
        },
      },
      null,
      2,
    )}\n`,
  );
}

function npmEnv() {
  return {
    ...process.env,
    npm_config_cache: npmCache,
  };
}

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}
