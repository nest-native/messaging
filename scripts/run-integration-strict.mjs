#!/usr/bin/env node
/**
 * Run the gated real-backend specs and fail unless every one of them ran.
 *
 * The specs under packages/messaging/test/integration skip themselves when
 * their backend's URL is unset, so a fork or a laptop without Docker stays
 * green. In CI that same skip turns a broken wiring into a pass: a job whose
 * MESSAGING_*_URL never reached the process reports every suite as skipped,
 * and the run is green while proving nothing. CI therefore runs the specs
 * through this script, which requires a non-empty run with no skip at all.
 *
 * Two traps shape the check. Node's test runner prints `ℹ skipped 0` in its
 * summary even when a whole `describe(..., { skip })` block was skipped — a
 * skipped suite is not a skipped test — so the summary alone would pass a run
 * in which every backend was missing. And the default reporter is TAP when
 * stdout is not a terminal, which a CI runner never is. The script pins the
 * spec reporter and fails on any `# SKIP` or `# TODO` marker, suites included.
 */
import { spawn } from 'node:child_process';
import { readdirSync } from 'node:fs';
import path from 'node:path';

const specDir = path.join('packages', 'messaging', 'test', 'integration');
const specs = readdirSync(specDir)
  .filter(file => file.endsWith('.spec.ts'))
  .sort()
  .map(file => path.join(specDir, file));

if (specs.length === 0) {
  fail(`No gated specs found under ${specDir}.`);
}

const child = spawn(
  process.execPath,
  [
    '--require',
    'ts-node/register',
    '--require',
    'reflect-metadata',
    '--test',
    '--test-reporter=spec',
    ...specs,
  ],
  {
    env: { ...process.env, TS_NODE_PROJECT: 'tsconfig.spec.json' },
    stdio: ['ignore', 'pipe', 'inherit'],
  },
);

let output = '';
child.stdout.on('data', chunk => {
  process.stdout.write(chunk);
  output += chunk;
});

child.on('close', code => {
  if (code !== 0) {
    process.exit(code ?? 1);
  }
  const tests = summaryCount('tests');
  if (tests === undefined) {
    fail('Could not read the test summary from the spec reporter output.');
  }
  if (tests === 0) {
    fail('The gated specs ran zero tests.');
  }
  const markers = output
    .split('\n')
    .filter(line => /#\s*(SKIP|TODO)\b/.test(line))
    .map(line => line.trim());
  if (markers.length > 0) {
    fail(
      `${markers.length} gated spec(s) or suite(s) did not run — every ` +
        `backend URL must be set in this job:\n  ${markers.join('\n  ')}`,
    );
  }
  console.log(
    `\nAll ${tests} gated specs ran against real backends; none skipped.`,
  );
});

function summaryCount(name) {
  const match = new RegExp(`^ℹ ${name} (\\d+)$`, 'm').exec(output);
  return match ? Number(match[1]) : undefined;
}

function fail(message) {
  console.error(`\n${message}`);
  process.exit(1);
}
