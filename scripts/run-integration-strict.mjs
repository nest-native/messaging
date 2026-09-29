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
 * The check reads a TAP copy of the run, never the spec reporter's text. The
 * summary counts skipped tests, not skipped suites, so a run in which a whole
 * `describe(..., { skip })` block never ran still reports `skipped 0`. And the
 * spec reporter prints a skip's reason in place of the word SKIP, so
 * `{ skip: 'no broker' }` or `t.skip('no broker')` would get past a search of
 * its output. TAP marks every skipped or todo test and suite with a `# SKIP`
 * or `# TODO` directive, reason or not, and escapes a `#` in a test's name, so
 * a name cannot fake one either.
 */
import { spawn } from 'node:child_process';
import { mkdtempSync, readdirSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';

// A TAP test point whose directive (the first unescaped `#`) is SKIP or TODO.
const SKIP_OR_TODO = /^\s*(?:not )?ok \d+\b.*?(?<!\\)#\s*(?:SKIP|TODO)\b/i;

const specDir = path.join('packages', 'messaging', 'test', 'integration');
const specs = readdirSync(specDir)
  .filter(file => file.endsWith('.spec.ts'))
  .sort()
  .map(file => path.join(specDir, file));

if (specs.length === 0) {
  fail(`No gated specs found under ${specDir}.`);
}

const reportDir = mkdtempSync(path.join(tmpdir(), 'messaging-gated-'));
const tapFile = path.join(reportDir, 'report.tap');

const child = spawn(
  process.execPath,
  [
    '--require',
    'ts-node/register',
    '--require',
    'reflect-metadata',
    '--test',
    // The spec reporter writes the log; the TAP copy is what gets checked.
    '--test-reporter=spec',
    '--test-reporter-destination=stdout',
    '--test-reporter=tap',
    `--test-reporter-destination=${tapFile}`,
    ...specs,
  ],
  {
    env: { ...process.env, TS_NODE_PROJECT: 'tsconfig.spec.json' },
    stdio: 'inherit',
  },
);

child.on('close', code => {
  const tap = readReport();
  if (code !== 0) {
    process.exit(code ?? 1);
  }
  const tests = summaryCount(tap, 'tests');
  if (tests === undefined) {
    fail('Could not read the summary of the TAP report.');
  }
  if (tests === 0) {
    fail('The gated specs ran zero tests.');
  }
  const markers = tap
    .split('\n')
    .filter(line => SKIP_OR_TODO.test(line))
    .map(line => line.trim());
  const counted = (summaryCount(tap, 'skipped') ?? 0) + (summaryCount(tap, 'todo') ?? 0);
  if (markers.length > 0 || counted > 0) {
    fail(
      `Gated specs or suites did not run (${counted} skipped or todo tests, ` +
        `${markers.length} marked) — every backend URL must be set in this ` +
        `job:\n  ${markers.join('\n  ')}`,
    );
  }
  console.log(`\nAll ${tests} gated specs ran against real backends; none skipped.`);
});

function readReport() {
  try {
    return readFileSync(tapFile, 'utf8');
  } catch {
    return '';
  } finally {
    rmSync(reportDir, { recursive: true, force: true });
  }
}

function summaryCount(tap, name) {
  const match = new RegExp(`^# ${name} (\\d+)$`, 'm').exec(tap);
  return match ? Number(match[1]) : undefined;
}

function fail(message) {
  console.error(`\n${message}`);
  process.exit(1);
}
