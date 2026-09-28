#!/usr/bin/env node
/**
 * Print the CI step summary for the sample matrix: the validation duration
 * and one row per sample folder, described by its own package.json.
 *
 * The table is read from `sample/` rather than written by hand. A hardcoded
 * table was copied here from another repository once and went on naming
 * samples this repository never had; generating it means a sample that is
 * added, renamed or removed can never leave the summary behind.
 *
 * Usage: node scripts/write-sample-summary.mjs <duration-ms>
 */
import fs from 'node:fs';
import path from 'node:path';

const durationMs = Number(process.argv[2]);
const sampleRoot = path.join(process.cwd(), 'sample');

const rows = fs
  .readdirSync(sampleRoot, { withFileTypes: true })
  .filter(entry => entry.isDirectory())
  .map(entry => entry.name)
  .sort()
  .map(folder => {
    const manifest = path.join(sampleRoot, folder, 'package.json');
    if (!fs.existsSync(manifest)) {
      return undefined;
    }
    const { description = '' } = JSON.parse(fs.readFileSync(manifest, 'utf8'));
    return `| \`${folder}\` | ${escapeCell(description)} |`;
  })
  .filter(Boolean);

const duration = Number.isFinite(durationMs)
  ? `${(durationMs / 1000).toFixed(2)}s (${durationMs}ms)`
  : 'unknown';

console.log(
  [
    '### Sample Matrix',
    '',
    '| Metric | Value |',
    '| :--- | ---: |',
    `| Sample validation duration | ${duration} |`,
    '',
    '| Sample | What it runs |',
    '| :--- | :--- |',
    ...rows,
    '',
  ].join('\n'),
);

function escapeCell(text) {
  return text.replace(/\|/g, '\\|').replace(/\s+/g, ' ').trim();
}
