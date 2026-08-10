import assert from 'node:assert/strict';
import test from 'node:test';

import { partitionCommandRunway } from '../src/app/command/commandViewModel.ts';

const NOW = Date.parse('2026-07-26T06:30:00.000Z');

function event(id, queueState, at, extra = {}) {
  return {
    id,
    source: 'checkpoint_jobs',
    kind: 'd3',
    status: queueState === 'in_progress' ? 'running' : 'pending',
    isOpen: true,
    queueState,
    claimableAt: at,
    happenedAt: at,
    ...extra,
  };
}

test('open work partitions exactly once across the four runway lanes', () => {
  const source = [
    event('overdue', 'overdue', '2026-07-26T05:30:00.000Z'),
    event('running', 'in_progress', '2026-07-26T05:45:00.000Z'),
    event('due', 'scheduled', '2026-07-26T12:30:00.000Z'),
    event('later', 'scheduled', '2026-07-27T06:30:00.001Z'),
    event('queued', 'queued', null),
    event('done', null, null, { status: 'done', isOpen: false }),
  ];
  const runway = partitionCommandRunway(source, NOW);
  const ids = [
    ...runway.overdue,
    ...runway.running,
    ...runway.dueNext,
    ...runway.later,
  ].map((item) => item.id);

  assert.deepEqual(new Set(ids), new Set(['overdue', 'running', 'due', 'later', 'queued']));
  assert.equal(ids.length, runway.allOpen.length);
  assert.equal(runway.running[0].status, 'running');
  assert.equal(runway.dueWithinTwelveHours.length, 1);
});
test('the exact twelve-hour boundary remains due next', () => {
  const runway = partitionCommandRunway([
    event('edge', 'scheduled', '2026-07-26T18:30:00.000Z'),
    event('after', 'scheduled', '2026-07-26T18:30:00.001Z'),
  ], NOW);

  assert.deepEqual(runway.dueNext.map((item) => item.id), ['edge']);
  assert.deepEqual(runway.later.map((item) => item.id), ['after']);
});

test('running work never becomes overdue in the client', () => {
  const runway = partitionCommandRunway([
    event('running-in-the-past', 'in_progress', '2026-07-25T06:30:00.000Z'),
  ], NOW);

  assert.equal(runway.overdue.length, 0);
  assert.deepEqual(runway.running.map((item) => item.id), ['running-in-the-past']);
});

test('runway membership is de-duplicated by event id', () => {
  const duplicate = event('same', 'queued', null);
  const runway = partitionCommandRunway([duplicate, { ...duplicate }], NOW);

  assert.equal(runway.allOpen.length, 1);
  assert.equal(runway.dueNext.length, 1);
});
