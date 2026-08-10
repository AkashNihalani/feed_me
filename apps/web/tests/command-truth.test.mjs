import assert from 'node:assert/strict';
import test from 'node:test';

import {
  checkpointClaimableAt,
  currentOperationalStates,
  queueTiming,
} from '../src/app/api/command/commandTruth.ts';

test('first-attempt checkpoints become claimable in the discovery slot after their due time', () => {
  assert.equal(
    checkpointClaimableAt('2026-07-17T05:00:00.000Z', 0), // 10:30 IST
    '2026-07-17T06:30:00.000Z', // 12:00 IST
  );
  assert.equal(
    checkpointClaimableAt('2026-07-17T13:30:00.000Z', 0), // 19:00 IST
    '2026-07-17T18:30:00.000Z', // next midnight IST
  );
  assert.equal(
    checkpointClaimableAt('2026-07-17T19:00:00.000Z', 0), // 00:30 IST
    '2026-07-18T06:30:00.000Z', // same IST day noon
  );
});

test('checkpoint retries are claimable at next_run_at', () => {
  assert.equal(
    checkpointClaimableAt('2026-07-17T05:00:00.000Z', 2),
    '2026-07-17T05:00:00.000Z',
  );
});

test('only open work can be scheduled or overdue', () => {
  const now = Date.parse('2026-07-17T10:00:00.000Z');
  assert.deepEqual(
    queueTiming('pending', { claimableAt: '2026-07-17T11:00:00.000Z' }, now),
    {
      isOpen: true,
      overdue: false,
      queueState: 'scheduled',
      actionableAt: '2026-07-17T11:00:00.000Z',
    },
  );
  assert.equal(
    queueTiming('retry', { claimableAt: '2026-07-17T09:00:00.000Z' }, now).queueState,
    'overdue',
  );
  assert.equal(
    queueTiming('done', { claimableAt: '2026-07-17T11:00:00.000Z' }, now).queueState,
    null,
  );
});

test('a newer success supersedes an older failure in the same lane', () => {
  const base = {
    source: 'run_jobs',
    kind: 'daily',
    feedId: 1,
    feederId: 10,
    postKey: null,
    nextRunAt: null,
    scheduledAt: null,
    dueAt: null,
    claimableAt: null,
    isOperational: true,
    isOpen: false,
    overdue: false,
    queueState: null,
  };
  const states = currentOperationalStates([
    { ...base, id: 'old', status: 'failed', happenedAt: '2026-07-16T10:00:00.000Z' },
    { ...base, id: 'new', status: 'done', happenedAt: '2026-07-17T10:00:00.000Z' },
  ]);
  assert.equal(states.length, 1);
  assert.equal(states[0].id, 'new');
  assert.equal(states[0].status, 'done');
});
