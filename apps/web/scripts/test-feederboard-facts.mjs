import assert from 'node:assert/strict';
import feederboardFacts from '../src/lib/feederboardFacts.js';

const { buildFeederboardFacts, buildPostMortemDeck, dayKey, dayPart } = feederboardFacts;
const post = (id, overrides = {}) => ({
  postKey: id,
  thumbnailUrl: `https://example.test/${id}.jpg`,
  handle: 'creator',
  postedAt: '2026-07-10T14:00:00.000Z',
  mediaType: 'reel',
  firstCheckpoint: 'D1',
  latestCheckpoint: 'D7',
  firstPercentile: 38,
  latestPercentile: 16,
  engagementRateMultiple: 1.4,
  rankingMultiple: 1.4,
  ...overrides,
});

assert.equal(dayKey(post('ist-a', { postedAt: '2026-07-10T18:45:00.000Z' })), '2026-Jul-11');
assert.equal(dayPart(post('ist-b', { postedAt: '2026-07-10T18:45:00.000Z' })), 'night');
assert.equal(dayPart(post('ist-c', { postedAt: '2026-07-10T01:15:00.000Z' })), 'morning');

const facts = buildFeederboardFacts([
  post('b', { handle: 'zulu', firstPercentile: 42, latestPercentile: 17, postedAt: '2026-07-10T14:00:00.000Z' }),
  post('a', { handle: 'alpha', firstPercentile: 42, latestPercentile: 17, postedAt: '2026-07-10T14:05:00.000Z' }),
  post('c', { latestPercentile: 20, postedAt: '2026-07-10T14:10:00.000Z', mediaType: 'carousel' }),
], 30);
const climb = facts.find((fact) => fact.kind === 'checkpoint-climb');
assert.ok(climb);
assert.match(climb.headline, /@alpha/);

const streakFacts = buildFeederboardFacts([
  post('run-1', { firstPercentile: null, latestPercentile: 24, postedAt: '2026-07-08T14:00:00.000Z' }),
  post('run-2', { firstPercentile: null, latestPercentile: 20, postedAt: '2026-07-09T14:00:00.000Z' }),
  post('run-3', { firstPercentile: null, latestPercentile: 16, postedAt: '2026-07-10T14:00:00.000Z' }),
], 30);
assert.equal(streakFacts.some((fact) => fact.kind === 'above-streak'), true);

const postMortemDeck = buildPostMortemDeck([
  post('pm-1', { firstPercentile: null, latestPercentile: 24, postedAt: '2026-07-08T14:00:00.000Z' }),
  post('pm-2', { firstPercentile: null, latestPercentile: 20, postedAt: '2026-07-09T14:00:00.000Z' }),
  post('pm-3', { firstPercentile: null, latestPercentile: 16, postedAt: '2026-07-10T14:00:00.000Z' }),
], 30);
assert.ok(postMortemDeck.length > 0);
assert.equal(postMortemDeck[0].trigger, 'above-streak');
assert.match(postMortemDeck[0].statement, /@creator is 3 posts into an above-usual run\./);
assert.equal(postMortemDeck[0].supportingPostKeys.length, 3);

const sparse = buildFeederboardFacts([post('sparse', { firstPercentile: 18, latestPercentile: 17, rankingMultiple: 1.05 })], 30);
assert.equal(sparse.length, 0);

console.log('Feederboard fact tests passed');
