const { test, before, after } = require('node:test');
const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const path = require('node:path');
let child, base;
const controllers = [];
before(async () => {
  child = spawn(process.execPath, [path.join(__dirname, '../server.js')], { env: { ...process.env, PORT: '0' }, stdio: ['ignore', 'pipe', 'pipe'] });
  base = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('Relay did not start')), 10000);
    child.once('error', reject);
    child.once('exit', code => { clearTimeout(timer); reject(new Error(`Relay exited (${code}) before startup`)); });
    child.stdout.on('data', b => { const match = String(b).match(/port (\d+)/); if (match) { clearTimeout(timer); resolve('http://127.0.0.1:' + match[1]); } });
  });
});
after(() => { controllers.forEach(c => c.abort()); child?.kill(); });
async function post(route, body, status = 200) {
  const response = await fetch(base + '/api/' + route, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body), signal: AbortSignal.timeout(5000) });
  const data = await response.json();
  assert.equal(response.status, status, JSON.stringify(data));
  return data;
}
async function stream(member) {
  const controller = new AbortController(); controllers.push(controller);
  const response = await fetch(`${base}/api/stream?room=${encodeURIComponent(member.room)}&id=${member.id}`, { signal: controller.signal });
  assert.equal(response.status, 200);
  assert.match(response.headers.get('content-type'), /text\/event-stream/);
  const reader = response.body.getReader();
  const events = [], waiters = [];
  (async () => {
    let buffer = '';
    try {
      for (;;) {
        const { done, value } = await reader.read(); if (done) break;
        buffer += new TextDecoder().decode(value);
        let position;
        while ((position = buffer.indexOf('\n\n')) >= 0) {
          const block = buffer.slice(0, position); buffer = buffer.slice(position + 2);
          const match = block.match(/^event: (.+)\ndata: (.+)$/);
          if (match) {
            const entry = { type: match[1], data: JSON.parse(match[2]) };
            const index = waiters.findIndex(w => w.type === entry.type && w.predicate(entry.data));
            if (index >= 0) { const waiter = waiters.splice(index, 1)[0]; clearTimeout(waiter.timer); waiter.resolve(entry.data); }
            else events.push(entry);
          }
        }
      }
    } catch (error) { if (error.name !== 'AbortError') throw error; }
  })();
  const nextEvent = (type, predicate = () => true, timeout = 5000) => {
    const index = events.findIndex(e => e.type === type && predicate(e.data));
    if (index >= 0) return Promise.resolve(events.splice(index, 1)[0].data);
    return new Promise((resolve, reject) => {
      const waiter = { type, predicate, resolve, timer: setTimeout(() => { waiters.splice(waiters.indexOf(waiter), 1); reject(new Error('Missing event: ' + type)); }, timeout) };
      waiters.push(waiter);
    });
  };
  nextEvent.close = () => controller.abort();
  nextEvent.discard = type => {
    for (let i = events.length - 1; i >= 0; i--) if (events[i].type === type) events.splice(i, 1);
  };
  return nextEvent;
}
test('serves the dark-only, two-step web interface', async () => {
  const response = await fetch(base); assert.equal(response.status, 200);
  const html = await response.text();
  assert.match(html, /name="color-scheme" content="dark"/);
  assert.match(html, /id="details-step"[^>]*hidden/);
  for (let size = 2; size <= 5; size++) assert.match(html, new RegExp(`name="capacity" value="${size}"`));
});
test('rejects invalid room sizes and malformed join data without crashing', async () => {
  for (const capacity of [1, 6, 0, -2, 2.5, '3', null, {}, true]) await post('join', { room: 'bad', name: 'Test', capacity }, 400);
  for (const body of [null, [], 42, { name: '', room: 'a' }, { name: 'A', room: '' }]) await post('join', body, 400);
});
for (let capacity = 2; capacity <= 5; capacity++) {
  test(`${capacity}-person room: departures preserve a conversation until only one remains`, async () => {
    const room = `room-${capacity}`;
    const members = [], streams = [];
    for (let i = 0; i < capacity; i++) {
      const member = await post('join', { room: room.toUpperCase(), name: 'Person ' + i, capacity });
      assert.equal(member.room, room); assert.equal(member.capacity, capacity);
      members.push(member); const events = await stream(member); streams.push(events);
      const hello = await events('hello'); assert.equal(hello.count, i + 1); assert.equal(hello.members.length, i + 1); assert.equal(hello.capacity, capacity);
      if (i === 0) await post('send', { ...member, text: 'Too early' }, 409);
    }
    const roster = await streams[0]('presence', p => p.count === capacity);
    assert.deepEqual(roster.members.map(m => m.name), members.map((_, i) => 'Person ' + i));
    await post('join', { room, name: 'Overflow', capacity }, 409);
    const mismatch = await post('join', { room, name: 'Wrong size', capacity: capacity === 2 ? 3 : 2 }, 409);
    assert.equal(mismatch.capacity, capacity);
    await post('send', { ...members[0], text: '   ' }, 400);
    await post('send', { room, id: 'invalid', text: 'Nope' }, 404);
    // Every participant broadcasts to every other participant with sender identity.
    for (let sender = 0; sender < capacity; sender++) {
      const text = `<hello & world> from ${sender}`;
      await post('send', { ...members[sender], text });
      for (let recipient = 0; recipient < capacity; recipient++) if (recipient !== sender) {
        const msg = await streams[recipient]('msg');
        assert.equal(msg.text, text); assert.equal(msg.name, 'Person ' + sender); assert.equal(msg.id, members[sender].id); assert.equal(typeof msg.t, 'number');
      }
    }
    // Every group size remains usable through all departures down to two people.
    for (let remaining = capacity - 1; remaining >= 2; remaining--) {
      const departing = members[remaining];
      await post('leave', departing);
      assert.equal((await streams[remaining]('ended')).reason, 'left');
      await post('send', { ...departing, text: 'Departed session' }, 404);
      const reducedRoster = await streams[0]('presence', p => p.count === remaining);
      assert.equal(reducedRoster.capacity, capacity);
      assert.deepEqual(reducedRoster.members.map(m => m.id), members.slice(0, remaining).map(m => m.id));
      const text = `Still chatting with ${remaining} people`;
      await post('send', { ...members[0], text });
      for (let recipient = 1; recipient < remaining; recipient++) {
        assert.equal((await streams[recipient]('msg')).text, text);
      }
    }
    // The penultimate departure closes the final participant's room, not earlier.
    await post('leave', members[1]);
    assert.equal((await streams[1]('ended')).reason, 'left');
    assert.equal((await streams[0]('ended')).reason, 'alone');
    for (const member of members) await post('send', { ...member, text: 'after leave' }, 404);
    const replacement = await post('join', { room, name: 'Fresh', capacity: 5 });
    assert.equal(replacement.capacity, 5); await post('leave', replacement);
  });
}
test('legacy clients default to a two-person room', async () => {
  const a = await post('join', { room: 'legacy', name: 'Old A' });
  const b = await post('join', { room: 'legacy', name: 'Old B' });
  assert.equal(a.capacity, 2); await post('join', { room: 'legacy', name: 'Old C' }, 409);
  await post('leave', b);
});
test('a departed group member frees one slot without resetting the remaining sessions', async () => {
  const room = 'replacement-member';
  const members = [], streams = [];
  for (let i = 0; i < 3; i++) {
    const member = await post('join', { room, name: 'Original ' + i, capacity: 3 });
    members.push(member); streams.push(await stream(member));
    await streams[i]('hello');
  }
  await post('leave', members[2]);
  assert.equal((await streams[2]('ended')).reason, 'left');
  await streams[0]('presence', p => p.count === 2);
  const newcomer = await post('join', { room, name: 'Newcomer', capacity: 3 });
  const newcomerEvents = await stream(newcomer);
  assert.equal((await newcomerEvents('hello')).count, 3);
  const roster = await streams[0]('presence', p => p.members.some(m => m.id === newcomer.id));
  assert.deepEqual(roster.members.map(m => m.id), [members[0].id, members[1].id, newcomer.id]);
  await post('send', { ...newcomer, text: 'Joined the ongoing conversation' });
  for (const events of streams.slice(0, 2)) assert.equal((await events('msg')).name, 'Newcomer');
  await post('leave', newcomer);
  await post('leave', members[1]);
  assert.equal((await streams[0]('ended')).reason, 'alone');
});

test('disconnect grace removes only expired members and preserves a reconnecting session', { timeout: 30000 }, async () => {
  async function connectedRoom(room, count) {
    const members = [], streams = [];
    for (let i = 0; i < count; i++) {
      const member = await post('join', { room, name: 'Person ' + i, capacity: 3 });
      members.push(member); streams.push(await stream(member));
      await streams[i]('hello');
    }
    return { members, streams };
  }

  // All scenarios share one real 15-second grace window instead of overriding it.
  const waiting = await connectedRoom('initial-waiting', 1);
  const group = await connectedRoom('expired-group-member', 3);
  const pair = await connectedRoom('expired-pair-member', 2);
  const reconnect = await connectedRoom('reconnecting-member', 3);
  const pending = await connectedRoom('never-opened-stream', 2);
  const pendingMember = await post('join', { room: 'never-opened-stream', name: 'Never connected', capacity: 3 });
  await pending.streams[0]('presence', p => p.count === 3);
  for (const active of [group, pending]) {
    // Drop old two-person snapshots from initial filling before awaiting a departure.
    active.streams[0].discard('presence');
  }

  group.streams[2].close();
  pair.streams[1].close();
  reconnect.streams[2].close();
  const reconnectedEvents = await stream(reconnect.members[2]);
  assert.equal((await reconnectedEvents('hello')).count, 3);

  const [groupPresence, pairEnded, pendingPresence] = await Promise.all([
    group.streams[0]('presence', p => p.count === 2, 20000),
    pair.streams[0]('ended', () => true, 20000),
    pending.streams[0]('presence', p => p.count === 2, 20000)
  ]);
  assert.deepEqual(groupPresence.members.map(m => m.id), group.members.slice(0, 2).map(m => m.id));
  assert.equal(pairEnded.reason, 'alone');
  assert.deepEqual(pendingPresence.members.map(m => m.id), pending.members.map(m => m.id));
  await post('send', { ...group.members[2], text: 'Expired' }, 404);
  await post('send', { ...pendingMember, text: 'Expired before streaming' }, 404);
  await post('send', { ...pair.members[0], text: 'Closed pair' }, 404);

  await post('send', { ...group.members[0], text: 'Two still talking' });
  assert.equal((await group.streams[1]('msg')).text, 'Two still talking');
  await post('send', { ...reconnect.members[2], text: 'My original session still works' });
  assert.equal((await reconnect.streams[0]('msg')).text, 'My original session still works');
  await post('join', { room: 'reconnecting-member', name: 'No extra slot', capacity: 3 }, 409);

  // A brand-new creator remains in the waiting room while its stream is connected.
  await post('send', { ...waiting.members[0], text: 'Still waiting for a guest' }, 409);
  const guest = await post('join', { room: 'initial-waiting', name: 'Guest', capacity: 3 });
  await waiting.streams[0]('presence', p => p.count === 2);
  await post('leave', guest);
  assert.equal((await waiting.streams[0]('ended')).reason, 'alone');

  await post('leave', group.members[1]);
  await post('leave', pending.members[1]);
  await post('leave', reconnect.members[2]);
  await post('leave', reconnect.members[1]);
});