// Chatverse relay. Developed by solo developer Prabhat.
// Everything lives in RAM only: no database, no files, no message logging.
// Run: node server.js   (optional env PORT, default 3000)
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');

const GRACE = 15000;          // ms a dropped member may reconnect before being removed
const rooms = new Map();      // code -> Map of members, with a fixed capacity chosen by the creator

const emit = (m, ev, data) => m.res && m.res.write(`event: ${ev}\ndata: ${JSON.stringify(data)}\n\n`);
const presence = room => ({ capacity: room.capacity, count: room.size, members: [...room].map(([id, m]) => ({ id, name: m.name })) });
const broadcastPresence = room => room.forEach(m => emit(m, 'presence', presence(room)));
const clean = (s, n) => String(s || '').replace(/[\u0000-\u001f]/g, '').trim().slice(0, n);

function endRoom(code, reason) {            // wipes the room for everyone
  const room = rooms.get(code);
  if (!room) return;
  rooms.delete(code);
  for (const m of room.values()) {
    clearTimeout(m.timer);
    emit(m, 'ended', { reason });
    const response = m.res;
    m.res = null;
    if (response) response.end();
  }
}

function removeMember(code, id, reason) {
  const room = rooms.get(code), member = room && room.get(id);
  if (!member) return;
  clearTimeout(member.timer);
  room.delete(id);
  emit(member, 'ended', { reason });
  const response = member.res;
  member.res = null;
  if (response) response.end();

  if (room.size === 0) {
    rooms.delete(code);
  } else if (room.started && room.size < 2) {
    // A new room may wait for its first guest. An existing conversation needs two.
    endRoom(code, 'alone');
  } else {
    broadcastPresence(room);
  }
}

const readJson = req => new Promise(done => {
  let b = '';
  req.on('data', c => { b += c; if (b.length > 8000) req.destroy(); });
  req.on('end', () => { try { const data = JSON.parse(b || '{}'); done(data && typeof data === 'object' && !Array.isArray(data) ? data : {}); } catch { done({}); } });
});

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  const json = (status, o) => { res.writeHead(status, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' }); res.end(JSON.stringify(o)); };

  if (req.method === 'GET' && url.pathname === '/') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
    return fs.createReadStream(path.join(__dirname, 'index.html')).pipe(res);
  }

  if (req.method === 'GET' && url.pathname === '/api/stream') {
    const code = url.searchParams.get('room'), id = url.searchParams.get('id');
    const room = rooms.get(code), me = room && room.get(id);
    if (!me) return json(404, { error: 'Session not found' });
    res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-store', 'X-Accel-Buffering': 'no' });
    clearTimeout(me.timer);
    const previous = me.res;
    me.res = res;
    if (previous) previous.end();
    const others = [...room.values()].filter(x => x !== me);
    emit(me, 'hello', { peer: others.length ? others[0].name : null, ...presence(room) });
    others.forEach(o => emit(o, 'peer', { name: me.name }));
    broadcastPresence(room);
    req.on('close', () => {
      if (me.res !== res || rooms.get(code) !== room || room.get(id) !== me) return;
      me.res = null;
      me.timer = setTimeout(() => removeMember(code, id, 'timeout'), GRACE);
    });
    return;
  }

  if (req.method !== 'POST') return json(404, {});
  const b = await readJson(req);
  const code = clean(b.room, 32).toLowerCase();

  if (url.pathname === '/api/join') {
    const name = clean(b.name, 24);
    if (!code || !name) return json(400, { error: 'Enter your name and a room code.' });
    // Old clients that omit capacity retain their original two-person behavior.
    const capacity = b.capacity === undefined ? 2 : b.capacity;
    if (!Number.isInteger(capacity) || capacity < 2 || capacity > 5)
      return json(400, { error: 'Choose a room size from 2 to 5 people.' });
    const room = rooms.get(code) || Object.assign(new Map(), { capacity, started: false });
    if (room.capacity !== capacity)
      return json(409, { error: `This is a ${room.capacity}-person room. Choose ${room.capacity} people to join.`, capacity: room.capacity });
    if (room.size >= room.capacity)
      return json(409, { error: `This room is full (${room.capacity} people). Try another code.` });
    const id = crypto.randomUUID();
    const m = { name, res: null, timer: setTimeout(() => removeMember(code, id, 'timeout'), GRACE) };
    room.set(id, m);
    if (room.size >= 2) room.started = true;
    rooms.set(code, room);
    broadcastPresence(room);
    return json(200, { id, room: code, capacity: room.capacity });
  }

  const room = rooms.get(code), me = room && room.get(b.id);
  if (!me) return json(404, { error: 'Session ended' });

  if (url.pathname === '/api/send') {
    const text = clean(b.text, 2000);
    const others = [...room.values()].filter(x => x !== me);
    if (!text) return json(400, { error: 'Empty message' });
    if (!others.length) return json(409, { error: 'Waiting for someone else to join.' });
    others.forEach(o => emit(o, 'msg', { id: b.id, name: me.name, text, t: Date.now() }));
    return json(200, { ok: true });
  }

  if (url.pathname === '/api/leave') { removeMember(code, b.id, 'left'); return json(200, { ok: true }); }
  json(404, {});
}).listen(process.env.PORT || 3000, () => console.log(`Chatverse is running on port ${server.address().port}. Nothing is stored.`));

setInterval(() => rooms.forEach(r => r.forEach(m => m.res && m.res.write(': ping\n\n'))), 20000);
