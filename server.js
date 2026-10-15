#!/usr/bin/env node
/* ============================================================================
   INFINITE FIGHT — servidor PvP online (sem dependências: só o Node.js 16+)

   O que ele faz:
     · serve o jogo (index.html, na mesma pasta) em  http://SEU-IP:3000
     · mantém salas de 2 jogadores e repassa as mensagens de um para o outro (WebSocket em /ws)
     · partida rápida (fila), sala com código de 4 letras, detecção de desconexão

   Como usar:
     node server.js                 (porta 3000)
     PORT=8080 node server.js       (outra porta)

   Jogar na mesma rede Wi-Fi: abra no celular/PC o endereço que aparece ao iniciar.
   Jogar pela internet: hospede num serviço que rode Node (Render, Railway, Fly.io...) ou use um túnel
   (ex.: cloudflared tunnel --url http://localhost:3000) e passe o link https para o amigo.
   ========================================================================== */
'use strict';
const http = require('http'), fs = require('fs'), path = require('path'), os = require('os'), crypto = require('crypto'), zlib = require('zlib');

const PORT = +process.env.PORT || 3000;
const HOST = process.env.HOST || '0.0.0.0';
const INDEX = process.env.INDEX || path.join(__dirname, 'index.html');
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const MAX_FRAME = 16 * 1024;        // maior mensagem aceita (bytes)
const RATE = 300;                   // mensagens por segundo por jogador (acima disso, descarta)
const LETTERS = 'ABCDEFGHJKLMNPQRSTUVWXYZ';
const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);

/* ---------- arquivos estáticos ---------- */
let cache = { m: 0, raw: null, gz: null };
function getIndex() {
  const st = fs.statSync(INDEX);
  if (st.mtimeMs !== cache.m) { cache.raw = fs.readFileSync(INDEX); cache.gz = zlib.gzipSync(cache.raw, { level: 6 }); cache.m = st.mtimeMs; }
  return cache;
}

const rooms = new Map();   // código -> { code, p:[peerA, peerB] }
const queue = [];          // fila da partida rápida
const peers = new Set();

const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/' || url === '/index.html') {
    try {
      const c = getIndex(), gz = /\bgzip\b/.test(req.headers['accept-encoding'] || '');
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store', ...(gz ? { 'Content-Encoding': 'gzip' } : {}) });
      res.end(gz ? c.gz : c.raw);
    } catch (e) { res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }); res.end('index.html não encontrado ao lado do server.js'); }
  } else if (url === '/health' || url === '/status') {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify({ ok: true, players: peers.size, rooms: rooms.size, queue: queue.length, chaos: chaos.size, teams: troom.size, db: USE_DB ? dbOk : false }));
  } else { res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }); res.end('404'); }
});

/* ---------- WebSocket mínimo (RFC 6455): texto, ping/pong, close, fragmentação ---------- */
function frame(op, payload) {
  const p = Buffer.isBuffer(payload) ? payload : Buffer.from(payload || '');
  const n = p.length; let h;
  if (n < 126) h = Buffer.from([0x80 | op, n]);
  else if (n < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(n, 2); }
  else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeUInt32BE(0, 2); h.writeUInt32BE(n, 6); }
  return Buffer.concat([h, p]);
}

let nextId = 1;
class Peer {
  constructor(sock) {
    this.sock = sock; this.id = nextId++; this.buf = Buffer.alloc(0); this.parts = []; this.partLen = 0;
    this.room = null; this.role = -1; this.name = 'Jogador'; this.alive = true; this.closed = false;
    this.tok = RATE; this.tl = Date.now();
    sock.setNoDelay(true); sock.setKeepAlive(true, 20000);
    sock.on('data', d => this.onData(d));
    sock.on('close', () => this.onClose());
    sock.on('error', () => { try { sock.destroy(); } catch (e) {} });
    peers.add(this);
    this.send({ t: 'hi', id: this.id });
  }
  send(o) { if (this.closed) return; try { this.sock.write(frame(0x1, JSON.stringify(o))); } catch (e) {} }
  close(code = 1000) {
    if (this.closed) return;
    try { const b = Buffer.alloc(2); b.writeUInt16BE(code); this.sock.write(frame(0x8, b)); this.sock.end(); } catch (e) {}
    setTimeout(() => { try { this.sock.destroy(); } catch (e) {} }, 500);
  }
  onData(d) {
    this.buf = Buffer.concat([this.buf, d]);
    if (this.buf.length > MAX_FRAME * 4) return this.close(1009);
    for (;;) {
      const b = this.buf; if (b.length < 2) return;
      const fin = !!(b[0] & 0x80), op = b[0] & 0x0f, masked = !!(b[1] & 0x80);
      let len = b[1] & 0x7f, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; if (b.readUInt32BE(2)) return this.close(1009); len = b.readUInt32BE(6); off = 10; }
      if (!masked) return this.close(1002);                 // o cliente é obrigado a mascarar
      if (len > MAX_FRAME) return this.close(1009);
      if (b.length < off + 4 + len) return;
      const mask = b.subarray(off, off + 4), pay = Buffer.from(b.subarray(off + 4, off + 4 + len));
      for (let i = 0; i < len; i++) pay[i] ^= mask[i & 3];
      this.buf = b.subarray(off + 4 + len);
      this.onFrame(fin, op, pay);
      if (this.closed) return;
    }
  }
  onFrame(fin, op, pay) {
    if (op === 0x8) { this.close(1000); return; }
    if (op === 0x9) { try { this.sock.write(frame(0xA, pay)); } catch (e) {} return; }
    if (op === 0xA) { this.alive = true; return; }
    if (op === 0x1 || op === 0x0) {
      if (op === 0x1) { this.parts = []; this.partLen = 0; }
      this.parts.push(pay); this.partLen += pay.length;
      if (this.partLen > MAX_FRAME) return this.close(1009);
      if (fin) { const s = Buffer.concat(this.parts).toString('utf8'); this.parts = []; this.partLen = 0; this.onText(s); }
    }
  }
  onText(s) {
    const n = Date.now(); this.tok = Math.min(RATE, this.tok + (n - this.tl) * RATE / 1000); this.tl = n;
    if (this.tok < 1) return; this.tok--;
    let m; try { m = JSON.parse(s); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;
    if (typeof m.name === 'string') this.name = m.name.replace(/[^\p{L}\p{N} _.-]/gu, '').slice(0, 12) || 'Jogador';
    switch (m.t) {
      case 'm': {                                           // repasse para o oponente
        const r = this.room; if (!r) return; const o = r.p[1 - this.role];
        if (o) o.send({ t: 'm', d: m.d });
        break;
      }
      case 'cj': chaosJoin(this, m); break;
      case 'tc': tJoin(this, m, true); break;
      case 'tj': tJoin(this, m, false); break;
      case 'tt': case 'ts': case 'tm': case 'tk': case 'tl': tMsg(this, m); break;
      case 'cm': { if (!this.cx) return; const o = { t: 'cm', f: this.cx.pid, d: m.d }, str = JSON.stringify(o); for (const q of chaos.values()) if (q !== this && !q.closed) { try { q.sock.write(frame(0x1, str)); } catch (e) {} } break; }
      case 'ck': chaosKill(this, m); break;
      case 'ping': this.send({ t: 'pong', ts: m.ts }); break;
      case 'create': createRoom(this); break;
      case 'join': joinRoom(this, String(m.code || '').toUpperCase().replace(/[^A-Z]/g, '').slice(0, 4)); break;
      case 'quick': quick(this); break;
      case 'leave': leave(this, true); break;
    }
  }
  onClose() { if (this.closed) return; this.closed = true; peers.delete(this); chaosLeave(this); tLeave(this); leave(this, true); log('saiu #' + this.id, '(' + peers.size + ' online)'); }
}

/* ---------- salas e fila ---------- */
function newCode() {
  for (let i = 0; i < 80; i++) { let c = ''; for (let j = 0; j < 4; j++) c += LETTERS[crypto.randomInt(LETTERS.length)]; if (!rooms.has(c)) return c; }
  return null;
}
function announce(r) {
  const names = [r.p[0] ? r.p[0].name : '', r.p[1] ? r.p[1].name : ''];
  r.p.forEach((q, i) => q && q.send({ t: 'room', code: r.code, role: i, n: r.p[1] ? 2 : 1, names }));
}
function leave(p, notify) {
  const qi = queue.indexOf(p); if (qi >= 0) queue.splice(qi, 1);
  const r = p.room; if (!r) return;
  rooms.delete(r.code);
  const o = r.p[1 - p.role]; p.room = null; p.role = -1;
  if (o) { o.room = null; o.role = -1; if (notify) o.send({ t: 'left' }); }
  log('sala ' + r.code + ' encerrada');
}
function createRoom(p) {
  leave(p, true);
  const code = newCode(); if (!code) return p.send({ t: 'err', m: 'Servidor cheio. Tente de novo.' });
  const r = { code, p: [p, null] }; rooms.set(code, r); p.room = r; p.role = 0; announce(r);
  log('sala ' + code + ' criada por ' + p.name);
}
function joinRoom(p, code) {
  leave(p, true);
  const r = rooms.get(code);
  if (!r) return p.send({ t: 'err', m: 'Sala não encontrada.' });
  if (r.p[1]) return p.send({ t: 'err', m: 'Essa sala já está cheia.' });
  r.p[1] = p; p.room = r; p.role = 1; announce(r);
  log('sala ' + code + ': ' + r.p[0].name + ' x ' + p.name);
}
function quick(p) {
  leave(p, true);
  while (queue.length) {
    const w = queue.shift();
    if (w === p || w.closed) continue;
    const code = newCode(); if (!code) return p.send({ t: 'err', m: 'Servidor cheio. Tente de novo.' });
    const r = { code, p: [w, p] }; rooms.set(code, r); w.room = r; w.role = 0; p.room = r; p.role = 1; announce(r);
    log('partida rápida ' + code + ': ' + w.name + ' x ' + p.name);
    return;
  }
  queue.push(p); p.send({ t: 'q' });
}

/* ---------- MODO CHAOS: sala única de até 12 jogadores, pontos salvos por "conta" (token do aparelho) ---------- */
const CHAOS_MAX = 12, chaos = new Map(); let cpid = 0;
/* ---- placar salvo de verdade: banco Upstash Redis (plano grátis) via REST; sem as variáveis, cai para arquivo (que some no Render grátis) ---- */
const SCORE_FILE = path.join(process.env.DATA_DIR || __dirname, 'scores.json');
const DB_URL = (process.env.UPSTASH_REDIS_REST_URL || '').replace(/\/+$/, ''), DB_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || '', USE_DB = !!(DB_URL && DB_TOKEN);
let scores = {}, scoresReady = !USE_DB, dbOk = false, flushing = false, flushErrAt = 0; const dirty = new Set();
async function dbCmd(cmd) {
  const r = await fetch(DB_URL, { method: 'POST', headers: { Authorization: 'Bearer ' + DB_TOKEN, 'Content-Type': 'application/json' }, body: JSON.stringify(cmd) });
  const j = await r.json(); if (!r.ok || j.error) throw new Error(j.error || ('HTTP ' + r.status)); return j.result;
}
async function loadScores() {
  if (!USE_DB) { try { scores = JSON.parse(fs.readFileSync(SCORE_FILE, 'utf8')); } catch (e) {} log('AVISO: sem banco configurado — os pontos do Chaos podem sumir quando o servidor reiniciar.'); return; }
  for (let i = 1; i <= 5; i++) {
    try { const flat = await dbCmd(['HGETALL', 'scores']) || []; for (let k = 0; k + 1 < flat.length; k += 2) { try { scores[flat[k]] = JSON.parse(flat[k + 1]); } catch (e) {} } dbOk = true; scoresReady = true; log('placar carregado do banco (' + Object.keys(scores).length + ' contas)'); return; }
    catch (e) { log('banco indisponível (' + e.message + '), tentativa ' + i + '/5'); await new Promise(r => setTimeout(r, 2000 * i)); }
  }
  scoresReady = true; log('ERRO: não consegui ler o banco; seguindo só com a memória (os pontos novos ainda tentam ser salvos).');
}
async function flushScores() {
  if (flushing || !dirty.size) return; flushing = true; const accs = [...dirty]; dirty.clear();
  try {
    if (USE_DB) { const args = ['HSET', 'scores']; for (const a of accs) if (scores[a]) args.push(a, JSON.stringify(scores[a])); if (args.length > 2) await dbCmd(args); dbOk = true; }
    else fs.writeFileSync(SCORE_FILE, JSON.stringify(scores));
  } catch (e) { for (const a of accs) dirty.add(a); if (Date.now() - flushErrAt > 60000) { flushErrAt = Date.now(); log('ERRO ao salvar placar: ' + e.message); } }
  flushing = false;
}
setInterval(flushScores, +process.env.FLUSH_MS || 10000).unref();
for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, async () => { try { await flushScores(); } catch (e) {} process.exit(0); });   // o Render avisa antes de reiniciar: salva na hora
function chaosJoin(p, m) {
  leave(p, true); chaosLeave(p);
  if (!scoresReady) return p.send({ t: 'err', m: 'O servidor está iniciando. Tente de novo em alguns segundos.' });
  if (chaos.size >= CHAOS_MAX) return p.send({ t: 'err', m: 'O Chaos está lotado (' + CHAOS_MAX + ' jogadores). Tente de novo em instantes.' });
  const acc = String(m.acc || '').replace(/[^a-zA-Z0-9]/g, '').slice(0, 32) || ('g' + p.id);
  let rec = scores[acc];
  if (!rec) { const first = Math.max(0, Math.min(3000, Math.floor(+m.pts || 0))); rec = scores[acc] = { name: p.name, pts: first, kills: 0 }; }   // 1ª vez da conta: aceita os pontos antigos do aparelho (até 3000); depois só o servidor conta
  rec.name = p.name; dirty.add(acc);
  p.cx = { pid: ++cpid, acc, ch: String(m.ch || 'fisch').replace(/[^a-z]/g, '').slice(0, 12) || 'fisch', lk: 0 }; chaos.set(p.cx.pid, p);
  const list = [...chaos.values()].filter(q => q !== p).map(q => ({ id: q.cx.pid, name: q.name, ch: q.cx.ch, pts: scores[q.cx.acc].pts }));
  p.send({ t: 'cw', me: p.cx.pid, pts: rec.pts, players: list });
  for (const q of chaos.values()) if (q !== p) q.send({ t: 'cn', p: { id: p.cx.pid, name: p.name, ch: p.cx.ch, pts: rec.pts } });
  log('chaos: ' + p.name + ' entrou (' + chaos.size + ')');
}
function chaosLeave(p) {
  if (!p.cx) return; chaos.delete(p.cx.pid);
  for (const q of chaos.values()) q.send({ t: 'cl', id: p.cx.pid }); p.cx = null;
}
function chaosKill(p, m) {                         // p = a vítima informa quem a derrubou (+10 pontos para o autor)
  if (!p.cx) return; const now = Date.now(); if (now - p.cx.lk < 2500) return; p.cx.lk = now;
  const k = chaos.get(m.k | 0);
  if (k && k !== p && k.cx) { const rec = scores[k.cx.acc]; rec.pts += 10; rec.kills = (rec.kills || 0) + 1; dirty.add(k.cx.acc); }
  for (const q of chaos.values()) q.send({ t: 'ck', k: k && k !== p ? k.cx.pid : 0, v: p.cx.pid, kn: k ? k.name : '', vn: p.name, pts: k && k !== p && q === k ? scores[k.cx.acc].pts : undefined });
}
setInterval(() => {
  if (!chaos.size) return;
  const top = Object.values(scores).sort((a, b) => b.pts - a.pts).slice(0, 10).map(r => [r.name, r.pts]);
  const on = [...chaos.values()].map(q => ({ id: q.cx.pid, pts: scores[q.cx.acc].pts }));
  for (const q of chaos.values()) q.send({ t: 'lb', top, on });
}, 3000).unref();

/* ---------- MODO EQUIPES: salas com código, 2 times (até 4 por time), vence quem chegar ao alvo de abates ou tiver mais no fim do tempo ---------- */
const TEAM_MAX = 8, T_TARGET = +process.env.T_TARGET || 15, T_LIMIT = +process.env.T_LIMIT || 300, troom = new Map();
function tNewCode() { for (let k = 0; k < 50; k++) { let c = ''; for (let i = 0; i < 4; i++) c += LETTERS[crypto.randomInt(LETTERS.length)]; if (!troom.has(c) && !rooms.has(c)) return c; } return null; }
function tInfo(r, p) { return { t: 'tr', code: r.code, me: p.tm.pid, host: r.host, state: r.state, players: [...r.players.values()].map(q => ({ id: q.tm.pid, name: q.name, ch: q.tm.ch, team: q.tm.team })), score: r.score, target: T_TARGET, left: Math.max(0, Math.round((r.endsAt - Date.now()) / 1000)) }; }
function tBroadcast(r) { for (const q of r.players.values()) q.send(tInfo(r, q)); }
function tBalance(r) { let a = 0, b = 0; for (const q of r.players.values()) q.tm.team ? b++ : a++; return a <= b ? 0 : 1; }
function tJoin(p, m, create) {
  tLeave(p); leave(p, true); chaosLeave(p);
  let r;
  if (create) { const code = tNewCode(); if (!code) return p.send({ t: 'err', m: 'Servidor cheio' }); r = { code, players: new Map(), host: 0, state: 'lobby', score: [0, 0], endsAt: 0, nextPid: 1 }; troom.set(code, r); }
  else { r = troom.get(String(m.code || '').toUpperCase().trim()); if (!r) return p.send({ t: 'err', m: 'Sala de equipes não encontrada' }); if (r.players.size >= TEAM_MAX) return p.send({ t: 'err', m: 'Sala cheia (máx. ' + TEAM_MAX + ')' }); }
  const pid = r.nextPid++; p.tm = { room: r, pid, team: tBalance(r), ch: String(m.ch || 'fisch').replace(/[^a-z]/g, '').slice(0, 12) || 'fisch', lk: 0, k: 0, d: 0 };
  r.players.set(pid, p); if (!r.host) r.host = pid; tBroadcast(r); log('equipes ' + r.code + ': ' + p.name + ' entrou (' + r.players.size + ')');
}
function tEnd(r, winner) {
  if (r.state !== 'play') return; r.state = 'end';
  const stats = [...r.players.values()].map(q => ({ id: q.tm.pid, name: q.name, team: q.tm.team, k: q.tm.k, d: q.tm.d }));
  for (const q of r.players.values()) q.send({ t: 'te', winner, score: r.score, stats });
}
function tLeave(p) {
  const tm = p.tm; if (!tm) return; const r = tm.room; r.players.delete(tm.pid); p.tm = null;
  if (!r.players.size) { troom.delete(r.code); return; }
  if (r.host === tm.pid) r.host = r.players.keys().next().value;
  if (r.state === 'play') { const c = [0, 0]; for (const q of r.players.values()) c[q.tm.team]++; if (!c[0] || !c[1]) tEnd(r, c[0] ? 0 : 1); }
  tBroadcast(r);
}
function tStart(r) {
  r.state = 'play'; r.score = [0, 0]; r.endsAt = Date.now() + T_LIMIT * 1000;
  for (const q of r.players.values()) { q.tm.k = 0; q.tm.d = 0; }
  for (const q of r.players.values()) q.send(Object.assign(tInfo(r, q), { t: 'tstart' }));
}
function tMsg(p, m) {
  const tm = p.tm; if (!tm) return; const r = tm.room;
  switch (m.t) {
    case 'tt': { if (r.state === 'play') return; const want = m.team ? 1 : 0; let n = 0; for (const q of r.players.values()) if (q.tm.team === want) n++; if (n >= TEAM_MAX / 2 || tm.team === want) return; tm.team = want; tBroadcast(r); break; }
    case 'ts': { if (r.host !== tm.pid || r.state === 'play') return; const c = [0, 0]; for (const q of r.players.values()) c[q.tm.team]++; if (!c[0] || !c[1]) return p.send({ t: 'err', m: 'Precisa de pelo menos 1 jogador em cada time.' }); tStart(r); break; }
    case 'tm': { if (r.state !== 'play') return; const str = JSON.stringify({ t: 'tm', f: tm.pid, d: m.d }); for (const q of r.players.values()) if (q !== p && !q.closed) { try { q.sock.write(frame(0x1, str)); } catch (e) {} } break; }
    case 'tk': {                                   // a vítima informa quem a derrubou
      if (r.state !== 'play') return; const now = Date.now(); if (now - tm.lk < 2500) return; tm.lk = now; tm.d++;
      const k = r.players.get(m.k | 0); let ok = k && k !== p && k.tm.team !== tm.team;
      if (ok) { k.tm.k++; r.score[k.tm.team]++; }
      for (const q of r.players.values()) q.send({ t: 'tk', k: ok ? k.tm.pid : 0, v: tm.pid, kn: ok ? k.name : '', vn: p.name, sc: r.score });
      if (ok && r.score[k.tm.team] >= T_TARGET) tEnd(r, k.tm.team); break; }
    case 'tl': tLeave(p); break;
  }
}
setInterval(() => { const now = Date.now(); for (const r of troom.values()) if (r.state === 'play' && now >= r.endsAt) tEnd(r, r.score[0] > r.score[1] ? 0 : r.score[1] > r.score[0] ? 1 : -1); }, 1000).unref();

server.on('upgrade', (req, sock) => {
  if (req.url.split('?')[0] !== '/ws' || String(req.headers.upgrade).toLowerCase() !== 'websocket' || !req.headers['sec-websocket-key']) { sock.destroy(); return; }
  const acc = crypto.createHash('sha1').update(req.headers['sec-websocket-key'] + GUID).digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + acc + '\r\n\r\n');
  const p = new Peer(sock);
  log('entrou #' + p.id, '(' + peers.size + ' online)');
});

// batimento: derruba conexões mortas
setInterval(() => {
  for (const p of peers) {
    if (!p.alive) { try { p.sock.destroy(); } catch (e) {} continue; }
    p.alive = false;
    try { p.sock.write(frame(0x9, '')); } catch (e) {}
  }
}, 20000).unref();

loadScores();
server.listen(PORT, HOST, () => {
  console.log('\n  INFINITE FIGHT — servidor PvP rodando na porta ' + PORT + '\n');
  console.log('  Neste computador:   http://localhost:' + PORT);
  for (const list of Object.values(os.networkInterfaces()))
    for (const a of list || []) if (a.family === 'IPv4' && !a.internal) console.log('  Mesma rede Wi-Fi:   http://' + a.address + ':' + PORT);
  try { getIndex(); } catch (e) { console.log('\n  ATENÇÃO: coloque o index.html do jogo na mesma pasta do server.js.'); }
  console.log('\n  (Ctrl+C para parar)\n');
});
