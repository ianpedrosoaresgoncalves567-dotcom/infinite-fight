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
    res.end(JSON.stringify({ ok: true, players: peers.size, rooms: rooms.size, queue: queue.length, chaos: chaos.size, events: ev.active.size, teams: troom.size, db: USE_DB ? dbOk : false }));
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
      case 'evc': evClaim(this, m); break;
      case 'evv': evVis(this, m); break;
      case 'evh': evHouse(this, m); break;
      case 'evr': evRup(this, m); break;
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
  p.send({ t: 'cw', me: p.cx.pid, pts: rec.pts, players: list, ev: evSnap() });
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

/* ---------- EVENTOS GLOBAIS DO CHAOS (Cidade do Caos · Etapa 3) ----------
   O SERVIDOR é o único que decide quando um evento nasce e termina; os jogadores só recebem o aviso.
   Todos os números estão em EV_CFG (um lugar só). Como a raridade funciona:
     1. o "relógio de eventos" só anda enquanto há gente no Chaos (sala vazia não gasta tempo);
     2. a cada intervalo aleatório (drawEveryMs) o servidor faz UM sorteio;
     3. o sorteio escolhe a categoria (peso) e depois rola a chance dela — na maioria das vezes NADA acontece;
     4. antes de iniciar: respeita tempo mínimo entre eventos, descanso da categoria (cooldownMs), descanso do
        próprio evento e a lista de incompatíveis (nunca 2 eventos incompatíveis juntos).
   Para testar rápido: EV_DEBUG=1 (sorteio a cada ~15 s, chance 100%, liga o "SINAL DE TESTE"). EV_ON=0 desliga tudo. */
const EV_DBG = process.env.EV_DEBUG === '1', MIN = 60e3, HOUR = 60 * MIN;
const EV_CFG = {
  on: process.env.EV_ON !== '0',
  tickMs: EV_DBG ? 2000 : 5000,                                   // de quanto em quanto tempo o servidor confere o relógio
  minPlayers: 1,                                                  // só roda com pelo menos esta gente no Chaos
  drawEveryMs: EV_DBG ? [15e3, 20e3] : [30 * MIN, 90 * MIN],      // intervalo aleatório entre sorteios (tempo de sala ocupada)
  minGapMs: EV_DBG ? 20e3 : 15 * MIN,                             // mínimo entre o FIM de um evento e o início do próximo
  maxSimult: 1,                                                   // quantos eventos ao mesmo tempo (os incompatíveis nunca juntam)
  cats: {                                                         // weight = peso no sorteio · chance = probabilidade de acontecer · cooldownMs = descanso da categoria
    raro:       { weight: 70, chance: EV_DBG ? 1 : .35, cooldownMs: EV_DBG ? 0 : 30 * MIN },
    muito_raro: { weight: 20, chance: EV_DBG ? 1 : .20, cooldownMs: EV_DBG ? 0 : 90 * MIN },
    lendario:   { weight: 10, chance: EV_DBG ? 1 : .15, cooldownMs: EV_DBG ? 0 : 4 * HOUR }   // o mais raro de todos: descanso de 4 h evita sequências
  }
};
/* Cada evento: cat, name, icon, durMs, weight (entre os da mesma categoria), cooldownMs (próprio), incompat (ids), exclusive,
   make() = dados que o servidor sorteia (ex.: local) e manda para todos, onStart/onEnd = lógica do servidor. As Etapas 4+ entram aqui. */
const EV_DEFS = {
  sinal_teste: { on: false, cat: 'raro', name: 'SINAL DE TESTE', icon: '📡', durMs: 15e3, weight: 1, cooldownMs: 0, incompat: [], exclusive: false, make: () => ({}) }
};
const ev = { clock: 0, nextDraw: 0, last: {}, lastCat: {}, lastEnd: 0, active: new Map(), seq: 0, lt: Date.now() };
const evRand = r => r[0] + Math.random() * (r[1] - r[0]);
const evPub = e => ({ sid: e.sid, id: e.id, cat: e.cat, name: e.def.name, icon: e.def.icon, left: Math.max(0, e.endsAt - Date.now()), dur: e.def.durMs, data: e.data, got: e.claimed ? [...e.claimed] : [], live: e.def.live ? e.def.live(e) : undefined });
const evSnap = () => [...ev.active.values()].map(evPub);          // quem entra no meio recebe o estado atual
const evBroadcast = o => { for (const q of chaos.values()) q.send(o); };
function evCan(d) { for (const a of ev.active.values()) if (d.exclusive || a.def.exclusive || d.incompat.includes(a.id) || a.def.incompat.includes(d.id)) return false; return true; }
function evStart(id, d, now) {
  const e = { sid: ++ev.seq, id, cat: d.cat, def: d, endsAt: now + d.durMs, data: d.make ? d.make() : {} };
  ev.active.set(e.sid, e); ev.last[id] = ev.lastCat[d.cat] = now;
  if (d.onStart) d.onStart(e);
  evBroadcast(Object.assign({ t: 'ev', a: 'start' }, evPub(e))); log('EVENTO [' + d.cat + '] ' + d.name + ' começou');
}
function evEnd(e) {
  if (!ev.active.delete(e.sid)) return; ev.lastEnd = Date.now();
  if (e.def.onEnd) e.def.onEnd(e);
  evBroadcast({ t: 'ev', a: 'end', sid: e.sid, id: e.id }); log('EVENTO ' + e.def.name + ' terminou');
}
function evDraw(now) {
  if (ev.active.size >= EV_CFG.maxSimult || now - ev.lastEnd < EV_CFG.minGapMs) return;
  const cats = Object.entries(EV_CFG.cats); let tot = 0; for (const c of cats) tot += c[1].weight;
  let r = Math.random() * tot, cat = cats[0][0]; for (const [k, c] of cats) { if ((r -= c.weight) < 0) { cat = k; break; } }
  if (EV_DBG && EV_DEFS[process.env.EV_ONLY]) cat = EV_DEFS[process.env.EV_ONLY].cat;   // teste: EV_ONLY=<id do evento> força só esse evento
  const C = EV_CFG.cats[cat];
  if (Math.random() >= C.chance) return;                                    // o sorteio falhou: nada acontece (o normal)
  if (now - (ev.lastCat[cat] || 0) < C.cooldownMs) return;                  // categoria descansando
  const pool = Object.entries(EV_DEFS).filter(([id, d]) => d.on && d.cat === cat && (!EV_DBG || !EV_DEFS[process.env.EV_ONLY] || id === process.env.EV_ONLY) && now - (ev.last[id] || 0) >= d.cooldownMs && evCan(d));
  if (!pool.length) return;
  let w = 0; for (const p of pool) w += p[1].weight; let x = Math.random() * w, pick = pool[0]; for (const p of pool) { if ((x -= p[1].weight) < 0) { pick = p; break; } }
  evStart(pick[0], pick[1], now);
}
function evTick() {
  const now = Date.now(), dt = Math.min(now - ev.lt, EV_CFG.tickMs * 3); ev.lt = now;
  for (const e of [...ev.active.values()]) if (now >= e.endsAt || !chaos.size) evEnd(e);   // sala vazia encerra tudo
  if (!EV_CFG.on || chaos.size < EV_CFG.minPlayers) return;
  ev.clock += dt; if (!ev.nextDraw) ev.nextDraw = ev.clock + evRand(EV_CFG.drawEveryMs);
  if (ev.clock < ev.nextDraw) return;
  ev.nextDraw = ev.clock + evRand(EV_CFG.drawEveryMs); evDraw(now);
}
setInterval(evTick, EV_CFG.tickMs).unref();
{ const tot = Object.values(EV_CFG.cats).reduce((s, c) => s + c.weight, 0), dh = HOUR / ((EV_CFG.drawEveryMs[0] + EV_CFG.drawEveryMs[1]) / 2);
  log('eventos ' + (EV_CFG.on ? 'ligados' : 'DESLIGADOS') + (EV_DBG ? ' (MODO TESTE)' : '') + ' · máx. aprox. de 1 a cada: ' + Object.entries(EV_CFG.cats).map(([k, c]) => k + ' ' + (1 / (c.weight / tot * c.chance * dh)).toFixed(1) + ' h').join(' · ') + ' (de sala ocupada)'); }

/* ---------- EVENTO: CHUVA DE FRAGMENTOS (raro) · Cidade do Caos · Etapa 4 ----------
   O servidor sorteia TODO o cronograma (onde e quando cada fragmento cai) e manda uma vez no início; os aparelhos só
   desenham e calculam o dano de quem está neles (igual ao resto do jogo). Recompensas: o servidor entrega os pontos uma
   única vez por fragmento (quem pegar primeiro), com limite por jogador. Tudo ajustável em FR. */
const FR = {
  durMs: 45e3, count: [16, 20],           // duração e quantidade de fragmentos
  firstMs: 3500, lastMs: 34e3,            // quando cai o primeiro e o último (a zona do último ainda dura zoneMs)
  warnMs: 2200, zoneMs: 7000, radius: 70, // aviso antes do impacto · tempo da zona perigosa · raio
  minDist: 300,                           // distância mínima entre fragmentos que ficam ativos ao mesmo tempo
  rewards: 4, rewardPts: 5, rewardMs: 12e3, maxClaims: 3   // fragmentos com recompensa · pontos · quanto tempo ela fica · limite por jogador
};
/* Pontos livres de impacto (x/10, y/10 alternados), calculados da cidade: ruas, calçadas e praça, longe de paredes.
   Se o mapa da cidade mudar, estes pontos precisam ser gerados de novo. */
const FR_PTS = [7,7,13,7,19,7,25,7,31,7,37,7,43,7,49,7,55,7,61,7,67,7,73,7,79,7,85,7,91,7,97,7,103,7,109,7,115,7,121,7,127,7,133,7,139,7,145,7,151,7,157,7,163,7,169,7,175,7,181,7,187,7,193,7,199,7,205,7,211,7,217,7,223,7,229,7,235,7,241,7,247,7,253,7,259,7,265,7,271,7,7,13,67,13,73,13,139,13,205,13,211,13,7,19,67,19,73,19,139,19,205,19,211,19,7,25,67,25,73,25,139,25,205,25,211,25,7,31,67,31,73,31,139,31,205,31,211,31,7,37,67,37,73,37,139,37,205,37,211,37,7,43,67,43,73,43,139,43,205,43,211,43,7,49,67,49,73,49,139,49,205,49,211,49,7,55,67,55,73,55,139,55,205,55,211,55,7,61,13,61,19,61,25,61,31,61,37,61,43,61,49,61,55,61,61,61,67,61,73,61,79,61,85,61,91,61,97,61,103,61,109,61,115,61,121,61,127,61,133,61,139,61,145,61,151,61,157,61,163,61,169,61,175,61,181,61,187,61,193,61,199,61,205,61,211,61,217,61,223,61,229,61,235,61,241,61,247,61,253,61,259,61,265,61,271,61,7,67,43,67,49,67,55,67,61,67,67,67,73,67,79,67,85,67,91,67,97,67,103,67,109,67,115,67,121,67,127,67,133,67,139,67,205,67,211,67,7,73,67,73,73,73,79,73,97,73,103,73,109,73,115,73,133,73,139,73,205,73,211,73,7,79,67,79,73,79,79,79,97,79,103,79,109,79,115,79,133,79,139,79,205,79,211,79,7,85,67,85,73,85,79,85,85,85,91,85,115,85,121,85,127,85,133,85,139,85,205,85,211,85,7,91,61,91,67,91,73,91,79,91,85,91,91,91,115,91,121,91,127,91,133,91,139,91,205,91,211,91,7,97,13,97,67,97,73,97,79,97,85,97,91,97,115,97,121,97,127,97,133,97,139,97,205,97,211,97,7,103,13,103,67,103,73,103,79,103,97,103,103,103,109,103,115,103,133,103,139,103,205,103,211,103,7,109,13,109,61,109,67,109,73,109,79,109,97,109,103,109,109,109,115,109,133,109,139,109,205,109,211,109,7,115,13,115,19,115,25,115,31,115,37,115,43,115,49,115,55,115,61,115,67,115,73,115,79,115,85,115,91,115,97,115,103,115,109,115,115,115,121,115,127,115,133,115,139,115,145,115,151,115,157,115,163,115,169,115,175,115,181,115,187,115,193,115,199,115,205,115,211,115,217,115,223,115,229,115,235,115,241,115,247,115,253,115,259,115,265,115,271,115,7,121,13,121,19,121,25,121,31,121,37,121,43,121,49,121,55,121,61,121,67,121,73,121,79,121,85,121,91,121,97,121,103,121,109,121,115,121,121,121,127,121,133,121,139,121,145,121,151,121,157,121,163,121,169,121,175,121,181,121,187,121,193,121,199,121,205,121,211,121,217,121,223,121,229,121,235,121,241,121,247,121,253,121,259,121,265,121,271,121,7,127,43,127,49,127,55,127,61,127,67,127,73,127,109,127,115,127,121,127,127,127,133,127,139,127,205,127,211,127,7,133,67,133,73,133,139,133,205,133,211,133,7,139,67,139,73,139,139,139,205,139,211,139,7,145,67,145,73,145,139,145,205,145,211,145,7,151,61,151,67,151,73,151,127,151,133,151,139,151,205,151,211,151,7,157,13,157,67,157,73,157,79,157,85,157,139,157,205,157,211,157,7,163,13,163,67,163,73,163,79,163,85,163,139,163,205,163,211,163,7,169,13,169,61,169,67,169,73,169,79,169,85,169,127,169,133,169,139,169,205,169,211,169];
EV_DEFS.fragmentos = {
  on: true, cat: 'raro', name: 'CHUVA DE FRAGMENTOS', icon: '☄️', durMs: FR.durMs, weight: 1, cooldownMs: EV_DBG ? 0 : 20 * MIN, incompat: [], exclusive: false,
  make() {
    const n = Math.round(FR.count[0] + Math.random() * (FR.count[1] - FR.count[0])), frags = [];
    for (let i = 0; i < n; i++) {
      const t = Math.round(FR.firstMs + (n > 1 ? i / (n - 1) : 0) * (FR.lastMs - FR.firstMs) + (Math.random() - .5) * 500); let x = 0, y = 0;
      for (let k = 0; k < 30; k++) {      // escolhe um ponto longe dos fragmentos que ainda estarão ativos
        const j = (Math.random() * FR_PTS.length / 2 | 0) * 2; x = FR_PTS[j] * 10; y = FR_PTS[j + 1] * 10;
        if (frags.every(f => t - f.t > FR.zoneMs + FR.warnMs || Math.hypot(f.x - x, f.y - y) >= FR.minDist)) break;
      }
      frags.push({ x, y, t, rw: 0 });
    }
    const idx = frags.map((_, i) => i).slice(2).sort(() => Math.random() - .5).slice(0, FR.rewards); for (const i of idx) frags[i].rw = 1;   // os 2 primeiros nunca têm recompensa
    return { frags, r: FR.radius, warn: FR.warnMs, zone: FR.zoneMs, rwMs: FR.rewardMs, rwPts: FR.rewardPts };
  },
  onStart(e) { e.claimed = new Set(); e.by = new Map(); }
};
function evClaim(p, m) {                  // um jogador diz "peguei o fragmento i": o servidor confere tudo
  const e = ev.active.get(m.sid | 0); if (!p.cx || !e || e.id !== 'fragmentos') return;
  const i = m.i | 0, f = e.data.frags[i]; if (!f || !f.rw || e.claimed.has(i)) return;                    // existe, tem recompensa e ainda não foi pega
  const el = Date.now() - (e.endsAt - e.def.durMs); if (el < f.t + 400 || el > f.t + FR.rewardMs) return;  // só depois do impacto e antes de sumir
  const n = e.by.get(p.cx.pid) || 0; if (n >= FR.maxClaims) return;
  e.claimed.add(i); e.by.set(p.cx.pid, n + 1);
  const rec = scores[p.cx.acc]; rec.pts += FR.rewardPts; dirty.add(p.cx.acc);
  evBroadcast({ t: 'ev', a: 'got', sid: e.sid, i, by: p.cx.pid }); p.send({ t: 'ev', a: 'pts', pts: rec.pts, n: FR.rewardPts });
}

/* ---------- EVENTO: O VISITANTE (muito raro) · Cidade do Caos · Etapa 5 ----------
   Um personagem misterioso aparece num ponto livre da cidade e some depois de VIS.durMs. Ninguém é obrigado a falar com ele.
   Cada visitante é uma linha em VIS.list (identidade, falas e comportamento) — para criar outro, basta copiar uma linha.
   Comportamentos (mode):  oferta  = ganha pontos ao falar (1 vez por jogador)
                           vigilia = ficar perto dele needMs sem sair do círculo
                           desafio = sobreviver needMs a uma sequência de ataques dele (dano calculado no aparelho de quem luta)
   O servidor confere: tempo mínimo decorrido, morte durante a prova e recompensa uma única vez por jogador. */
const VIS = {
  durMs: 100e3, radius: 55,
  list: [
    { w: 1, id: 'mercador', name: 'O MERCADOR SEM ROSTO', icon: '🎭', color: '#b07cff', mode: 'oferta', pts: 15,
      lines: { hi: 'Eu não vendo nada hoje. Só deixo um presente.', ok: 'Guarde isto. Nunca me viu.', again: 'Você já levou o que era seu.' } },
    { w: 1, id: 'sentinela', name: 'A SENTINELA CINZENTA', icon: '🗿', color: '#9fb4c7', mode: 'vigilia', needMs: 15e3, pts: 25,
      lines: { hi: 'Fique ao meu lado. Não se mova do círculo.', ok: 'A vigília terminou. Você resistiu.', again: 'Sua vigília já foi cumprida.' } },
    { w: 1, id: 'duelista', name: 'O DUELISTA DE CINZAS', icon: '🔥', color: '#ff6a3d', mode: 'desafio', needMs: 20e3, pts: 40,
      lines: { hi: 'Sobreviva ao meu fogo e eu pago o preço.', ok: 'Ainda de pé. Respeito.', again: 'Já nos enfrentamos.' } }
  ]
};
EV_DEFS.visitante = {
  on: true, cat: 'muito_raro', name: 'O VISITANTE', icon: '🎭', durMs: VIS.durMs, weight: 1, cooldownMs: EV_DBG ? 0 : 60 * MIN, incompat: [], exclusive: false,
  make() {
    let w = 0; for (const q of VIS.list) w += q.w; let x = Math.random() * w, v = VIS.list[0]; for (const q of VIS.list) { if ((x -= q.w) < 0) { v = q; break; } }
    if (EV_DBG && process.env.EV_VIS) v = VIS.list.find(q => q.id === process.env.EV_VIS) || v;   // teste: EV_VIS=mercador|sentinela|duelista força o visitante
    const j = (Math.random() * FR_PTS.length / 2 | 0) * 2;
    return { x: FR_PTS[j] * 10, y: FR_PTS[j + 1] * 10, v: { id: v.id, name: v.name, icon: v.icon, color: v.color, mode: v.mode, needMs: v.needMs || 0, pts: v.pts, lines: v.lines, r: VIS.radius } };
  },
  onStart(e) { e.st = new Map(); }
};
function evVis(p, m) {                    // o jogador conversa (talk), desiste (cancel) ou diz que terminou a prova (done)
  const e = ev.active.get(m.sid | 0); if (!p.cx || !e || e.id !== 'visitante') return;
  const v = e.data.v, s = e.st.get(p.cx.pid) || { t0: 0, done: 0 }; e.st.set(p.cx.pid, s); const now = Date.now();
  const say = txt => p.send({ t: 'ev', a: 'vsay', sid: e.sid, txt });
  if (s.done) return m.a === 'talk' ? say(v.lines.again) : 0;
  if (m.a === 'cancel') { s.t0 = 0; return; }
  if (m.a === 'talk') {
    if (v.mode === 'oferta') return grant(p, e, s, v);
    if (!s.t0) { s.t0 = now; say(v.lines.hi); p.send({ t: 'ev', a: 'vstart', sid: e.sid, need: v.needMs }); }
  } else if (m.a === 'done' && s.t0 && v.mode !== 'oferta') {
    if (now - s.t0 < v.needMs - 600) return;                                    // cedo demais
    if ((p.cx.lk || 0) >= s.t0) { s.t0 = 0; p.send({ t: 'ev', a: 'vfail', sid: e.sid }); return; }   // morreu durante a prova
    grant(p, e, s, v);
  }
  function grant(p, e, s, v) {
    s.done = 1; s.t0 = 0; const rec = scores[p.cx.acc]; rec.pts += v.pts; dirty.add(p.cx.acc);
    p.send({ t: 'ev', a: 'pts', pts: rec.pts, n: v.pts }); say(v.lines.ok); log('visitante: ' + p.name + ' recebeu ' + v.pts + ' pts');
  }
}

/* ---------- EVENTO: A CASA IMPOSSÍVEL (muito raro) · Cidade do Caos · Etapa 6 ----------
   Uma das 3 construções com interior (0 casa, 1 loja, 2 galpão) é escolhida pelo servidor e ganha passagens impossíveis
   para salas extras. Missão: achar 3 Fragmentos de Realidade (um na construção, um em cada sala extra) e abrir o Baú
   Impossível. O 1º a abrir ganha a recompensa rara; quem abrir depois ganha um prêmio menor. O servidor manda só
   {construção, semente}: a partir da semente todos os aparelhos montam as MESMAS salas, armadilhas e fragmentos. */
const CASA = { durMs: 180e3, relicPts: 10, chestPts: 100, chestLatePts: 30, relicMinMs: 3000, chestMinMs: 8000 };
EV_DEFS.casa_impossivel = {
  on: true, cat: 'muito_raro', name: 'A CASA IMPOSSÍVEL', icon: '🌀', durMs: CASA.durMs, weight: 1, cooldownMs: EV_DBG ? 0 : 60 * MIN, incompat: [], exclusive: false,
  make() { return { b: Math.floor(Math.random() * 3), seed: 1 + Math.floor(Math.random() * 1e6), cb: '' }; },   // cb = nome de quem abriu o baú primeiro
  onStart(e) { e.st = new Map(); e.chestBy = 0; }
};
function evHouse(p, m) {                  // o jogador diz "peguei o fragmento i" (relic) ou "abri o baú" (chest)
  const e = ev.active.get(m.sid | 0); if (!p.cx || !e || e.id !== 'casa_impossivel') return;
  const s = e.st.get(p.cx.pid) || { rel: new Set(), chest: 0 }; e.st.set(p.cx.pid, s);
  const el = Date.now() - (e.endsAt - e.def.durMs), rec = scores[p.cx.acc];
  const pay = n => { rec.pts += n; dirty.add(p.cx.acc); p.send({ t: 'ev', a: 'pts', pts: rec.pts, n }); };
  if (m.a === 'relic') {
    const i = m.i | 0; if (i < 0 || i > 2 || s.rel.has(i) || el < CASA.relicMinMs) return;
    s.rel.add(i); pay(CASA.relicPts); p.send({ t: 'ev', a: 'hrel', sid: e.sid, n: s.rel.size });
  } else if (m.a === 'chest') {
    if (s.rel.size < 3 || s.chest || el < CASA.chestMinMs) return; s.chest = 1;
    if (!e.chestBy) { e.chestBy = p.cx.pid; e.data.cb = p.name; pay(CASA.chestPts); evBroadcast({ t: 'ev', a: 'hchest', sid: e.sid, by: p.name }); log('casa impossível: ' + p.name + ' abriu o baú'); }
    else pay(CASA.chestLatePts);
  }
}

/* ---------- EVENTO: A RUPTURA (LENDÁRIO) · Cidade do Caos · Etapa 7 ----------
   Uma fenda dimensional se abre sobre uma região da cidade. Criaturas saem dela em ondas e, por fim, o Colosso.
   O servidor é dono da VIDA de cada criatura (igual para todos): os aparelhos mandam o dano que causam, o servidor
   confere (limite por jogador por segundo, criatura nascida e viva) e devolve a vida. Movimento e ataques das criaturas
   saem do relógio do evento + semente, então todos veem o mesmo sem trocar mensagens. Só o dano que a criatura causa
   em VOCÊ é calculado no seu aparelho (como no resto do jogo).
   Fim: vitória (Colosso derrotado, a fenda colapsa e a recompensa é dividida pelo dano causado) ou fracasso (tempo
   esgotado, a fenda se fecha sozinha, prêmio de consolação). Nada permanente fica no mapa.
   Exclusivo: nenhum outro evento acontece junto. Ganchos para o futuro (Despertar Secreto): EV_HOOKS — nada os usa hoje. */
const RUP = {
  durMs: 150e3, zoneR: 520, closeMs: 9e3,                            // duração · raio da região afetada · tempo do colapso após a vitória
  kinds: {                                                           // hp base (escala com jogadores) · raio
    larva:   { hp: 450,  r: 16 },
    guardiao: { hp: 1400, r: 24 },
    colosso: { hp: 6500, r: 46 }
  },
  hpPerPlayer: .35, maxPlayersScale: 12,                             // +35% de vida por jogador extra, até 12
  sched: [[8e3, 'larva', 4], [30e3, 'larva', 4], [45e3, 'guardiao', 2], [70e3, 'colosso', 1], [95e3, 'larva', 4]],   // [quando (ms), tipo, quantos]
  dmgPerSec: 4000, dmgBurst: 6000, spawnGraceMs: 500,                // limite de dano aceito por jogador (anti-trapaça)
  winPool: 300, minReward: 20, mvpBonus: 100, lossReward: 10         // pontos
};
const EV_HOOKS = { legendStart: [], legendEnd: [] };                  // futuros Despertares Secretos podem se registrar aqui
const evHook = (n, ...a) => { for (const f of EV_HOOKS[n]) try { f(...a); } catch (e) { log('gancho falhou: ' + e.message); } };
EV_DEFS.ruptura = {
  on: true, cat: 'lendario', name: 'A RUPTURA', icon: '🌌', durMs: RUP.durMs, weight: 1, cooldownMs: EV_DBG ? 0 : 6 * HOUR, incompat: [], exclusive: true,
  make() {
    const j = (Math.random() * FR_PTS.length / 2 | 0) * 2, n = Math.min(RUP.maxPlayersScale, Math.max(1, chaos.size)), sc = 1 + RUP.hpPerPlayer * (n - 1), cr = []; let id = 0;
    for (const [sp, k, q] of RUP.sched) for (let i = 0; i < q; i++) cr.push({ id: ++id, k, sp: sp + i * 400, mh: Math.round(RUP.kinds[k].hp * sc), r: RUP.kinds[k].r });
    return { x: FR_PTS[j] * 10, y: FR_PTS[j + 1] * 10, R: RUP.zoneR, seed: 1 + Math.floor(Math.random() * 1e6), cr };
  },
  onStart(e) { e.hp = new Map(e.data.cr.map(c => [c.id, c.mh])); e.dead = new Set(); e.part = new Map(); e.win = 0; e.tok = new Map(); e.dirty = new Set(); ev.lastLegend = { id: 'ruptura', at: Date.now(), win: null }; evHook('legendStart', e); },
  live(e) { return e.hp ? { hp: [...e.hp].filter(([id]) => !e.dead.has(id)), dead: [...e.dead], win: e.win } : undefined; },
  onEnd(e) {
    const P = [...e.part.values()].filter(q => q.dmg > 0), tot = P.reduce((s, q) => s + q.dmg, 0); let top = null;
    for (const q of P) if (!top || q.dmg > top.dmg) top = q;
    ev.lastLegend = { id: 'ruptura', at: Date.now(), win: !!e.win, top: top && top.name };
    for (const q of P) {
      const n = e.win ? Math.max(RUP.minReward, Math.round(RUP.winPool * q.dmg / tot)) + (q === top ? RUP.mvpBonus : 0) : RUP.lossReward, rec = scores[q.acc];
      if (!rec) continue; rec.pts += n; dirty.add(q.acc);
      for (const c of chaos.values()) if (c.cx && c.cx.pid === q.pid) c.send({ t: 'ev', a: 'pts', pts: rec.pts, n });
    }
    evBroadcast({ t: 'ev', a: 'rend', sid: e.sid, win: !!e.win, top: top ? { name: top.name, dmg: Math.round(top.dmg) } : null, n: P.length });
    log('RUPTURA ' + (e.win ? 'VENCIDA' : 'terminou sem vitória') + ' · ' + P.length + ' participantes' + (top ? ' · maior dano: ' + top.name : ''));
    evHook('legendEnd', e, { win: !!e.win, top });
  }
};
function evRup(p, m) {                    // dano causado pelo jogador às criaturas: [[id, dano], ...]
  const e = [...ev.active.values()].find(x => x.id === 'ruptura'); if (!p.cx || !e || (m.sid | 0) !== e.sid || e.win || !Array.isArray(m.h)) return;
  const now = Date.now(), el = now - (e.endsAt - e.def.durMs), pid = p.cx.pid, tk = e.tok.get(pid) || { v: RUP.dmgBurst, t: now };
  tk.v = Math.min(RUP.dmgBurst, tk.v + (now - tk.t) / 1000 * RUP.dmgPerSec); tk.t = now; e.tok.set(pid, tk);
  for (const [id, d0] of m.h.slice(0, 8)) {
    const c = e.data.cr.find(x => x.id === (id | 0)), d = Number(d0); if (!c || !(d > 0) || !Number.isFinite(d) || e.dead.has(c.id) || el < c.sp + RUP.spawnGraceMs) continue;
    const ok = Math.min(d, tk.v, e.hp.get(c.id)); if (ok <= 0) continue; tk.v -= ok;
    e.hp.set(c.id, e.hp.get(c.id) - ok); e.dirty.add(c.id);
    const q = e.part.get(pid) || { pid, acc: p.cx.acc, name: p.name, dmg: 0 }; q.dmg += ok; e.part.set(pid, q);
    if (e.hp.get(c.id) <= 0.5) {
      e.dead.add(c.id); evBroadcast({ t: 'ev', a: 'rdie', sid: e.sid, id: c.id, by: p.name });
      if (c.k === 'colosso') { e.win = 1; e.endsAt = Math.min(e.endsAt, now + RUP.closeMs); for (const o of e.data.cr) e.dead.add(o.id); evBroadcast({ t: 'ev', a: 'rwin', sid: e.sid, by: p.name, left: e.endsAt - now }); }
    }
  }
}
setInterval(() => {                       // devolve a vida das criaturas que mudaram (4 vezes por segundo)
  const e = [...ev.active.values()].find(x => x.id === 'ruptura'); if (!e || !e.dirty || !e.dirty.size) return;
  const h = [...e.dirty].filter(id => !e.dead.has(id)).map(id => [id, Math.max(1, Math.round(e.hp.get(id)))]); e.dirty.clear();
  if (h.length) evBroadcast({ t: 'ev', a: 'rhp', sid: e.sid, h });
}, 250).unref();

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
