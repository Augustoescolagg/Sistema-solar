#!/usr/bin/env node
/*
  Servidor multiplayer (rede local) do Sistema Solar 3D.
  - Sem dependências: só precisa do Node.js 16+ (https://nodejs.org).
  - Serve o jogo (index.html) e faz o "relay" dos jogadores via WebSocket em /ws.

  Como usar:
    1) Coloque server.js e index.html na mesma pasta.
    2) Rode:   node server.js
    3) O terminal mostra os endereços, ex.:  http://192.168.0.10:3000
    4) Todo mundo na mesma rede Wi-Fi abre esse endereço no navegador.

  Opções (variáveis de ambiente):  PORT=3000  MAX_PLAYERS=16  HTML=caminho/do/arquivo.html
*/
'use strict';
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto'), os = require('os');

const PORT = +process.env.PORT || 3000;
const HTML = process.env.HTML || path.join(__dirname, 'index.html');
const MAX = +process.env.MAX_PLAYERS || 16;
const TICK_MS = 1000 / 15;               // frequência com que os estados são enviados aos jogadores
const SEED = crypto.randomInt(1, 2 ** 31); // semente do mundo: todos os clientes geram o MESMO mapa
const NCOLORS = 12;                      // cores de tripulante disponíveis (índice no cliente)
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

// ---------------------------------------------------------------- HTTP
const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/' || url === '/index.html') {
    fs.readFile(HTML, (err, data) => {
      if (err) { res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' }); res.end('index.html não encontrado ao lado do server.js'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
      res.end(data);
    });
  } else if (url === '/info') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ players: players.size, max: MAX, seed: SEED }));
  } else { res.writeHead(404, { 'Content-Type': 'text/plain' }); res.end('404'); }
});

// ---------------------------------------------------------------- WebSocket mínimo (RFC 6455)
class WS {
  constructor(sock) {
    this.sock = sock; this.buf = Buffer.alloc(0); this.open = true; this.frag = null; this.alive = Date.now();
    this.onmessage = null; this.onclose = null;
    sock.setNoDelay(true);
    sock.on('data', d => this._data(d));
    sock.on('close', () => this._end());
    sock.on('error', () => this._end());
  }
  _data(d) {
    this.alive = Date.now();
    this.buf = Buffer.concat([this.buf, d]);
    if (this.buf.length > (1 << 20)) return this.close(1009);
    for (;;) {
      const b = this.buf;
      if (b.length < 2) return;
      const fin = b[0] & 128, op = b[0] & 15, masked = b[1] & 128;
      let len = b[1] & 127, o = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); o = 4; }
      else if (len === 127) { if (b.length < 10) return; if (b.readUInt32BE(2)) return this.close(1009); len = b.readUInt32BE(6); o = 10; }
      if (len > 65536) return this.close(1009);
      if (!masked) return this.close(1002);             // frames de cliente precisam ser mascarados
      if (b.length < o + 4 + len) return;
      const mask = b.subarray(o, o + 4), pl = Buffer.allocUnsafe(len);
      for (let i = 0; i < len; i++) pl[i] = b[o + 4 + i] ^ mask[i & 3];
      this.buf = b.subarray(o + 4 + len);
      this._frame(op, fin, pl);
      if (!this.open) return;
    }
  }
  _frame(op, fin, pl) {
    if (op === 0x8) return this.close(1000);            // close
    if (op === 0x9) return this._write(0xA, pl);        // ping -> pong
    if (op === 0xA) return;                             // pong
    if (op === 0x1 || op === 0x2) { this.frag = [pl]; this.fop = op; }
    else if (op === 0x0 && this.frag) this.frag.push(pl);
    else return;
    if (fin) {
      const full = Buffer.concat(this.frag); this.frag = null;
      if (this.fop === 0x1 && this.onmessage) this.onmessage(full.toString('utf8'));
    }
  }
  _write(op, payload) {
    if (!this.open) return;
    const n = payload.length; let h;
    if (n < 126) h = Buffer.from([0x80 | op, n]);
    else if (n < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(n, 2); }
    else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeUInt32BE(0, 2); h.writeUInt32BE(n, 6); }
    try { this.sock.write(Buffer.concat([h, payload])); } catch (e) { this._end(); }
  }
  send(str) { this._write(0x1, Buffer.from(str, 'utf8')); }
  ping() { this._write(0x9, Buffer.alloc(0)); }
  close(code) {
    if (!this.open) return;
    const p = Buffer.alloc(2); p.writeUInt16BE(code || 1000, 0);
    this._write(0x8, p); this.open = false;
    try { this.sock.end(); } catch (e) {} this._end(true);
  }
  _end(already) {
    if (this.dead) return; this.dead = true; this.open = false;
    try { this.sock.destroy(); } catch (e) {}
    if (this.onclose) this.onclose();
  }
}

server.on('upgrade', (req, sock) => {
  const url = (req.url || '').split('?')[0], key = req.headers['sec-websocket-key'];
  if (url !== '/ws' || !key || (req.headers.upgrade || '').toLowerCase() !== 'websocket') { sock.destroy(); return; }
  const acc = crypto.createHash('sha1').update(key + GUID).digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + acc + '\r\n\r\n');
  attach(new WS(sock), req.socket.remoteAddress);
});

// ---------------------------------------------------------------- Jogadores
const players = new Map();   // id -> { id, ws, name, ci, st, dirty, tokens, lastChat }
let nextId = 1;

const clean = (s, n) => String(s == null ? '' : s).replace(/[\u0000-\u001f\u007f<>]/g, '').trim().slice(0, n);
const num = (v, lo, hi) => { v = +v; return Number.isFinite(v) ? Math.max(lo, Math.min(hi, v)) : 0; };
const bcast = (obj, except) => { const s = JSON.stringify(obj); for (const p of players.values()) if (p !== except && p.ws.open) p.ws.send(s); };

function freeColor(pref, self) {
  const used = new Set([...players.values()].filter(q => q.joined && q !== self).map(q => q.ci));
  if (Number.isInteger(pref) && pref >= 0 && pref < NCOLORS && !used.has(pref)) return pref;
  for (let i = 0; i < NCOLORS; i++) if (!used.has(i)) return i;
  return Math.floor(Math.random() * NCOLORS);
}

function attach(ws, ip) {
  if (players.size >= MAX) { ws.send(JSON.stringify({ k: 'full', max: MAX })); ws.close(1013); return; }
  const p = { id: nextId++, ws, name: '', ci: 0, st: null, dirty: false, tokens: 60, lastChat: 0, joined: false };
  players.set(p.id, p);

  ws.onmessage = txt => {
    let m; try { m = JSON.parse(txt); } catch (e) { return; }
    if (!m || typeof m.k !== 'string') return;
    if (p.tokens < 1) return; p.tokens--;            // limite de mensagens por segundo
    if (m.k === 'hello' && !p.joined) {
      p.name = clean(m.name, 14) || ('Astro' + p.id);
      p.ci = freeColor(m.ci, p); p.joined = true;
      ws.send(JSON.stringify({
        k: 'welcome', id: p.id, seed: SEED, ci: p.ci, name: p.name, max: MAX,
        players: [...players.values()].filter(q => q !== p && q.joined).map(q => ({ id: q.id, name: q.name, ci: q.ci }))
      }));
      bcast({ k: 'join', id: p.id, name: p.name, ci: p.ci }, p);
      console.log(`+ ${p.name} (#${p.id}) entrou de ${ip} — ${count()} online`);
    } else if (!p.joined) { return; }
    else if (m.k === 's') {                          // estado do jogador
      p.st = [
        p.id, m.p === 2 ? 2 : m.p === 1 ? 1 : 0, m.c ? 1 : 0,
        +num(m.x, -2000, 2000).toFixed(2), +num(m.y, -500, 500).toFixed(2), +num(m.z, -2000, 2000).toFixed(2),
        +num(m.f, -10, 10).toFixed(2), +num(m.s, 0, 60).toFixed(2), m.a ? 1 : 0, m.d ? 1 : 0, m.n ? 1 : 0, m.e ? 1 : 0
      ];
      p.dirty = true;
    } else if (m.k === 'chat') {
      const now = Date.now(), t = clean(m.m, 120);
      if (!t || now - p.lastChat < 600) return; p.lastChat = now;
      bcast({ k: 'chat', id: p.id, m: t });
      console.log(`  <${p.name}> ${t}`);
    } else if (m.k === 'emo') {
      bcast({ k: 'emo', id: p.id, e: Math.max(0, Math.min(7, m.e | 0)) }, p);
    }
  };
  ws.onclose = () => {
    if (!players.delete(p.id)) return;
    if (p.joined) { bcast({ k: 'leave', id: p.id }); console.log(`- ${p.name} (#${p.id}) saiu — ${count()} online`); }
  };
}
const count = () => [...players.values()].filter(p => p.joined).length;

// envia, a cada tick, o estado de todos para cada jogador (sem repetir o próprio)
setInterval(() => {
  for (const p of players.values()) p.tokens = Math.min(60, p.tokens + 40);   // recarrega o limite
  const live = [...players.values()].filter(p => p.joined && p.st);
  if (!live.some(p => p.dirty)) return;
  for (const to of players.values()) {
    if (!to.joined || !to.ws.open) continue;
    const arr = live.filter(p => p !== to).map(p => p.st);
    if (arr.length) to.ws.send(JSON.stringify({ k: 'snap', p: arr }));
  }
  for (const p of live) p.dirty = false;
}, TICK_MS);

// derruba conexões mortas (celular que dormiu, Wi-Fi que caiu)
setInterval(() => {
  const now = Date.now();
  for (const p of players.values()) {
    if (now - p.ws.alive > 45000) { p.ws.close(1001); continue; }
    p.ws.ping();
  }
}, 15000);

server.listen(PORT, '0.0.0.0', () => {
  console.log('\n🪐  Sistema Solar 3D — servidor multiplayer');
  console.log('    Semente do mundo: ' + SEED + '  |  máximo: ' + MAX + ' jogadores\n');
  console.log('    Abra no navegador (todos na mesma rede):');
  console.log('      Neste computador:  http://localhost:' + PORT);
  for (const list of Object.values(os.networkInterfaces()))
    for (const i of list || []) if (i.family === 'IPv4' && !i.internal) console.log('      Outros aparelhos:  http://' + i.address + ':' + PORT);
  console.log('\n    (se ninguém conseguir conectar, libere a porta ' + PORT + ' no firewall)\n');
});
