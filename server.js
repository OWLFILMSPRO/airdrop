/**
 * OWL AirDrop — Signaling Server
 * Node.js + Express + ws
 *
 * Suporta dois modos via variável de ambiente BASE_PATH:
 *
 *   BASE_PATH=/        → serve em airdrop.owlfilms.pro/
 *   BASE_PATH=/airdrop → serve em owlfilms.pro/airdrop  (padrão)
 *
 * WebSocket path: <BASE_PATH>/ws   (ex: /ws ou /airdrop/ws)
 * Peers são agrupados pela faixa /24 do IP — só se veem dentro
 * da mesma rede local. Nenhum dado de arquivo passa pelo servidor.
 */

'use strict';

const express = require('express');
const http    = require('http');
const { WebSocketServer } = require('ws');
const { v4: uuidv4 }      = require('uuid');
const path    = require('path');
const fs      = require('fs');

// ─── Base path config ─────────────────────────────────────────
// Remove trailing slash, keep leading slash
// Examples: '/'→'' (root),  '/airdrop'→'/airdrop'
const RAW_BASE = process.env.BASE_PATH ?? '/airdrop';
const BASE     = RAW_BASE.replace(/\/$/, '');  // '' or '/airdrop'
const WS_PATH  = `${BASE}/ws` || '/ws';        // '/ws' or '/airdrop/ws'

console.log(`  BASE_PATH : "${BASE || '/'}"`);
console.log(`  WS_PATH   : "${WS_PATH}"`);

// ─── App & HTTP server ────────────────────────────────────────
const app    = express();
const server = http.createServer(app);

// ─── Serve index.html (com config injetada) ──────────────────
const INDEX_HTML = path.join(__dirname, 'public', 'index.html');

function serveIndex (res) {
  let html = fs.readFileSync(INDEX_HTML, 'utf8');
  // Injeta configuração de runtime antes do </head>
  const config = `<script>
  window.__OWL__ = {
    wsPath  : ${JSON.stringify(WS_PATH)},
    basePath: ${JSON.stringify(BASE || '/')}
  };
</script>`;
  html = html.replace('</head>', config + '\n</head>');
  res.type('html').send(html);
}

// ─── Rotas HTTP ───────────────────────────────────────────────
// index:false → Express nunca serve index.html diretamente;
// o serveIndex() injeta o window.__OWL__ config antes de enviar.
const staticOpts = { index: false };

if (BASE) {
  // Modo subpath: /airdrop
  app.get(BASE,       (_req, res) => serveIndex(res));
  app.get(BASE + '/', (_req, res) => serveIndex(res));
  app.use(BASE, express.static(path.join(__dirname, 'public'), staticOpts));
  // Redireciona raiz → subpath
  app.get('/', (_req, res) => res.redirect(301, BASE));
} else {
  // Modo raiz: /
  app.get('/',  (_req, res) => serveIndex(res));
  app.use('/', express.static(path.join(__dirname, 'public'), staticOpts));
}

// ─── Name generation ─────────────────────────────────────────
const ADJECTIVES = [
  'Âmbar','Ártico','Audaz','Azul','Brilhante','Bravo','Calmo','Céltico',
  'Claro','Corajoso','Cristal','Dourado','Elétrico','Épico','Esmeralda',
  'Feroz','Furtivo','Gelado','Glorioso','Grande','Imperial','Índigo',
  'Infinito','Jovial','Lunar','Mágico','Majestoso','Nobre','Neon',
  'Oceânico','Ousado','Prata','Profundo','Rápido','Reluzente','Rubi',
  'Safira','Selvagem','Sereno','Sideral','Silencioso','Solar','Sônico',
  'Tempestuoso','Turquesa','Veloz','Vibrante','Vívido','Zênite'
];
const ANIMALS = [
  'Águia','Alce','Axolote','Baleia','Boto','Búfalo','Camaleão','Chita',
  'Cobra','Condor','Corvo','Crocodilo','Dragão','Falcão','Flamingo',
  'Golfinho','Gorila','Guepardo','Iguana','Jaguar','Lince',
  'Leopardo','Leão','Lobo','Manta','Narval','Orca','Ornitorrinco',
  'Pantera','Panda','Peixe-Espada','Pelicano','Puma','Python','Raia',
  'Rinoceronte','Salamandra','Serpente','Tigre','Touro','Tucano',
  'Urso','Veado','Viper','Wolverine','Zebra'
];

function generateName () {
  const adj    = ADJECTIVES[Math.floor(Math.random() * ADJECTIVES.length)];
  const animal = ANIMALS  [Math.floor(Math.random() * ANIMALS.length)];
  return `${adj} ${animal}`;
}

// ─── IP / subnet helpers ──────────────────────────────────────
function getClientIP (req) {
  const fwd = req.headers['x-forwarded-for'];
  if (fwd) return fwd.split(',')[0].trim();
  return req.socket.remoteAddress || '0.0.0.0';
}

function getSubnet (ip) {
  if (ip === '::1' || ip === '127.0.0.1' || ip === 'localhost') return 'localhost';
  const plain = ip.replace(/^::ffff:/, '');
  const parts = plain.split('.');
  if (parts.length === 4) return parts.slice(0, 3).join('.');
  return ip; // IPv6: group by full address
}

// ─── Peer registry ────────────────────────────────────────────
/** @type {Map<string, {ws: WebSocket, displayName: string, subnet: string}>} */
const peers = new Map();

function peersInSubnet (subnet) {
  const list = [];
  for (const [id, p] of peers) {
    if (p.subnet === subnet) list.push({ peerId: id, displayName: p.displayName });
  }
  return list;
}

function broadcast (subnet, msg, excludeId = null) {
  const data = JSON.stringify(msg);
  for (const [id, p] of peers) {
    if (p.subnet === subnet && id !== excludeId && p.ws.readyState === 1) {
      p.ws.send(data);
    }
  }
}

// ─── WebSocket signaling ──────────────────────────────────────
const wss = new WebSocketServer({ server, path: WS_PATH });

// Keepalive ping a cada 25s para evitar timeout no Render free tier
const PING_INTERVAL = 25_000;
const pingTimer = setInterval(() => {
  wss.clients.forEach(ws => {
    if (ws.isAlive === false) { ws.terminate(); return; }
    ws.isAlive = false;
    ws.ping();
  });
}, PING_INTERVAL);
wss.on('close', () => clearInterval(pingTimer));

wss.on('connection', (ws, req) => {
  const peerId      = uuidv4();
  const ip          = getClientIP(req);
  const subnet      = getSubnet(ip);
  const displayName = generateName();

  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  peers.set(peerId, { ws, displayName, subnet });
  console.log(`[+] "${displayName}" (${peerId.slice(0,8)}) | ip=${ip} | subnet=${subnet}`);

  // Welcome + peers existentes
  ws.send(JSON.stringify({
    type        : 'welcome',
    peerId,
    displayName,
    peers       : peersInSubnet(subnet).filter(p => p.peerId !== peerId)
  }));

  // Avisa peers existentes
  broadcast(subnet, { type: 'peer-joined', peerId, displayName }, peerId);

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (!peers.has(peerId)) return;

    switch (msg.type) {
      case 'signal':
      case 'transfer-request':
      case 'transfer-response': {
        const target = peers.get(msg.to);
        if (target && target.ws.readyState === 1) {
          target.ws.send(JSON.stringify({ ...msg, from: peerId }));
        }
        break;
      }
    }
  });

  const onDisconnect = () => {
    if (!peers.has(peerId)) return;
    peers.delete(peerId);
    console.log(`[-] "${displayName}" (${peerId.slice(0,8)}) disconnected`);
    broadcast(subnet, { type: 'peer-left', peerId });
  };

  ws.on('close', onDisconnect);
  ws.on('error', err => { console.error(`[!] WS error for "${displayName}":`, err.message); onDisconnect(); });
});

// ─── Start ────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log('');
  console.log('  🦅 OWL AirDrop — Signaling Server');
  console.log(`  ─────────────────────────────────────`);
  console.log(`  Local  : http://localhost:${PORT}${BASE || '/'}`);
  console.log(`  WS     : ws://localhost:${PORT}${WS_PATH}`);
  console.log(`  Prod   : https://airdrop.owlfilms.pro${BASE || '/'}`);
  console.log('');
});
