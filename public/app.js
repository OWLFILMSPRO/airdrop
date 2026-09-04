/* ═══════════════════════════════════════════════════════════════
   OWL AirDrop — Frontend App
   Pure JS: WebSocket signaling + WebRTC P2P file transfer
   ═══════════════════════════════════════════════════════════════ */

'use strict';

// ─── Constants ────────────────────────────────────────────────
const CHUNK_SIZE        = 64 * 1024;          // 64 KB per DataChannel send
const BUFFER_THRESHOLD  = 1 * 1024 * 1024;    // pause sends above 1 MB buffered
const BUFFER_RESUME     = 256 * 1024;         // resume below 256 KB
const WS_RECONNECT_MS   = 3_000;
const ICE_SERVERS       = [
  { urls: 'stun:stun.l.google.com:19302' },
  { urls: 'stun:stun.l.google.com:53' },
  { urls: 'stun:stun.l.google.com:80' },
  { urls: 'stun:global.stun.twilio.com:3478' }
];

// Peer icons — chosen deterministically by name
const ICONS = ['🐬','🦊','🐺','🦁','🐯','🦅','🦈','🐙','🦋','🦎',
               '🐧','🦜','🐻','🦍','🦒','🐘','🦔','🦩','🐊','🦕'];

// ─── App state ────────────────────────────────────────────────
let myPeerId     = null;
let myName       = null;

/** @type {Map<string, {displayName: string, element: HTMLElement}>} */
const knownPeers = new Map();

/** @type {Map<string, WebRTCConnection>} */
const activeConns = new Map();

// ═══════════════════════════════════════════════════════════════
// Utility
// ═══════════════════════════════════════════════════════════════
function formatBytes (bytes) {
  if (bytes === 0) return '0 B';
  if (bytes < 1024)               return `${bytes} B`;
  if (bytes < 1024 ** 2)          return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 ** 3)          return `${(bytes / 1024 ** 2).toFixed(1)} MB`;
  return `${(bytes / 1024 ** 3).toFixed(2)} GB`;
}

function peerIcon (name) {
  let hash = 0;
  for (let i = 0; i < name.length; i++) hash = (hash * 31 + name.charCodeAt(i)) >>> 0;
  return ICONS[hash % ICONS.length];
}

// ═══════════════════════════════════════════════════════════════
// Signaling Client (WebSocket → server)
// ═══════════════════════════════════════════════════════════════
class SignalingClient extends EventTarget {
  constructor () {
    super();
    this._ws            = null;
    this._reconnectTimer = null;
    this._connect();
  }

  _connect () {
    const proto  = location.protocol === 'https:' ? 'wss:' : 'ws:';
    // wsPath injetado pelo server.js: '/ws' (root) ou '/airdrop/ws' (subpath)
    const wsPath = (window.__OWL__ && window.__OWL__.wsPath) || '/airdrop/ws';
    const url    = `${proto}//${location.host}${wsPath}`;

    this._ws = new WebSocket(url);

    this._ws.onopen = () => {
      clearTimeout(this._reconnectTimer);
      this.dispatchEvent(new Event('open'));
    };

    this._ws.onclose = () => {
      this.dispatchEvent(new Event('close'));
      this._reconnectTimer = setTimeout(() => this._connect(), WS_RECONNECT_MS);
    };

    this._ws.onerror = () => {/* close fires after, handled there */};

    this._ws.onmessage = ({ data }) => {
      try {
        const msg = JSON.parse(data);
        this.dispatchEvent(new CustomEvent('msg', { detail: msg }));
      } catch (e) {
        console.error('[WS] parse error', e);
      }
    };
  }

  send (obj) {
    if (this._ws && this._ws.readyState === WebSocket.OPEN) {
      this._ws.send(JSON.stringify(obj));
    }
  }
}

// ═══════════════════════════════════════════════════════════════
// WebRTC Connection
// ═══════════════════════════════════════════════════════════════
/**
 * Wraps one RTCPeerConnection + RTCDataChannel pair.
 *
 * Events dispatched (CustomEvent.detail varies):
 *   'channel-open'    — DataChannel is ready
 *   'file-start'      — { name, size, mimeType, fileIndex, totalFiles }
 *   'file-chunk'      — { chunk: ArrayBuffer, received, total }
 *   'file-done'       — { blob, info }
 *   'transfer-done'   — all files finished
 *   'send-progress'   — { sent, total, fileName, fileIndex, totalFiles }
 *   'state-change'    — RTCPeerConnection.connectionState string
 */
class WebRTCConnection extends EventTarget {
  /**
   * @param {string}           peerId
   * @param {SignalingClient}  signaling
   * @param {boolean}          initiator  true = creates offer + DataChannel
   */
  constructor (peerId, signaling, initiator) {
    super();
    this.peerId    = peerId;
    this.sig       = signaling;
    this.initiator = initiator;

    this.pc      = null;
    this.channel = null;

    // Receive bookkeeping
    this._rxBuf      = [];
    this._rxSize     = 0;
    this._rxFileInfo = null;

    this._init();
  }

  // ── Setup ─────────────────────────────────────────────────
  _init () {
    this.pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });

    this.pc.onicecandidate = ({ candidate }) => {
      if (candidate) {
        this.sig.send({ type: 'signal', to: this.peerId,
                        data: { candidate } });
      }
    };

    this.pc.onconnectionstatechange = () => {
      const s = this.pc.connectionState;
      console.log(`[WebRTC] ${this.peerId.slice(0,8)} → ${s}`);
      this.dispatchEvent(new CustomEvent('state-change', { detail: s }));
    };

    if (this.initiator) {
      this.channel = this.pc.createDataChannel('owl-files', { ordered: true });
      this._bindChannel(this.channel);
    } else {
      this.pc.ondatachannel = ({ channel }) => {
        this.channel = channel;
        this._bindChannel(channel);
      };
    }
  }

  _bindChannel (ch) {
    ch.binaryType = 'arraybuffer';

    ch.onopen  = () => {
      console.log(`[DataChannel] open with ${this.peerId.slice(0,8)}`);
      this.dispatchEvent(new Event('channel-open'));
    };
    ch.onclose = () => console.log(`[DataChannel] closed`);
    ch.onerror = e  => console.error(`[DataChannel] error`, e);

    ch.onmessage = ({ data }) => {
      if (typeof data === 'string') {
        this._handleControl(JSON.parse(data));
      } else {
        this._handleBinary(data);
      }
    };
  }

  // ── Receive side ──────────────────────────────────────────
  _handleControl (msg) {
    switch (msg.type) {
      case 'file-start':
        this._rxBuf      = [];
        this._rxSize     = 0;
        this._rxFileInfo = msg;
        this.dispatchEvent(new CustomEvent('file-start', { detail: msg }));
        break;

      case 'file-done': {
        const blob = new Blob(this._rxBuf, { type: this._rxFileInfo?.mimeType || '' });
        this.dispatchEvent(new CustomEvent('file-done', {
          detail: { blob, info: this._rxFileInfo }
        }));
        this._rxBuf  = [];
        this._rxSize = 0;
        break;
      }

      case 'transfer-done':
        this.dispatchEvent(new Event('transfer-done'));
        break;
    }
  }

  _handleBinary (chunk) {
    this._rxBuf.push(chunk);
    this._rxSize += chunk.byteLength;
    this.dispatchEvent(new CustomEvent('file-chunk', {
      detail: {
        chunk,
        received : this._rxSize,
        total    : this._rxFileInfo?.size ?? 0,
      }
    }));
  }

  // ── Signaling ─────────────────────────────────────────────
  async createOffer () {
    const offer = await this.pc.createOffer();
    await this.pc.setLocalDescription(offer);
    this.sig.send({ type: 'signal', to: this.peerId,
                    data: { sdp: this.pc.localDescription } });
  }

  async handleSignal ({ sdp, candidate }) {
    if (sdp) {
      await this.pc.setRemoteDescription(new RTCSessionDescription(sdp));
      if (sdp.type === 'offer') {
        const answer = await this.pc.createAnswer();
        await this.pc.setLocalDescription(answer);
        this.sig.send({ type: 'signal', to: this.peerId,
                        data: { sdp: this.pc.localDescription } });
      }
    } else if (candidate) {
      try {
        await this.pc.addIceCandidate(new RTCIceCandidate(candidate));
      } catch (e) {
        console.warn('[ICE] addIceCandidate failed', e.message);
      }
    }
  }

  // ── Send side ─────────────────────────────────────────────
  /** @param {FileList|File[]} files */
  async sendFiles (files) {
    const list = Array.from(files);

    for (let i = 0; i < list.length; i++) {
      await this._sendOneFile(list[i], i, list.length);
    }

    this.channel.send(JSON.stringify({ type: 'transfer-done' }));
  }

  async _sendOneFile (file, index, total) {
    this.channel.send(JSON.stringify({
      type      : 'file-start',
      name      : file.name,
      size      : file.size,
      mimeType  : file.type || 'application/octet-stream',
      fileIndex : index,
      totalFiles: total,
    }));

    let offset = 0;
    while (offset < file.size) {
      // Respect DataChannel buffer to avoid dropping chunks
      await this._drainBuffer();

      const slice  = file.slice(offset, offset + CHUNK_SIZE);
      const buffer = await slice.arrayBuffer();
      this.channel.send(buffer);
      offset += buffer.byteLength;

      this.dispatchEvent(new CustomEvent('send-progress', {
        detail: {
          sent      : Math.min(offset, file.size),
          total     : file.size,
          fileName  : file.name,
          fileIndex : index,
          totalFiles: total,
        }
      }));
    }

    this.channel.send(JSON.stringify({ type: 'file-done', fileIndex: index }));
  }

  _drainBuffer () {
    return new Promise(resolve => {
      if (this.channel.bufferedAmount < BUFFER_THRESHOLD) {
        resolve(); return;
      }
      const check = () => {
        if (this.channel.bufferedAmount <= BUFFER_RESUME) resolve();
        else setTimeout(check, 30);
      };
      setTimeout(check, 30);
    });
  }

  // ── Teardown ──────────────────────────────────────────────
  close () {
    try { this.channel?.close(); } catch {}
    try { this.pc?.close();      } catch {}
  }
}

// ═══════════════════════════════════════════════════════════════
// UI helpers
// ═══════════════════════════════════════════════════════════════
const UI = {
  // ── DOM refs ─────────────────────────────────────────────
  $  : id => document.getElementById(id),
  peersGrid : null,

  init () {
    this.peersGrid = this.$('peers-grid');
  },

  // ── Own device ───────────────────────────────────────────
  setMyName (name) {
    this.$('my-name').textContent = name;
  },

  setConnected (ok) {
    const dot = this.$('connection-dot');
    dot.className = 'dot ' + (ok ? 'connected' : 'disconnected');
  },

  showStatus (msg, isError = false) {
    const el = this.$('status-msg');
    el.textContent   = msg;
    el.className     = isError ? 'error' : '';
    el.style.display = msg ? 'block' : 'none';
  },

  // ── Peer cards ───────────────────────────────────────────
  createPeerCard (peerId, displayName) {
    const card    = document.createElement('div');
    card.className       = 'peer-card';
    card.dataset.peerId  = peerId;

    const icon = peerIcon(displayName);

    card.innerHTML = `
      <div class="peer-avatar" role="button" tabindex="0"
           aria-label="Enviar para ${displayName}">
        <span class="peer-icon">${icon}</span>
        <div  class="peer-ripple" aria-hidden="true"></div>
      </div>
      <div class="peer-name">${displayName}</div>
      <input type="file" class="file-input" multiple aria-hidden="true"
             tabindex="-1" style="display:none">
    `;

    const avatar    = card.querySelector('.peer-avatar');
    const fileInput = card.querySelector('.file-input');

    // Click / keyboard → open picker
    const openPicker = () => fileInput.click();
    avatar.addEventListener('click', openPicker);
    avatar.addEventListener('keydown', e => {
      if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); openPicker(); }
    });

    fileInput.addEventListener('change', () => {
      if (fileInput.files.length) {
        startTransfer(peerId, fileInput.files);
        fileInput.value = '';
      }
    });

    // Drag & Drop
    avatar.addEventListener('dragenter', e => { e.preventDefault(); card.classList.add('drag-over'); });
    avatar.addEventListener('dragover',  e => { e.preventDefault(); });
    avatar.addEventListener('dragleave', e => {
      if (!card.contains(e.relatedTarget)) card.classList.remove('drag-over');
    });
    avatar.addEventListener('drop', e => {
      e.preventDefault();
      card.classList.remove('drag-over');
      if (e.dataTransfer.files.length) startTransfer(peerId, e.dataTransfer.files);
    });

    this.peersGrid.appendChild(card);
    // Animate in next frame
    requestAnimationFrame(() => requestAnimationFrame(() => card.classList.add('visible')));
    this.updateEmpty();
    return card;
  },

  removePeerCard (card) {
    card.classList.add('leaving');
    card.addEventListener('transitionend', () => { card.remove(); this.updateEmpty(); }, { once: true });
  },

  updateEmpty () {
    this.$('empty-state').style.display = knownPeers.size === 0 ? 'flex' : 'none';
  },

  // ── Toasts ───────────────────────────────────────────────
  toast (msg, type = 'info', duration = 3500) {
    const wrap  = this.$('toast-container');
    const el    = document.createElement('div');
    el.className   = `toast toast-${type}`;
    el.textContent = msg;
    wrap.appendChild(el);
    requestAnimationFrame(() => requestAnimationFrame(() => el.classList.add('show')));
    setTimeout(() => {
      el.classList.remove('show');
      el.addEventListener('transitionend', () => el.remove(), { once: true });
    }, duration);
  },

  // ── Receive confirm modal ─────────────────────────────────
  showReceivePrompt (fromName, files, callback) {
    const modal   = this.$('receive-modal');
    this.$('receive-from').textContent = fromName;

    const listEl = this.$('receive-file-list');
    listEl.innerHTML = '';
    let total = 0;
    files.forEach(f => {
      total += f.size;
      const li = document.createElement('li');
      li.textContent = `${f.name}  (${formatBytes(f.size)})`;
      listEl.appendChild(li);
    });
    this.$('receive-total-size').textContent = `Total: ${formatBytes(total)}`;

    modal.classList.add('open');

    const close = accepted => {
      modal.classList.remove('open');
      this.$('receive-accept').onclick = null;
      this.$('receive-reject').onclick = null;
      callback(accepted);
    };

    this.$('receive-accept').onclick = () => close(true);
    this.$('receive-reject').onclick = () => close(false);
  },

  // ── Receive progress modal ────────────────────────────────
  openReceiveProgress (fromName) {
    this.$('rp-from').textContent = fromName;
    this.$('receive-progress-modal').classList.add('open');
  },

  updateReceiveProgress (fileName, received, total, fileIndex, totalFiles) {
    const pct = total > 0 ? Math.round(received / total * 100) : 0;
    this.$('rp-filename').textContent = fileName;
    this.$('rp-bar').style.width      = `${pct}%`;
    this.$('rp-bytes').textContent    = `${formatBytes(received)} / ${formatBytes(total)}`;
    this.$('rp-counter').textContent  = `Arquivo ${fileIndex + 1} de ${totalFiles}`;
  },

  closeReceiveProgress () {
    this.$('receive-progress-modal').classList.remove('open');
  },

  // ── Send progress modal ───────────────────────────────────
  openSendProgress (toName) {
    this.$('sp-to').textContent  = toName;
    this.$('sp-bar').style.width = '0%';
    this.$('sp-filename').textContent = '…';
    this.$('sp-bytes').textContent    = '';
    this.$('send-progress-modal').classList.add('open');
  },

  updateSendProgress (fileName, sent, total, fileIndex, totalFiles) {
    const pct = total > 0 ? Math.round(sent / total * 100) : 0;
    this.$('sp-bar').style.width    = `${pct}%`;
    this.$('sp-filename').textContent = fileName;
    this.$('sp-bytes').textContent    =
      `${formatBytes(sent)} / ${formatBytes(total)}  ·  Arquivo ${fileIndex + 1}/${totalFiles}`;
  },

  closeSendProgress () {
    this.$('send-progress-modal').classList.remove('open');
  },
};

// ═══════════════════════════════════════════════════════════════
// Transfer orchestration
// ═══════════════════════════════════════════════════════════════

/**
 * Attach event listeners to a WebRTCConnection for both sender and receiver.
 * @param {WebRTCConnection} conn
 * @param {string}           peerId
 * @param {'send'|'recv'}    role
 * @param {FileList}         [filesToSend]  only when role==='send'
 */
function wireConnection (conn, peerId, role, filesToSend) {
  const peer     = knownPeers.get(peerId);
  const peerName = peer?.displayName ?? 'Dispositivo';

  // ── Receive side ─────────────────────────────────────────
  if (role === 'recv') {
    let currentFileIndex = 0, currentFileTotalFiles = 1;

    conn.addEventListener('file-start', e => {
      const { name, size, fileIndex, totalFiles } = e.detail;
      currentFileIndex     = fileIndex;
      currentFileTotalFiles = totalFiles;
      UI.openReceiveProgress(peerName);
      UI.updateReceiveProgress(name, 0, size, fileIndex, totalFiles);
    });

    conn.addEventListener('file-chunk', e => {
      const { received, total } = e.detail;
      const info = conn._rxFileInfo;
      UI.updateReceiveProgress(
        info?.name ?? '', received, total,
        currentFileIndex, currentFileTotalFiles
      );
    });

    conn.addEventListener('file-done', e => {
      const { blob, info } = e.detail;
      triggerDownload(blob, info.name);
      UI.toast(`✅ ${info.name} recebido!`, 'success');
    });

    conn.addEventListener('transfer-done', () => {
      UI.closeReceiveProgress();
      activeConns.delete(peerId);
      conn.close();
    });
  }

  // ── Send side ─────────────────────────────────────────────
  if (role === 'send') {
    conn.addEventListener('channel-open', async () => {
      UI.openSendProgress(peerName);
      await conn.sendFiles(filesToSend);
      UI.closeSendProgress();
      UI.toast('✅ Todos os arquivos enviados!', 'success');
      activeConns.delete(peerId);
    });

    conn.addEventListener('send-progress', e => {
      const { sent, total, fileName, fileIndex, totalFiles } = e.detail;
      UI.updateSendProgress(fileName, sent, total, fileIndex, totalFiles);
    });
  }

  // ── Shared: connection failures ──────────────────────────
  conn.addEventListener('state-change', e => {
    if (e.detail === 'failed' || e.detail === 'closed') {
      activeConns.delete(peerId);
      UI.closeSendProgress();
      UI.closeReceiveProgress();
      if (e.detail === 'failed') UI.toast('❌ Conexão P2P falhou.', 'error');
    }
  });
}

/**
 * Initiate a file transfer to a remote peer.
 * @param {string}   peerId
 * @param {FileList} files
 */
async function startTransfer (peerId, files) {
  if (!files?.length || !myPeerId) return;
  if (activeConns.has(peerId)) {
    UI.toast('Já existe uma transferência em andamento.', 'warning'); return;
  }

  const fileInfos = Array.from(files).map(f => ({
    name: f.name, size: f.size, type: f.type
  }));

  // Build the RTCPeerConnection now (initiator), but don't create offer yet.
  // The offer is created after the receiver accepts.
  const conn = new WebRTCConnection(peerId, signaling, true);
  wireConnection(conn, peerId, 'send', files);
  activeConns.set(peerId, conn);

  // Ask server to relay transfer-request to the target peer
  signaling.send({ type: 'transfer-request', to: peerId, files: fileInfos });
  UI.toast('⏳ Aguardando confirmação…', 'info', 5000);
}

function triggerDownload (blob, name) {
  const a  = document.createElement('a');
  a.href   = URL.createObjectURL(blob);
  a.download = name;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  setTimeout(() => URL.revokeObjectURL(a.href), 30_000);
}

// ═══════════════════════════════════════════════════════════════
// Signaling message dispatcher
// ═══════════════════════════════════════════════════════════════
const signaling = new SignalingClient();

signaling.addEventListener('open', () => {
  UI.setConnected(true);
  UI.showStatus('');
  console.log('[WS] connected');
});

signaling.addEventListener('close', () => {
  UI.setConnected(false);
  UI.showStatus('Desconectado — reconectando…', true);
  // Clear peers on disconnect (they'll be resent on reconnect)
  for (const [id, peer] of knownPeers) {
    UI.removePeerCard(peer.element);
    knownPeers.delete(id);
  }
});

signaling.addEventListener('msg', ({ detail: msg }) => {
  switch (msg.type) {

    // ── Server sends our identity + list of existing peers ──
    case 'welcome': {
      myPeerId = msg.peerId;
      myName   = msg.displayName;
      UI.setMyName(myName);
      UI.showStatus('');
      msg.peers.forEach(p => addPeer(p.peerId, p.displayName));
      console.log(`[me] ${myName} (${myPeerId.slice(0,8)})`);
      break;
    }

    case 'peer-joined':
      addPeer(msg.peerId, msg.displayName);
      UI.toast(`📡 ${msg.displayName} entrou na rede`, 'info', 2500);
      break;

    case 'peer-left':
      removePeer(msg.peerId);
      break;

    // ── WebRTC signal relay (offer/answer/ICE) ──
    case 'signal': {
      let conn = activeConns.get(msg.from);
      if (!conn) {
        // We are the receiver side — create non-initiator connection
        conn = new WebRTCConnection(msg.from, signaling, false);
        wireConnection(conn, msg.from, 'recv');
        activeConns.set(msg.from, conn);
      }
      conn.handleSignal(msg.data).catch(e =>
        console.error('[signal] handleSignal error', e)
      );
      break;
    }

    // ── Incoming transfer request: show accept/reject modal ──
    case 'transfer-request': {
      const peer     = knownPeers.get(msg.from);
      const fromName = peer?.displayName ?? 'Dispositivo desconhecido';

      UI.showReceivePrompt(fromName, msg.files, accepted => {
        signaling.send({ type: 'transfer-response', to: msg.from, accepted });
        if (!accepted) {
          UI.toast('Transferência recusada.', 'info', 2000);
        }
      });
      break;
    }

    // ── Sender gets accept/reject answer ──
    case 'transfer-response': {
      const conn = activeConns.get(msg.from);
      if (!conn) break;

      if (msg.accepted) {
        // Now initiate the WebRTC handshake
        conn.createOffer().catch(e =>
          console.error('[webrtc] createOffer error', e)
        );
      } else {
        UI.toast('❌ Transferência recusada pelo destinatário.', 'error');
        conn.close();
        activeConns.delete(msg.from);
        UI.closeSendProgress();
      }
      break;
    }

    default:
      console.warn('[WS] unknown message type:', msg.type);
  }
});

// ═══════════════════════════════════════════════════════════════
// Peer management
// ═══════════════════════════════════════════════════════════════
function addPeer (peerId, displayName) {
  if (knownPeers.has(peerId)) return;
  const el = UI.createPeerCard(peerId, displayName);
  knownPeers.set(peerId, { displayName, element: el });
}

function removePeer (peerId) {
  const peer = knownPeers.get(peerId);
  if (!peer) return;
  UI.removePeerCard(peer.element);
  knownPeers.delete(peerId);
  UI.toast(`${peer.displayName} saiu da rede`, 'info', 2000);
  // Clean up any active WebRTC connection
  const conn = activeConns.get(peerId);
  if (conn) { conn.close(); activeConns.delete(peerId); }
}

// ═══════════════════════════════════════════════════════════════
// Boot
// ═══════════════════════════════════════════════════════════════
UI.init();
UI.updateEmpty();
UI.showStatus('Conectando…');
