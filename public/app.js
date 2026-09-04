/* ═══════════════════════════════════════════════════════════════
   OWL AirDrop — Frontend App
   Pure JS: WebSocket signaling + WebRTC P2P file transfer
   ═══════════════════════════════════════════════════════════════ */

'use strict';

// ─── Constants ────────────────────────────────────────────────
const CHUNK_SIZE        = 64 * 1024;
const BUFFER_THRESHOLD  = 1 * 1024 * 1024;
const BUFFER_RESUME     = 256 * 1024;
const WS_RECONNECT_MS   = 3_000;

// Usando apenas STUN confiável do Google (igual ao PairDrop)
// A negociação será feita em background para evitar timeouts!
const ICE_SERVERS = [
  { urls: 'stun:stun.l.google.com:19302' },
  { urls: 'stun:stun1.l.google.com:19302' }
];

const ICONS = ['🐬','🦊','🐺','🦁','🐯','🦅','🦈','🐙','🦋','🦎',
               '🐧','🦜','🐻','🦍','🦒','🐘','🦔','🦩','🐊','🦕'];

let myPeerId  = null;
let myName    = null;

/** @type {Map<string, {displayName: string, element: HTMLElement}>} */
const knownPeers  = new Map();
/** @type {Map<string, WebRTCConnection>} */
const activeConns = new Map();

// ═══════════════════════════════════════════════════════════════
// Utility
// ═══════════════════════════════════════════════════════════════
function formatBytes (bytes) {
  if (bytes === 0) return '0 B';
  if (bytes < 1024)       return `${bytes} B`;
  if (bytes < 1024 ** 2)  return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 ** 3)  return `${(bytes / 1024 ** 2).toFixed(1)} MB`;
  return `${(bytes / 1024 ** 3).toFixed(2)} GB`;
}

function peerIcon (name) {
  let hash = 0;
  for (let i = 0; i < name.length; i++) hash = (hash * 31 + name.charCodeAt(i)) >>> 0;
  return ICONS[hash % ICONS.length];
}

// ═══════════════════════════════════════════════════════════════
// Signaling Client
// ═══════════════════════════════════════════════════════════════
class SignalingClient extends EventTarget {
  constructor () {
    super();
    this._ws             = null;
    this._reconnectTimer = null;
    this._connect();
  }

  _connect () {
    const proto  = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsPath = (window.__OWL__ && window.__OWL__.wsPath) || '/airdrop/ws';
    const url    = `${proto}//${location.host}${wsPath}`;
    this._ws = new WebSocket(url);

    this._ws.onopen    = () => { clearTimeout(this._reconnectTimer); this.dispatchEvent(new Event('open')); };
    this._ws.onclose   = () => { this.dispatchEvent(new Event('close')); this._reconnectTimer = setTimeout(() => this._connect(), WS_RECONNECT_MS); };
    this._ws.onerror   = () => {};
    this._ws.onmessage = ({ data }) => {
      try { this.dispatchEvent(new CustomEvent('msg', { detail: JSON.parse(data) })); }
      catch (e) { console.error('[WS] parse error', e); }
    };
  }

  send (obj) {
    if (this._ws && this._ws.readyState === WebSocket.OPEN)
      this._ws.send(JSON.stringify(obj));
  }
}

// ═══════════════════════════════════════════════════════════════
// WebRTC Connection
// ═══════════════════════════════════════════════════════════════
class WebRTCConnection extends EventTarget {
  constructor (peerId, signaling, initiator) {
    super();
    this.peerId    = peerId;
    this.sig       = signaling;
    this.initiator = initiator;
    this.pc        = null;
    this.channel   = null;

    // Receive bookkeeping
    this._rxBuf      = [];
    this._rxSize     = 0;
    this._rxFileInfo = null;

    this._pendingCandidates = [];
    this._remoteDescSet     = false;

    this._init();
  }

  _init () {
    this.pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });

    this.pc.onicecandidate = ({ candidate }) => {
      if (candidate)
        this.sig.send({ type: 'signal', to: this.peerId, data: { candidate } });
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
    ch.bufferedAmountLowThreshold = BUFFER_RESUME;

    ch.onopen  = () => { console.log(`[DC] open → ${this.peerId.slice(0,8)}`); this.dispatchEvent(new Event('channel-open')); };
    ch.onclose = () => console.log('[DC] closed');
    ch.onerror = e  => console.error('[DC] error', e);
    ch.onmessage = ({ data }) => {
      if (typeof data === 'string') this._handleControl(JSON.parse(data));
      else                          this._handleBinary(data);
    };
  }

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
        this.dispatchEvent(new CustomEvent('file-done', { detail: { blob, info: this._rxFileInfo } }));
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
      detail: { chunk, received: this._rxSize, total: this._rxFileInfo?.size ?? 0 }
    }));
  }

  async createOffer () {
    const offer = await this.pc.createOffer();
    await this.pc.setLocalDescription(offer);
    this.sig.send({ type: 'signal', to: this.peerId, data: { sdp: this.pc.localDescription } });
  }

  async handleSignal ({ sdp, candidate }) {
    if (sdp) {
      await this.pc.setRemoteDescription(new RTCSessionDescription(sdp));
      this._remoteDescSet = true;

      for (const c of this._pendingCandidates) {
        try { await this.pc.addIceCandidate(new RTCIceCandidate(c)); }
        catch (e) { console.warn('[ICE] queued candidate failed', e.message); }
      }
      this._pendingCandidates = [];

      if (sdp.type === 'offer') {
        const answer = await this.pc.createAnswer();
        await this.pc.setLocalDescription(answer);
        this.sig.send({ type: 'signal', to: this.peerId, data: { sdp: this.pc.localDescription } });
      }
    } else if (candidate) {
      if (this._remoteDescSet) {
        try { await this.pc.addIceCandidate(new RTCIceCandidate(candidate)); }
        catch (e) { console.warn('[ICE] addIceCandidate failed', e.message); }
      } else {
        this._pendingCandidates.push(candidate);
      }
    }
  }

  async sendFiles (files) {
    const list = Array.from(files);
    for (let i = 0; i < list.length; i++) await this._sendOneFile(list[i], i, list.length);
    this.channel.send(JSON.stringify({ type: 'transfer-done' }));
  }

  async _sendOneFile (file, index, total) {
    this.channel.send(JSON.stringify({
      type: 'file-start', name: file.name, size: file.size,
      mimeType: file.type || 'application/octet-stream',
      fileIndex: index, totalFiles: total,
    }));

    let offset = 0;
    while (offset < file.size) {
      await this._drainBuffer();
      const buffer = await file.slice(offset, offset + CHUNK_SIZE).arrayBuffer();
      this.channel.send(buffer);
      offset += buffer.byteLength;
      this.dispatchEvent(new CustomEvent('send-progress', {
        detail: { sent: Math.min(offset, file.size), total: file.size,
                  fileName: file.name, fileIndex: index, totalFiles: total }
      }));
    }
    this.channel.send(JSON.stringify({ type: 'file-done', fileIndex: index }));
  }

  _drainBuffer () {
    return new Promise(resolve => {
      if (this.channel.bufferedAmount < BUFFER_THRESHOLD) { resolve(); return; }
      const check = () => {
        if (this.channel.bufferedAmount <= BUFFER_RESUME) resolve();
        else setTimeout(check, 30);
      };
      setTimeout(check, 30);
    });
  }

  close () {
    try { this.channel?.close(); } catch {}
    try { this.pc?.close();      } catch {}
  }
}

// ═══════════════════════════════════════════════════════════════
// UI helpers
// ═══════════════════════════════════════════════════════════════
const UI = {
  $: id => document.getElementById(id),
  peersGrid: null,

  init () { this.peersGrid = this.$('peers-grid'); },
  setMyName (name) { this.$('my-name').textContent = name; },
  setConnected (ok) { this.$('connection-dot').className = 'dot ' + (ok ? 'connected' : 'disconnected'); },

  showStatus (msg, isError = false) {
    const el = this.$('status-msg');
    el.textContent   = msg;
    el.className     = isError ? 'error' : '';
    el.style.display = msg ? 'block' : 'none';
  },

  createPeerCard (peerId, displayName) {
    const card = document.createElement('div');
    card.className      = 'peer-card';
    card.dataset.peerId = peerId;
    card.innerHTML = `
      <div class="peer-avatar" role="button" tabindex="0" aria-label="Enviar para ${displayName}">
        <span class="peer-icon">${peerIcon(displayName)}</span>
        <div class="peer-ripple" aria-hidden="true"></div>
      </div>
      <div class="peer-name">${displayName}</div>
      <input type="file" class="file-input" multiple aria-hidden="true" tabindex="-1" style="display:none">
    `;

    const avatar    = card.querySelector('.peer-avatar');
    const fileInput = card.querySelector('.file-input');

    const openPicker = () => fileInput.click();
    avatar.addEventListener('click', openPicker);
    avatar.addEventListener('keydown', e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); openPicker(); } });
    fileInput.addEventListener('change', () => { if (fileInput.files.length) { startTransfer(peerId, fileInput.files); fileInput.value = ''; } });

    avatar.addEventListener('dragenter', e => { e.preventDefault(); card.classList.add('drag-over'); });
    avatar.addEventListener('dragover',  e => { e.preventDefault(); });
    avatar.addEventListener('dragleave', e => { if (!card.contains(e.relatedTarget)) card.classList.remove('drag-over'); });
    avatar.addEventListener('drop', e => { e.preventDefault(); card.classList.remove('drag-over'); if (e.dataTransfer.files.length) startTransfer(peerId, e.dataTransfer.files); });

    this.peersGrid.appendChild(card);
    requestAnimationFrame(() => requestAnimationFrame(() => card.classList.add('visible')));
    this.updateEmpty();
    return card;
  },

  removePeerCard (card) {
    card.classList.add('leaving');
    card.addEventListener('transitionend', () => { card.remove(); this.updateEmpty(); }, { once: true });
  },

  updateEmpty () { this.$('empty-state').style.display = knownPeers.size === 0 ? 'flex' : 'none'; },

  toast (msg, type = 'info', duration = 3500) {
    const el = document.createElement('div');
    el.className   = `toast toast-${type}`;
    el.textContent = msg;
    this.$('toast-container').appendChild(el);
    requestAnimationFrame(() => requestAnimationFrame(() => el.classList.add('show')));
    setTimeout(() => { el.classList.remove('show'); el.addEventListener('transitionend', () => el.remove(), { once: true }); }, duration);
  },

  // Auto-download tradicional + fallback de botão
  triggerDownload (fileName, blob) {
    const url = URL.createObjectURL(blob);
    
    // Tenta baixar automaticamente
    const a = document.createElement('a');
    a.href = url;
    a.download = fileName;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);

    // Mostra o toast com botão como fallback (caso o popup blocker bloqueie)
    const el = document.createElement('div');
    el.className = 'toast toast-success';
    el.style.cssText = 'display:flex;align-items:center;gap:10px;padding-right:8px;';
    el.innerHTML = `
      <span>✅ <strong>${fileName}</strong></span>
      <a href="${url}" download="${fileName}"
         style="background:#2ea043;color:#fff;padding:4px 12px;border-radius:6px;
                text-decoration:none;font-size:.85rem;white-space:nowrap;">⬇ Salvar</a>
    `;
    this.$('toast-container').appendChild(el);
    requestAnimationFrame(() => requestAnimationFrame(() => el.classList.add('show')));
    setTimeout(() => {
      el.classList.remove('show');
      el.addEventListener('transitionend', () => { el.remove(); URL.revokeObjectURL(url); }, { once: true });
    }, 60_000);
  },

  showReceivePrompt (fromName, files, callback) {
    const modal = this.$('receive-modal');
    this.$('receive-from').textContent = fromName;
    const listEl = this.$('receive-file-list');
    listEl.innerHTML = '';
    let total = 0;
    files.forEach(f => { total += f.size; const li = document.createElement('li'); li.textContent = `${f.name}  (${formatBytes(f.size)})`; listEl.appendChild(li); });
    this.$('receive-total-size').textContent = `Total: ${formatBytes(total)}`;
    modal.classList.add('open');
    const close = accepted => { modal.classList.remove('open'); this.$('receive-accept').onclick = null; this.$('receive-reject').onclick = null; callback(accepted); };
    this.$('receive-accept').onclick = () => close(true);
    this.$('receive-reject').onclick = () => close(false);
  },

  openReceiveProgress (fromName) { this.$('rp-from').textContent = fromName; this.$('receive-progress-modal').classList.add('open'); },

  updateReceiveProgress (fileName, received, total, fileIndex, totalFiles) {
    const pct = total > 0 ? Math.round(received / total * 100) : 0;
    this.$('rp-filename').textContent = fileName;
    this.$('rp-bar').style.width      = `${pct}%`;
    this.$('rp-bytes').textContent    = `${formatBytes(received)} / ${formatBytes(total)}`;
    this.$('rp-counter').textContent  = `Arquivo ${fileIndex + 1} de ${totalFiles}`;
  },

  closeReceiveProgress () { this.$('receive-progress-modal').classList.remove('open'); },

  openSendProgress (toName) {
    this.$('sp-to').textContent       = toName;
    this.$('sp-bar').style.width      = '0%';
    this.$('sp-filename').textContent = '…';
    this.$('sp-bytes').textContent    = '';
    this.$('send-progress-modal').classList.add('open');
  },

  updateSendProgress (fileName, sent, total, fileIndex, totalFiles) {
    const pct = total > 0 ? Math.round(sent / total * 100) : 0;
    this.$('sp-bar').style.width      = `${pct}%`;
    this.$('sp-filename').textContent = fileName;
    this.$('sp-bytes').textContent    = `${formatBytes(sent)} / ${formatBytes(total)}  ·  Arquivo ${fileIndex + 1}/${totalFiles}`;
  },

  closeSendProgress () { this.$('send-progress-modal').classList.remove('open'); },
};

// ═══════════════════════════════════════════════════════════════
// Transfer orchestration
// ═══════════════════════════════════════════════════════════════
function wireConnection (conn, peerId, role, filesToSend) {
  const peer     = knownPeers.get(peerId);
  const peerName = peer?.displayName ?? 'Dispositivo';

  if (role === 'recv') {
    let curIdx = 0, curTotal = 1;

    conn.addEventListener('file-start', e => {
      const { name, size, fileIndex, totalFiles } = e.detail;
      curIdx = fileIndex; curTotal = totalFiles;
      UI.openReceiveProgress(peerName);
      UI.updateReceiveProgress(name, 0, size, fileIndex, totalFiles);
    });

    conn.addEventListener('file-chunk', e => {
      const { received, total } = e.detail;
      UI.updateReceiveProgress(conn._rxFileInfo?.name ?? '', received, total, curIdx, curTotal);
    });

    conn.addEventListener('file-done', e => {
      UI.triggerDownload(e.detail.info.name, e.detail.blob);
    });

    conn.addEventListener('transfer-done', () => {
      UI.closeReceiveProgress();
      activeConns.delete(peerId);
      conn.close();
    });
  }

  if (role === 'send') {
    conn._isAccepted = false;
    conn._isOpen = false;

    const checkStart = async () => {
      if (conn._isAccepted && conn._isOpen && filesToSend) {
        UI.openSendProgress(peerName);
        await conn.sendFiles(filesToSend);
        filesToSend = null; // limpa após enviar
        UI.closeSendProgress();
        UI.toast('✅ Todos os arquivos enviados!', 'success');
        activeConns.delete(peerId);
      }
    };

    conn.addEventListener('channel-open', () => {
      conn._isOpen = true;
      checkStart();
    });

    conn.addEventListener('accepted', () => {
      conn._isAccepted = true;
      checkStart();
    });

    conn.addEventListener('send-progress', e => {
      const { sent, total, fileName, fileIndex, totalFiles } = e.detail;
      UI.updateSendProgress(fileName, sent, total, fileIndex, totalFiles);
    });
  }

  conn.addEventListener('state-change', e => {
    if (e.detail === 'failed') {
      activeConns.delete(peerId);
      UI.closeSendProgress();
      UI.closeReceiveProgress();
      UI.toast('❌ Conexão P2P falhou.', 'error');
    } else if (e.detail === 'closed') {
      activeConns.delete(peerId);
    }
  });
}

async function startTransfer (peerId, files) {
  if (!files?.length || !myPeerId) return;
  if (activeConns.has(peerId)) { UI.toast('Já existe uma transferência em andamento.', 'warning'); return; }
  
  const fileInfos = Array.from(files).map(f => ({ name: f.name, size: f.size, type: f.type }));
  const conn = new WebRTCConnection(peerId, signaling, true);
  wireConnection(conn, peerId, 'send', files);
  activeConns.set(peerId, conn);

  // Iniciar WebRTC *imediatamente* em background para evitar timeout de ICE, igual ao PairDrop!
  conn.createOffer().catch(e => console.error('[webrtc] offer error', e));

  // Pedir permissão ao usuário
  signaling.send({ type: 'transfer-request', to: peerId, files: fileInfos });
  UI.toast('⏳ Aguardando confirmação…', 'info', 5000);
}

// ═══════════════════════════════════════════════════════════════
// Signaling message dispatcher
// ═══════════════════════════════════════════════════════════════
const signaling = new SignalingClient();

signaling.addEventListener('open',  () => { UI.setConnected(true);  UI.showStatus(''); });
signaling.addEventListener('close', () => {
  UI.setConnected(false);
  UI.showStatus('Desconectado — reconectando…', true);
  for (const [id, peer] of knownPeers) { UI.removePeerCard(peer.element); knownPeers.delete(id); }
});

signaling.addEventListener('msg', ({ detail: msg }) => {
  switch (msg.type) {

    case 'welcome': {
      myPeerId = msg.peerId; myName = msg.displayName;
      UI.setMyName(myName); UI.showStatus('');
      msg.peers.forEach(p => addPeer(p.peerId, p.displayName));
      console.log(`[me] ${myName} (${myPeerId.slice(0,8)})`);
      break;
    }

    case 'peer-joined': addPeer(msg.peerId, msg.displayName); UI.toast(`📡 ${msg.displayName} entrou na rede`, 'info', 2500); break;
    case 'peer-left':   removePeer(msg.peerId); break;

    case 'signal': {
      let conn = activeConns.get(msg.from);
      if (!conn) {
        conn = new WebRTCConnection(msg.from, signaling, false);
        wireConnection(conn, msg.from, 'recv');
        activeConns.set(msg.from, conn);
      }
      conn.handleSignal(msg.data).catch(e => console.error('[signal] error', e));
      break;
    }

    case 'transfer-request': {
      const fromName = knownPeers.get(msg.from)?.displayName ?? 'Dispositivo desconhecido';
      UI.showReceivePrompt(fromName, msg.files, accepted => {
        signaling.send({ type: 'transfer-response', to: msg.from, accepted });
        if (!accepted) UI.toast('Transferência recusada.', 'info', 2000);
      });
      break;
    }

    case 'transfer-response': {
      const conn = activeConns.get(msg.from);
      if (!conn) break;
      if (msg.accepted) {
        // Dispara o evento de aceitação. Se o WebRTC já conectou em background, envia os arquivos agora!
        conn.dispatchEvent(new Event('accepted'));
      } else {
        UI.toast('❌ Transferência recusada pelo destinatário.', 'error');
        conn.close(); activeConns.delete(msg.from); UI.closeSendProgress();
      }
      break;
    }

    default: console.warn('[WS] unknown msg type:', msg.type);
  }
});

// ═══════════════════════════════════════════════════════════════
// Peer management
// ═══════════════════════════════════════════════════════════════
function addPeer (peerId, displayName) {
  if (knownPeers.has(peerId)) return;
  knownPeers.set(peerId, { displayName, element: UI.createPeerCard(peerId, displayName) });
}

function removePeer (peerId) {
  const peer = knownPeers.get(peerId);
  if (!peer) return;
  UI.removePeerCard(peer.element);
  knownPeers.delete(peerId);
  UI.toast(`${peer.displayName} saiu da rede`, 'info', 2000);
  const conn = activeConns.get(peerId);
  if (conn) { conn.close(); activeConns.delete(peerId); }
}

// ═══════════════════════════════════════════════════════════════
// Boot
// ═══════════════════════════════════════════════════════════════
UI.init();
UI.updateEmpty();
UI.showStatus('Conectando…');
