# OWL AirDrop 🦅

Transferência de arquivos P2P entre dispositivos na mesma rede local, no estilo AirDrop/PairDrop.  
Nenhum arquivo passa pelo servidor — transferência direta via **WebRTC**.

## Stack

| Camada | Tecnologia |
|--------|-----------|
| Servidor de sinalização | Node.js + `ws` + Express |
| Transferência | WebRTC RTCDataChannel |
| Frontend | HTML + CSS + JS puro |
| Deploy | Nginx reverse proxy + HTTPS |

## Como funciona

1. Dispositivos abrem `https://owlfilms.pro/airdrop`
2. Conectam ao servidor de sinalização via WebSocket (`/airdrop-ws`)
3. Servidor agrupa dispositivos pela faixa `/24` do IP local
4. Cada dispositivo recebe um nome aleatório (ex: "Golfinho Azul")
5. Clique ou arraste arquivos sobre o card de outro dispositivo
6. Receptor aceita/recusa via modal
7. Arquivos são transferidos em chunks de 64 KB via DataChannel

## Instalação

```bash
# 1. Instalar dependências
npm install

# 2. Rodar localmente (desenvolvimento)
npm start
# → http://localhost:3000/airdrop

# 3. Ou com watch mode (Node ≥ 18)
npm run dev
```

## Deploy (VPS + Nginx)

```bash
# No servidor
git clone <repo> /opt/owl-airdrop
cd /opt/owl-airdrop
npm install --production

# Instalar PM2 para manter o servidor rodando
npm install -g pm2
pm2 start server.js --name owl-airdrop
pm2 save
pm2 startup

# Configurar Nginx
sudo cp nginx.conf.example /etc/nginx/sites-available/owlfilms
sudo ln -s /etc/nginx/sites-available/owlfilms /etc/nginx/sites-enabled/
sudo nginx -t && sudo nginx -s reload
```

## Variáveis de ambiente

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `PORT`   | `3000` | Porta do servidor Node.js |

## Estrutura

```
AIRDROP/
├── server.js              # Servidor de sinalização WebSocket
├── package.json
├── nginx.conf.example     # Configuração Nginx de exemplo
├── public/
│   ├── index.html         # SPA principal
│   ├── style.css          # Design escuro responsivo
│   └── app.js             # Lógica WebRTC + UI
```

## Recursos

- ✅ Detecção automática de peers na mesma rede Wi-Fi
- ✅ Nomes gerados automaticamente (sem cadastro)
- ✅ Transferência P2P real (servidor apenas faz relay do handshake)
- ✅ Múltiplos arquivos por vez, em fila
- ✅ Barra de progresso por arquivo
- ✅ Aceitar / Recusar transferências
- ✅ Drag & Drop sobre o ícone do destinatário
- ✅ Layout responsivo (desktop + mobile)
- ✅ Chunking de 64 KB com controle de backpressure

## Limitações (fora do MVP)

- Sem TURN server → não funciona entre redes diferentes
- Sem histórico de transferências
- Sem autenticação
