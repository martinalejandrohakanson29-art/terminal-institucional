// Prueba standalone del WS de BingX (trade stream) — confirma el framing gzip + ping/pong
// antes de integrarlo al adapter real. No toca server.js.
// Uso: node scripts/test-bingx-ws.js

const WebSocket = require('ws');
const zlib = require('zlib');

const URL = 'wss://open-api-swap.bingx.com/swap-market';
const ws = new WebSocket(URL);

let received = 0;
const MAX_MSGS = 5;

ws.on('open', () => {
  console.log('WS conectado, suscribiendo a BTC-USDT@trade...');
  ws.send(JSON.stringify({ id: 'test-1', reqType: 'sub', dataType: 'BTC-USDT@trade' }));
});

ws.on('message', (data) => {
  let text;
  try {
    text = zlib.gunzipSync(data).toString('utf-8');
  } catch (e) {
    console.log('No se pudo gunzip, tratando como texto plano:', data.toString().slice(0, 100));
    return;
  }

  if (text === 'Ping') {
    ws.send('Pong');
    console.log('<- Ping recibido, -> Pong enviado');
    return;
  }

  console.log('Mensaje:', text.slice(0, 300));
  received++;
  if (received >= MAX_MSGS) {
    console.log(`\nOK: recibidos ${MAX_MSGS} mensajes de trade reales. Cerrando.`);
    ws.close();
    process.exit(0);
  }
});

ws.on('error', (e) => console.error('ERROR WS:', e.message));
ws.on('close', () => console.log('WS cerrado.'));

setTimeout(() => {
  console.log('Timeout de 20s sin suficientes mensajes — revisar.');
  process.exit(1);
}, 20000);
