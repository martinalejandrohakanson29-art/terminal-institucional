// Adapter BingX — mismo contrato que binance.js, pero con las diferencias reales
// confirmadas contra la API en vivo (ver scripts/test-bingx.js y test-bingx-ws.js):
//   - símbolo con guion: BTC-USDT
//   - klines NO traen taker_buy_base (order flow) → se acumula aparte desde el WS de trades
//   - no hay endpoint público de históricos de Open Interest → solo se acumula hacia adelante
//   - WS de mercado: un solo feed, mensajes gzip, requiere responder "Pong" a "Ping"
const zlib = require('zlib');
const crypto = require('crypto');
const WebSocket = require('ws');

const SYMBOL = 'BTC-USDT';
const WS_URL = 'wss://open-api-swap.bingx.com/swap-market';
const DUR_MS = { '1m': 60000, '5m': 300000, '15m': 900000, '1h': 3600000 };
// Confirmado contra /openApi/swap/v2/quote/contracts (ver scripts/test-bingx.js):
// tradeMinQuantity 0.0001, tradeMinUSDT 2, pricePrecision 1 (tick 0.1, igual que Binance).
const QTY_STEP = 0.0001;
const MIN_NOTIONAL_USDT = 2;
const PRICE_STEP = 0.1;
// A diferencia de Binance (testnet.binancefuture.com), BingX SÍ tiene dominio demo propio
// con fondos VST (confirmado en la doc: "Apply VST" — demo domain open-api-vst.bingx.com).
const BASE_REAL    = 'https://open-api.bingx.com';
const BASE_DEMO_VST = 'https://open-api-vst.bingx.com';

async function fetchKlines({ interval, limit, endTime }) {
    const dur = DUR_MS[interval] || 60000;
    const params = new URLSearchParams({ symbol: SYMBOL, interval, limit: String(limit) });
    if (endTime) params.set('endTime', String(endTime));
    const url = `https://open-api.bingx.com/openApi/swap/v3/quote/klines?${params}`;
    const r = await fetch(url);
    const j = await r.json();
    if (!j || j.code !== 0 || !Array.isArray(j.data)) {
        throw new Error(j && j.msg ? j.msg : `respuesta inesperada de BingX klines (${JSON.stringify(j).slice(0, 100)})`);
    }
    // Normalizado al mismo array shape que Binance klines:
    // [openTime, open, high, low, close, volume, closeTime, _, _, takerBuyBase]
    // BingX no da closeTime ni takerBuyBase → closeTime se deriva, takerBuyBase queda null
    // (se completa después acumulando el WS de trades — ver necesitaAcumularDelta).
    return j.data.map(k => {
        const openTime = Number(k.time);
        return [openTime, k.open, k.high, k.low, k.close, k.volume, openTime + dur - 1, 0, 0, null];
    });
}

async function fetchOpenInterest() {
    const r = await fetch(`https://open-api.bingx.com/openApi/swap/v2/quote/openInterest?symbol=${SYMBOL}`);
    const j = await r.json();
    if (!j || j.code !== 0 || !j.data || !j.data.openInterest) {
        throw new Error(j && j.msg ? j.msg : 'respuesta inesperada de BingX openInterest');
    }
    return parseFloat(j.data.openInterest);
}

// BingX no documenta un endpoint público de histórico de Open Interest (a diferencia de
// Binance futures/data/openInterestHist). No hay forma de backfillear el pasado: el filtro
// de OI en BingX solo tendrá cobertura desde que este collector empezó a correr.
async function fetchOpenInterestHist() {
    return [];
}

function gunzip(data) {
    return zlib.gunzipSync(data).toString('utf-8');
}

// Conecta el feed único de trades de BingX (perp) y normaliza cada trade al mismo
// shape que usa el cazador de ballenas de Binance: { price, qty, isSell }.
// Devuelve el WebSocket crudo para que el watchdog externo pueda hacer .terminate().
function connectTrades({ onOpen, onTrade, onClose, onError }) {
    const ws = new WebSocket(WS_URL);

    ws.on('open', () => {
        ws.send(JSON.stringify({ id: 'ballenas-bingx', reqType: 'sub', dataType: `${SYMBOL}@trade` }));
        if (onOpen) onOpen();
    });

    ws.on('message', (data) => {
        let text;
        try {
            text = gunzip(data);
        } catch (e) {
            return; // frame no comprimido/corrupto — se ignora, el watchdog reconecta si el feed queda mudo
        }
        if (text === 'Ping') {
            ws.send('Pong');
            return;
        }
        let msg;
        try {
            msg = JSON.parse(text);
        } catch (e) {
            return;
        }
        if (!Array.isArray(msg.data)) return;
        for (const t of msg.data) {
            onTrade({ price: parseFloat(t.p), qty: parseFloat(t.q), isSell: t.m === true, ts: Number(t.T) });
        }
    });

    ws.on('error', (err) => { if (onError) onError(err); });
    ws.on('close', () => { if (onClose) onClose(); });

    return ws;
}

const whaleFeeds = [
    { mercado: 'PERP', connect: connectTrades },
];

// BingX no expone taker_buy_base en klines: hay que acumularlo minuto a minuto desde
// el WS de trades (ver connectTrades) y volcarlo aparte a klines_1m.
const necesitaAcumularDelta = true;

// ── Ejecución (fase 2) ──────────────────────────────────────────────────
// NO VALIDADO CONTRA UNA CUENTA REAL DE BINGX (ni siquiera demo/VST) — se implementó
// a partir de la documentación oficial confirmada (ver hallazgos en la conversación:
// balance en /user/balance, posición en /user/positions con positionAmt, orden en
// /trade/order con positionSide/reduceOnly, params firmados ORDENADOS ALFABÉTICAMENTE
// a diferencia de Binance). Probar con tamaño mínimo en el dominio demo VST antes de
// cualquier cuenta real.
function buildSignedQuery(secret, params = {}) {
    const all = { ...params, timestamp: Date.now() };
    const base = Object.keys(all).sort().map(k => `${k}=${all[k]}`).join('&');
    const signature = crypto.createHmac('sha256', secret).update(base).digest('hex');
    return `${base}&signature=${signature}`;
}

async function request(ctx, method, path, params = {}) {
    const url = `${ctx.base}${path}?${buildSignedQuery(ctx.secret, params)}`;
    try {
        const r = await fetch(url, { method, headers: { 'X-BX-APIKEY': ctx.apiKey } });
        const body = await r.json();
        return { ok: r.ok && body && body.code === 0, body };
    } catch (e) {
        return { ok: false, error: e.message };
    }
}

async function getBalance(ctx) {
    const { ok, body, error } = await request(ctx, 'GET', '/openApi/swap/v3/user/balance');
    if (!ok) throw new Error(error || (body && body.msg) || 'no se pudo leer balance BingX');
    const data = body.data;
    const asset = Array.isArray(data) ? data.find(a => a.asset === 'USDT') : data;
    if (!asset) throw new Error('sin balance USDT en la respuesta de BingX');
    return { wallet: parseFloat(asset.balance), disponible: parseFloat(asset.availableMargin) };
}

async function getPositionAmt(ctx) {
    const { ok, body, error } = await request(ctx, 'GET', '/openApi/swap/v2/user/positions', { symbol: SYMBOL });
    if (!ok) throw new Error(error || (body && body.msg) || 'positions no disponible en BingX');
    const rows = Array.isArray(body.data) ? body.data : [];
    const pos = rows.find(p => p.symbol === SYMBOL);
    return pos ? parseFloat(pos.positionAmt) : 0;
}

function redondearPrecio(precio) {
    return Math.round(precio / PRICE_STEP) * PRICE_STEP;
}

async function ejecutarOrden(ctx, params, etiqueta) {
    const r = await request(ctx, 'POST', '/openApi/swap/v2/trade/order', params);
    if (r.ok) console.log(`[BingX u${ctx.uid}] ✅ Orden ${etiqueta} — orderId: ${r.body?.data?.order?.orderId ?? '—'}`);
    else console.error(`[BingX u${ctx.uid}] ❌ Orden ${etiqueta} rechazada — ${r.body?.code}: ${r.body?.msg || r.error}`);
    return r;
}

// positionSide: 'BOTH' — requerido por BingX en todas las órdenes; asume modo One-way
// (forzado en asegurarConfiguracionCuenta con dualSidePosition:false). En Hedge Mode
// tendría que ser 'LONG'/'SHORT' según el lado de la posición, no del side de la orden.
async function placeEntryOrder(ctx, { lado, qty }) {
    const r = await ejecutarOrden(ctx, {
        symbol: SYMBOL, type: 'MARKET', side: lado === 'long' ? 'BUY' : 'SELL', positionSide: 'BOTH', quantity: qty,
    }, 'ENTRADA');
    const o = r.body?.data?.order;
    return { ok: r.ok, body: r.body, avgPrice: o ? parseFloat(o.avgPrice) : null, orderId: o ? String(o.orderId) : null };
}

async function placeCloseOrder(ctx, { lado, qty }) {
    return ejecutarOrden(ctx, {
        symbol: SYMBOL, type: 'MARKET', side: lado === 'long' ? 'SELL' : 'BUY', positionSide: 'BOTH', quantity: qty, reduceOnly: 'true',
    }, 'CIERRE');
}

async function placeStopOrder(ctx, { lado, qty, stopPrice, tipo }) {
    const r = await ejecutarOrden(ctx, {
        symbol: SYMBOL, type: tipo, side: lado === 'long' ? 'SELL' : 'BUY', positionSide: 'BOTH', quantity: qty,
        stopPrice: redondearPrecio(stopPrice), reduceOnly: 'true', workingType: 'MARK_PRICE',
    }, `${tipo}`);
    const o = r.body?.data?.order;
    return { ok: r.ok, body: r.body, orderId: o ? String(o.orderId) : null };
}

async function cancelOrder(ctx, orderId) {
    if (!orderId) return;
    const r = await request(ctx, 'DELETE', '/openApi/swap/v2/trade/order', { symbol: SYMBOL, orderId });
    if (!r.ok) console.error(`[BingX u${ctx.uid}] Error cancelando orden ${orderId}: ${r.body?.msg || r.error}`);
}

async function cancelAllOrders(ctx) {
    const r = await request(ctx, 'DELETE', '/openApi/swap/v2/trade/allOpenOrders', { symbol: SYMBOL });
    if (!r.ok) console.error(`[BingX u${ctx.uid}] Error cancelando todas las órdenes: ${r.body?.msg || r.error}`);
}

// BingX trackea el leverage por separado para LONG y SHORT (incluso en modo One-way) — el
// endpoint NO acepta side:'BOTH' (confirmado contra la doc oficial: "side" solo admite
// LONG/SHORT). Hay que pegarle dos veces para dejar ambos lados igual.
async function setLeverage(ctx, leverage) {
    const lev = Math.round(leverage);
    const [rLong, rShort] = await Promise.all([
        request(ctx, 'POST', '/openApi/swap/v2/trade/leverage', { symbol: SYMBOL, side: 'LONG', leverage: lev }),
        request(ctx, 'POST', '/openApi/swap/v2/trade/leverage', { symbol: SYMBOL, side: 'SHORT', leverage: lev }),
    ]);
    const r = { ok: rLong.ok && rShort.ok };
    if (r.ok) console.log(`[BingX u${ctx.uid}] Leverage ${lev}x (LONG+SHORT) ✅`);
    else console.warn(`[BingX u${ctx.uid}] ⚠️ No se pudo fijar leverage: LONG ${rLong.body?.code}/${rLong.body?.msg} · SHORT ${rShort.body?.code}/${rShort.body?.msg}`);
    return r.ok;
}

// Errores tratados como "ya está así" (idempotente) — igual que Binance con -4059/-4046.
// No confirmado contra la API real: revisar el código exacto la primera vez que dispare.
async function asegurarConfiguracionCuenta(ctx) {
    try {
        const r = await request(ctx, 'POST', '/openApi/swap/v1/positionSide/dual', { dualSidePosition: 'false' });
        if (r.ok || r.body?.code === 80014) console.log(`[BingX u${ctx.uid}] Modo de posición: One-way ✅`);
        else console.warn(`[BingX u${ctx.uid}] ⚠️ No se pudo fijar One-way: ${r.body?.code} ${r.body?.msg}`);
    } catch (e) { console.error(`[BingX u${ctx.uid}] Error modo de posición:`, e.message); }

    try {
        const mt = ctx.marginType || 'ISOLATED';
        const r = await request(ctx, 'POST', '/openApi/swap/v2/trade/marginType', { symbol: SYMBOL, marginType: mt });
        if (r.ok) console.log(`[BingX u${ctx.uid}] Margin type: ${mt} ✅`);
        else console.warn(`[BingX u${ctx.uid}] ⚠️ No se pudo fijar margin type: ${r.body?.code} ${r.body?.msg}`);
    } catch (e) { console.error(`[BingX u${ctx.uid}] Error margin type:`, e.message); }
}

const trade = {
    baseReal: BASE_REAL,
    baseDemo: BASE_DEMO_VST,
    qtyStep: QTY_STEP,
    minNotionalUsdt: MIN_NOTIONAL_USDT,
    priceStep: PRICE_STEP,
    getBalance,
    getPositionAmt,
    placeEntryOrder,
    placeCloseOrder,
    placeStopOrder,
    cancelOrder,
    cancelAllOrders,
    setLeverage,
    asegurarConfiguracionCuenta,
    redondearPrecio,
};

module.exports = {
    name: 'bingx',
    symbol: SYMBOL,
    fetchKlines,
    fetchOpenInterest,
    fetchOpenInterestHist,
    whaleFeeds,
    necesitaAcumularDelta,
    trade,
};
