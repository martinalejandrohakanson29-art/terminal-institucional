// Adapter Binance — envuelve los mismos endpoints que server.js usaba directo,
// para que el resto del código sea agnóstico de a qué exchange le está hablando.
const crypto = require('crypto');

const SYMBOL = 'BTCUSDT';
const QTY_STEP = 0.001;
const MIN_NOTIONAL_USDT = 100;
const PRICE_STEP = 0.1; // BTCUSDT perp tick size
const BASE_REAL = 'https://fapi.binance.com';
const BASE_DEMO = 'https://testnet.binancefuture.com';

async function fetchKlines({ interval, limit, endTime }) {
    const url = `https://api.binance.com/api/v3/klines?symbol=${SYMBOL}&interval=${interval}&limit=${limit}${endTime ? `&endTime=${endTime}` : ''}`;
    const r = await fetch(url);
    const j = await r.json();
    if (!Array.isArray(j)) throw new Error(j && j.msg ? j.msg : `respuesta inesperada de Binance klines (${JSON.stringify(j).slice(0, 100)})`);
    // Formato nativo Binance ya trae taker_buy_base en el índice 9 — no hace falta derivarlo.
    return j;
}

async function fetchOpenInterest() {
    const r = await fetch(`https://fapi.binance.com/fapi/v1/openInterest?symbol=${SYMBOL}`);
    const j = await r.json();
    if (!j || !j.openInterest) throw new Error('respuesta inesperada de Binance openInterest');
    return parseFloat(j.openInterest);
}

async function fetchOpenInterestHist({ period, limit, endTime }) {
    const url = `https://fapi.binance.com/futures/data/openInterestHist?symbol=${SYMBOL}&period=${period}&limit=${limit}&endTime=${endTime}`;
    const r = await fetch(url);
    const j = await r.json();
    if (!Array.isArray(j)) return [];
    // [{ sumOpenInterest, timestamp }] → normalizado { valor, timestamp }
    return j.map(d => ({ valor: parseFloat(d.sumOpenInterest), timestamp: Number(d.timestamp) }));
}

// El cazador de ballenas de Binance ya usaba dos feeds (spot + perp) con formato plano JSON,
// sin compresión. No necesita adapter propio de framing — se deja tal cual en server.js.
const whaleFeeds = [
    { url: 'wss://stream.binance.com:9443/ws/btcusdt@aggTrade', mercado: 'SPOT' },
    { url: 'wss://fstream.binance.com/ws/btcusdt@aggTrade', mercado: 'PERP' },
];

// Binance entrega taker_buy_base nativo en klines: no requiere acumulación propia de delta.
const necesitaAcumularDelta = false;

// ── Ejecución ────────────────────────────────────────────────────────────
// Idéntico al código que ya corría en producción en server.js — solo relocalizado
// acá para exponerlo con la misma interfaz que el adapter de BingX.
function firmarParams(params, secret) {
    return crypto.createHmac('sha256', secret).update(params).digest('hex');
}

async function ejecutarOrden(ctx, url, etiqueta) {
    try {
        const resp = await fetch(url, {
            method: 'POST',
            headers: { 'X-MBX-APIKEY': ctx.apiKey, 'Content-Type': 'application/x-www-form-urlencoded' },
        });
        const body = await resp.json();
        if (resp.ok) {
            console.log(`[Binance u${ctx.uid}] ✅ Orden ${etiqueta} — orderId: ${body.orderId ?? body.algoId ?? '—'}`);
        } else {
            console.error(`[Binance u${ctx.uid}] ❌ Orden ${etiqueta} rechazada — ${body.code}: ${body.msg}`);
        }
        return { ok: resp.ok, body };
    } catch (e) {
        console.error(`[Binance u${ctx.uid}] ❌ Error red orden ${etiqueta}: ${e.message}`);
        return { ok: false, error: e.message };
    }
}

async function getBalance(ctx) {
    const ts  = Date.now();
    const q   = `timestamp=${ts}&recvWindow=10000`;
    const url = `${ctx.base}/fapi/v2/balance?${q}&signature=${firmarParams(q, ctx.secret)}`;
    const r    = await fetch(url, { headers: { 'X-MBX-APIKEY': ctx.apiKey } });
    const body = await r.json();
    if (!Array.isArray(body)) throw new Error(body && body.msg ? body.msg : 'no se pudo leer balance');
    const usdt = body.find(b => b.asset === 'USDT');
    return {
        wallet:     usdt ? parseFloat(usdt.balance) : 0,
        disponible: usdt ? parseFloat(usdt.availableBalance) : 0,
    };
}

async function getPositionAmt(ctx) {
    const ts  = Date.now();
    const q   = `symbol=${SYMBOL}&timestamp=${ts}&recvWindow=10000`;
    const url = `${ctx.base}/fapi/v2/positionRisk?${q}&signature=${firmarParams(q, ctx.secret)}`;
    const r    = await fetch(url, { headers: { 'X-MBX-APIKEY': ctx.apiKey } });
    const body = await r.json();
    if (!Array.isArray(body)) throw new Error(body && body.msg ? body.msg : 'positionRisk no disponible');
    const pos = body.find(x => x.symbol === SYMBOL);
    return pos ? parseFloat(pos.positionAmt) : 0;
}

function redondearPrecio(precio) {
    return Math.round(precio / PRICE_STEP) * PRICE_STEP;
}

async function placeEntryOrder(ctx, { lado, qty }) {
    const side = lado === 'long' ? 'BUY' : 'SELL';
    const ts   = Date.now();
    const p    = `symbol=${SYMBOL}&side=${side}&type=MARKET&quantity=${qty}&timestamp=${ts}`;
    const url  = `${ctx.base}/fapi/v1/order?${p}&signature=${firmarParams(p, ctx.secret)}`;
    const r = await ejecutarOrden(ctx, url, 'ENTRADA');
    return { ok: r.ok, body: r.body, avgPrice: parseFloat(r.body?.avgPrice) || null, orderId: r.body?.orderId ? String(r.body.orderId) : null };
}

async function placeCloseOrder(ctx, { lado, qty }) {
    const side = lado === 'long' ? 'SELL' : 'BUY';
    const ts   = Date.now();
    const p    = `symbol=${SYMBOL}&side=${side}&type=MARKET&quantity=${qty}&reduceOnly=true&timestamp=${ts}`;
    const url  = `${ctx.base}/fapi/v1/order?${p}&signature=${firmarParams(p, ctx.secret)}`;
    return ejecutarOrden(ctx, url, 'CIERRE');
}

// Desde 2025-12-09 Binance migró las órdenes condicionales al servicio Algo: /fapi/v1/order
// las rechaza con -4120, así que van por /fapi/v1/algoOrder (algoType=CONDITIONAL,
// stopPrice → triggerPrice, la respuesta trae algoId en vez de orderId).
async function placeStopOrder(ctx, { lado, qty, stopPrice, tipo }) {
    const side = lado === 'long' ? 'SELL' : 'BUY';
    const sp   = redondearPrecio(stopPrice);
    const ts   = Date.now();
    const p    = `algoType=CONDITIONAL&symbol=${SYMBOL}&side=${side}&type=${tipo}&quantity=${qty}&triggerPrice=${sp}&reduceOnly=true&workingType=MARK_PRICE&timestamp=${ts}`;
    const url  = `${ctx.base}/fapi/v1/algoOrder?${p}&signature=${firmarParams(p, ctx.secret)}`;
    const r = await ejecutarOrden(ctx, url, tipo);
    const id = r.body?.algoId ?? r.body?.orderId;
    return { ok: r.ok, body: r.body, orderId: id ? String(id) : null };
}

async function cancelOrder(ctx, algoId) {
    if (!algoId) return;
    try {
        const ts  = Date.now();
        const q   = `symbol=${SYMBOL}&algoId=${algoId}&timestamp=${ts}`;
        const url = `${ctx.base}/fapi/v1/algoOrder?${q}&signature=${firmarParams(q, ctx.secret)}`;
        await fetch(url, { method: 'DELETE', headers: { 'X-MBX-APIKEY': ctx.apiKey } });
    } catch (e) { console.error(`[Binance u${ctx.uid}] Error cancelando orden ${algoId}: ${e.message}`); }
}

async function cancelAllOrders(ctx) {
    const ts = Date.now();
    const eliminar = async (endpoint) => {
        try {
            const q   = `symbol=${SYMBOL}&timestamp=${ts}`;
            const url = `${ctx.base}/${endpoint}?${q}&signature=${firmarParams(q, ctx.secret)}`;
            await fetch(url, { method: 'DELETE', headers: { 'X-MBX-APIKEY': ctx.apiKey } });
        } catch (e) { console.error(`[Binance u${ctx.uid}] Error cancelando ${endpoint}: ${e.message}`); }
    };
    await eliminar('fapi/v1/algoOpenOrders');
    await eliminar('fapi/v1/allOpenOrders');
}

async function setLeverage(ctx, leverage) {
    const ts = Date.now();
    const q  = `symbol=${SYMBOL}&leverage=${Math.round(leverage)}&timestamp=${ts}`;
    const url = `${ctx.base}/fapi/v1/leverage?${q}&signature=${firmarParams(q, ctx.secret)}`;
    const r = await ejecutarOrden(ctx, url, `LEVERAGE-${Math.round(leverage)}x`);
    return r.ok;
}

// Asegura One-way (no Hedge) y el margin type de la cuenta. Los códigos -4059 / -4046
// significan "no hace falta cambiar" y se tratan como éxito.
async function asegurarConfiguracionCuenta(ctx) {
    try {
        const ts  = Date.now();
        const q   = `dualSidePosition=false&timestamp=${ts}`;
        const url = `${ctx.base}/fapi/v1/positionSide/dual?${q}&signature=${firmarParams(q, ctx.secret)}`;
        const r   = await fetch(url, { method: 'POST', headers: { 'X-MBX-APIKEY': ctx.apiKey } });
        const b   = await r.json();
        if (r.ok || b.code === -4059) console.log(`[Binance u${ctx.uid}] Modo de posición: One-way ✅`);
        else console.warn(`[Binance u${ctx.uid}] ⚠️ No se pudo fijar One-way: ${b.code} ${b.msg}`);
    } catch (e) { console.error(`[Binance u${ctx.uid}] Error modo de posición:`, e.message); }

    try {
        const mt  = ctx.marginType || 'ISOLATED';
        const ts  = Date.now();
        const q   = `symbol=${SYMBOL}&marginType=${mt}&timestamp=${ts}`;
        const url = `${ctx.base}/fapi/v1/marginType?${q}&signature=${firmarParams(q, ctx.secret)}`;
        const r   = await fetch(url, { method: 'POST', headers: { 'X-MBX-APIKEY': ctx.apiKey } });
        const b   = await r.json();
        if (r.ok || b.code === -4046) console.log(`[Binance u${ctx.uid}] Margin type: ${mt} ✅`);
        else console.warn(`[Binance u${ctx.uid}] ⚠️ No se pudo fijar margin type: ${b.code} ${b.msg}`);
    } catch (e) { console.error(`[Binance u${ctx.uid}] Error margin type:`, e.message); }
}

const trade = {
    baseReal: BASE_REAL,
    baseDemo: BASE_DEMO,
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
    name: 'binance',
    symbol: SYMBOL,
    fetchKlines,
    fetchOpenInterest,
    fetchOpenInterestHist,
    whaleFeeds,
    necesitaAcumularDelta,
    trade,
};
