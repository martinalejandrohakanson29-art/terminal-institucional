// ============================================================
// MOTOR DE BACKTEST — núcleo compartido
// ============================================================
// Extraído de server.js para que el optimizador (scripts/optimizar.js) corra EXACTAMENTE
// el mismo motor que el backtest de /estrategias y la señal en vivo. Cualquier divergencia
// entre "lo que el optimizador encontró" y "lo que la UI muestra" sería un bug silencioso,
// así que hay una sola implementación y este archivo es su única fuente.

function calcEMA(values, period) {
    const k = 2 / (period + 1);
    const result = new Array(values.length).fill(null);
    if (values.length < period) return result;
    result[period - 1] = values.slice(0, period).reduce((s, v) => s + v, 0) / period;
    for (let i = period; i < values.length; i++) {
        result[i] = values[i] * k + result[i - 1] * (1 - k);
    }
    return result;
}

function calcRSI(closes, period = 14) {
    const p = period;
    const result = new Array(closes.length).fill(null);
    if (closes.length <= p) return result;
    let gSum = 0, lSum = 0;
    for (let i = 1; i <= p; i++) {
        const d = closes[i] - closes[i - 1];
        if (d > 0) gSum += d; else lSum -= d;
    }
    let avgG = gSum / p, avgL = lSum / p;
    result[p] = avgL === 0 ? 100 : 100 - 100 / (1 + avgG / avgL);
    for (let i = p + 1; i < closes.length; i++) {
        const d = closes[i] - closes[i - 1];
        avgG = (avgG * (p - 1) + (d > 0 ? d : 0)) / p;
        avgL = (avgL * (p - 1) + (d < 0 ? -d : 0)) / p;
        result[i] = avgL === 0 ? 100 : 100 - 100 / (1 + avgG / avgL);
    }
    return result;
}

function calcMACDArr(closes, fast = 12, slow = 26, signal = 9) {
    const emaF = calcEMA(closes, fast);
    const emaS = calcEMA(closes, slow);
    const macdLine = closes.map((_, i) =>
        emaF[i] !== null && emaS[i] !== null ? emaF[i] - emaS[i] : null
    );
    const signalLine = new Array(closes.length).fill(null);
    const firstIdx = macdLine.findIndex(v => v !== null);
    if (firstIdx < 0) return { macd: macdLine, signal: signalLine };
    const sigEMA = calcEMA(macdLine.slice(firstIdx), signal);
    sigEMA.forEach((v, i) => { signalLine[firstIdx + i] = v; });
    return { macd: macdLine, signal: signalLine };
}

function calcADX(highs, lows, closes, period = 14) {
    const n = closes.length;
    const result = new Array(n).fill(null);
    if (n < 2 * period + 1) return result;

    const tr = [], plusDM = [], minusDM = [];
    for (let i = 1; i < n; i++) {
        const up   = highs[i] - highs[i - 1];
        const down = lows[i - 1] - lows[i];
        plusDM.push(up > down && up > 0 ? up : 0);
        minusDM.push(down > up && down > 0 ? down : 0);
        tr.push(Math.max(highs[i] - lows[i], Math.abs(highs[i] - closes[i - 1]), Math.abs(lows[i] - closes[i - 1])));
    }

    let smTR = 0, smPDM = 0, smMDM = 0;
    for (let i = 0; i < period; i++) { smTR += tr[i]; smPDM += plusDM[i]; smMDM += minusDM[i]; }

    const dx = [];
    const pushDX = () => {
        const pdi = smTR > 0 ? (smPDM / smTR) * 100 : 0;
        const mdi = smTR > 0 ? (smMDM / smTR) * 100 : 0;
        const s = pdi + mdi;
        dx.push(s > 0 ? Math.abs(pdi - mdi) / s * 100 : 0);
    };
    pushDX();
    for (let i = period; i < tr.length; i++) {
        smTR  = smTR  - smTR  / period + tr[i];
        smPDM = smPDM - smPDM / period + plusDM[i];
        smMDM = smMDM - smMDM / period + minusDM[i];
        pushDX();
    }

    if (dx.length < period) return result;
    let adx = dx.slice(0, period).reduce((s, v) => s + v, 0) / period;
    result[2 * period - 1] = adx;
    for (let j = period; j < dx.length; j++) {
        adx = (adx * (period - 1) + dx[j]) / period;
        result[period + j] = adx;
    }
    return result;
}

// ATR de Wilder. Devuelve array alineado a los closes (null hasta calentar).
function calcATR(highs, lows, closes, period = 14) {
    const n = closes.length;
    const atr = new Array(n).fill(null);
    if (n <= period) return atr;
    const tr = new Array(n).fill(0);
    for (let i = 1; i < n; i++) {
        tr[i] = Math.max(
            highs[i] - lows[i],
            Math.abs(highs[i] - closes[i - 1]),
            Math.abs(lows[i]  - closes[i - 1])
        );
    }
    let a = 0;
    for (let i = 1; i <= period; i++) a += tr[i];
    a /= period;
    atr[period] = a;
    for (let i = period + 1; i < n; i++) { a = (a * (period - 1) + tr[i]) / period; atr[i] = a; }
    return atr;
}

// Pendiente de la EMA normalizada por ATR: slopeNorm = (ema[i] - ema[i-slopeBars]) / atr[i].
// Mismo cálculo que el indicador "Angulación EMA" del terminal (público/index.html).
function calcEMAangSlope(closes, highs, lows, emaLen = 200, atrLen = 14, slopeBars = 10) {
    const n = closes.length;
    const slope = new Array(n).fill(null);
    const ema = calcEMA(closes, emaLen);
    const atr = calcATR(highs, lows, closes, atrLen);
    for (let i = slopeBars; i < n; i++) {
        if (ema[i] == null || ema[i - slopeBars] == null || atr[i] == null || atr[i] === 0) continue;
        slope[i] = (ema[i] - ema[i - slopeBars]) / atr[i];
    }
    return slope;
}

function calcVWAP(bars, session = 'daily') {
    const n = bars.length;
    const result = new Array(n).fill(null);
    let cumPV = 0, cumV = 0, lastKey = null;
    for (let i = 0; i < n; i++) {
        const ts     = parseInt(bars[i][0]);
        const high   = parseFloat(bars[i][2]);
        const low    = parseFloat(bars[i][3]);
        const close  = parseFloat(bars[i][4]);
        const volume = parseFloat(bars[i][5]);
        const date   = new Date(ts);
        let key;
        if (session === 'weekly') {
            const dow = (date.getUTCDay() + 6) % 7; // Mon=0
            key = ts - (dow * 86400000) - (ts % 86400000);
        } else if (session === 'monthly') {
            key = `${date.getUTCFullYear()}-${date.getUTCMonth()}`;
        } else {
            key = `${date.getUTCFullYear()}-${date.getUTCMonth()}-${date.getUTCDate()}`;
        }
        if (key !== lastKey) { cumPV = 0; cumV = 0; lastKey = key; }
        const tp = (high + low + close) / 3;
        cumPV += tp * volume;
        cumV  += volume;
        result[i] = cumV > 0 ? cumPV / cumV : null;
    }
    return result;
}

// Agrega velas 1m en buckets más grandes (p.ej. 1h) en el mismo formato array que
// Binance. Se usa para temporalidades que no guardamos (la BD solo cachea 1m y deriva
// 5m/15m por SQL), así el resultado es idéntico con fuente BD o Binance. Igual que
// fetchKlinesDesdeBD, descarta el bucket EN CURSO: sus velas 1m están incompletas y
// los indicadores calculados sobre él cambiarían al re-evaluar un minuto después.
function agregarVelas1m(bars1m, bucketMs) {
    const out = [];
    let cur = null, curB = -1;
    for (const b of bars1m) {
        const bk = Math.floor(parseInt(b[0]) / bucketMs);
        // taker_buy_base null (BingX sin delta acumulado) no debe contaminar la suma del
        // bucket: se suma solo lo conocido, y si NINGUNA vela 1m tenía dato queda null.
        const tb = b[9] == null ? null : parseFloat(b[9]);
        if (bk !== curB) {
            if (cur) out.push(cur);
            curB = bk;
            cur = [bk * bucketMs, parseFloat(b[1]), parseFloat(b[2]), parseFloat(b[3]),
                   parseFloat(b[4]), parseFloat(b[5]), bk * bucketMs + bucketMs - 1, 0, 0, tb];
        } else {
            cur[2] = Math.max(cur[2], parseFloat(b[2]));
            cur[3] = Math.min(cur[3], parseFloat(b[3]));
            cur[4] = parseFloat(b[4]);
            cur[5] += parseFloat(b[5]);
            if (tb != null) cur[9] = (cur[9] ?? 0) + tb;
        }
    }
    if (cur) out.push(cur);
    const lastClose1m = bars1m.length ? parseInt(bars1m[bars1m.length - 1][6]) : 0;
    while (out.length && out[out.length - 1][6] > lastClose1m) out.pop();
    return out;
}

function lookupHTF(sortedTs, byTs, target) {
    let lo = 0, hi = sortedTs.length - 1, found = -1;
    while (lo <= hi) {
        const mid = (lo + hi) >> 1;
        if (sortedTs[mid] <= target) { found = mid; lo = mid + 1; } else hi = mid - 1;
    }
    return found >= 0 ? byTs.get(sortedTs[found]) : null;
}

function calcCapitalEntrada(p, capital, posicionesAbiertas) {
    let monto;
    if (p.posicionTipo === 'monto_fijo') {
        monto = p.posicionValor ?? capital;
    } else if (p.posicionTipo === 'porc_capital_inicial') {
        monto = p.initialCapital * ((p.posicionValor ?? 100) / 100);
    } else {
        // porc_capital_actual
        monto = capital * ((p.posicionValor ?? 100) / 100);
    }
    // Descontar lo ya asignado a posiciones abiertas para no sobre-exponer el capital.
    // Solo entrar si el disponible cubre el monto completo — sin posiciones parciales.
    const asignado = posicionesAbiertas.reduce((s, pos) => s + pos.capitalAtEntry, 0);
    const disponible = capital - asignado;
    return disponible >= monto ? monto : 0;
}

// Costo total de una operación como fracción del capital de la posición (margen):
// comisión y slippage se cobran en entrada + salida; el funding se prorratea por el
// tiempo en operación. Todo escala con la palanca porque se aplica sobre el nocional.
// El funding es DIRECCIONAL: funding positivo → longs pagan, shorts cobran (costo negativo).
// side = 1 (long) o -1 (short). Si se omite, se asume que se paga siempre (conservador).
function costoOperacion(p, palanca, minutesHeld, side = 1) {
    const fees = (p.commission   / 100) * 2 * palanca;
    const slip = (p.slippagePerc / 100) * 2 * palanca;
    // side * fundingPerc: long con funding positivo paga (+), short cobra (-).
    const fund = side * (p.fundingPerc / 100) * (Math.max(0, minutesHeld) / 480) * palanca;
    return fees + slip + fund;
}

function runBacktest(bars1m, bars5m, bars15m, whalesArr, p, oiArr, lsArr) {
    // Memoización opcional de indicadores. Solo la usa el optimizador (lib/optim/): al barrer
    // cientos de combinaciones sobre las MISMAS velas, la EMA 200 de 1m —o el ADX de 15m, o el
    // VWAP diario— se recalcula idéntica una y otra vez, y ese precómputo domina el costo por
    // corrida. `p._cache` es un Map con vida de una ventana de datos. Sin él, memo() ejecuta y
    // no guarda: el camino es byte a byte el de siempre, así que el backtest de la UI no cambia.
    const _cache = p._cache instanceof Map ? p._cache : null;
    const memo = (clave, calcular) => {
        if (!_cache) return calcular();
        if (!_cache.has(clave)) _cache.set(clave, calcular());
        return _cache.get(clave);
    };
    // Índice HTF (timestamps ordenados + Map ts→valor) listo para lookupHTF.
    const indiceHTF = (bars, vals) => ({
        ts:  bars.map(b => parseInt(b[6])).sort((a, b) => a - b),
        map: new Map(bars.map((b, i) => [parseInt(b[6]), vals[i]]))
    });

    const c1m = memo('close|1m', () => bars1m.map(b => parseFloat(b[4])));

    // Indexamos los indicadores HTF por tiempo de CIERRE (b[6]), no de apertura (b[0]).
    // Así lookupHTF(ts) solo devuelve velas HTF ya cerradas respecto a la vela 1m actual,
    // evitando look-ahead bias (usar el valor final de una vela 15m/5m aún en formación).
    const c15m = memo('close|15m', () => bars15m.map(b => parseFloat(b[4])));
    const c5m_pb = memo('close|5m', () => bars5m.map(b => parseFloat(b[4])));
    // Velas de 1h derivadas de las de 1m: las piden ADX y EMA de Tendencia en tf '1h'.
    // Agregar 200k velas es caro, así que se hace una sola vez por ventana.
    const bars1h = () => memo('bars|1h', () => agregarVelas1m(bars1m, 3600000));
    // Máximos y mínimos de 1m: solo los usan ADX y Angulación en tf '1m', así que se
    // materializan a demanda (recorrer 200k velas de más por corrida no es gratis).
    const h1m = () => memo('high|1m', () => bars1m.map(b => parseFloat(b[2])));
    const l1m = () => memo('low|1m',  () => bars1m.map(b => parseFloat(b[3])));

    // Pullback EMA arrays — configurables por el usuario (período + temporalidad)
    const pbEMAConfig = (Array.isArray(p.pullbackEMAs) && p.pullbackEMAs.length > 0)
        ? p.pullbackEMAs
        : [{ period:50,tf:'1m' },{ period:100,tf:'1m' },{ period:200,tf:'1m' },{ period:500,tf:'1m' }];
    const pbArr1m = {}, pbArr5m = {}, pbArr15m = {};
    for (const { period, tf } of pbEMAConfig) {
        if (tf === '1m' && !pbArr1m[period]) {
            pbArr1m[period] = memo(`ema|1m|${period}`, () => calcEMA(c1m, period));
        } else if (tf === '5m' && !pbArr5m[period]) {
            pbArr5m[period] = memo(`emaIdx|5m|${period}`, () => indiceHTF(bars5m, calcEMA(c5m_pb, period)));
        } else if (tf === '15m' && !pbArr15m[period]) {
            pbArr15m[period] = memo(`emaIdx|15m|${period}`, () => indiceHTF(bars15m, calcEMA(c15m, period)));
        }
    }
    // RSI — configurable período, temporalidad y umbrales de entrada
    const rsiPeriod   = p.rsiPeriod   || 14;
    const rsiTf       = p.rsiTf       || '15m';
    const rsiLongMin  = p.rsiLongMin  ?? 60;
    const rsiShortMax = p.rsiShortMax ?? 40;
    const useRsiFilter = p.useRsiFilter !== false;   // default ON (compat. estrategias previas)
    let rsiDirect = null, rsiByTs = null, tsRsi = null;
    if (rsiTf === '1m') {
        rsiDirect = memo(`rsi|1m|${rsiPeriod}`, () => calcRSI(c1m, rsiPeriod));
    } else if (rsiTf === '5m') {
        const idx = memo(`rsiIdx|5m|${rsiPeriod}`, () => indiceHTF(bars5m, calcRSI(c5m_pb, rsiPeriod)));
        rsiByTs = idx.map; tsRsi = idx.ts;
    } else {
        const idx = memo(`rsiIdx|15m|${rsiPeriod}`, () => indiceHTF(bars15m, calcRSI(c15m, rsiPeriod)));
        rsiByTs = idx.map; tsRsi = idx.ts;
    }

    const c5m = c5m_pb;
    // MACD — configurable período (fast/slow/signal) y temporalidad
    const macdFast   = p.macdFast   || 12;
    const macdSlow   = p.macdSlow   || 26;
    const macdSignal = p.macdSignal || 9;
    const macdTf     = p.macdTf     || '5m';
    const useMacdFilter = p.useMacdFilter !== false;  // default ON (compat. estrategias previas)
    const macdClave = `${macdFast}|${macdSlow}|${macdSignal}`;
    // Empareja línea MACD y señal en un solo array/Map, que es como los consume el loop.
    const macdPares = (closes) => {
        const { macd: mArr, signal: sArr } = calcMACDArr(closes, macdFast, macdSlow, macdSignal);
        return mArr.map((m, idx) => ({ macd: m, sig: sArr[idx] }));
    };
    let macdDirect = null, macdByTs = null, tsMACD = null;
    if (macdTf === '1m') {
        macdDirect = memo(`macd|1m|${macdClave}`, () => macdPares(c1m));
    } else if (macdTf === '5m') {
        const idx = memo(`macdIdx|5m|${macdClave}`, () => indiceHTF(bars5m, macdPares(c5m)));
        macdByTs = idx.map; tsMACD = idx.ts;
    } else {
        const idx = memo(`macdIdx|15m|${macdClave}`, () => indiceHTF(bars15m, macdPares(c15m)));
        macdByTs = idx.map; tsMACD = idx.ts;
    }

    const h15m = memo('high|15m', () => bars15m.map(b => parseFloat(b[2])));
    const l15m = memo('low|15m',  () => bars15m.map(b => parseFloat(b[3])));
    // ADX — temporalidad configurable (1m/5m/15m/1h). La de 1h no se guarda en ningún
    // lado: se deriva agregando las velas 1m del período (agregarVelas1m).
    const adxTf = ['1m', '5m', '15m', '1h'].includes(p.adxTf) ? p.adxTf : '15m';
    let adxDirect = null, adxByTs = null, tsAdx = null;
    if (p.useADXFilter) {
        if (adxTf === '1m') {
            adxDirect = memo('adx|1m', () => calcADX(h1m(), l1m(), c1m));
        } else {
            const idx = memo(`adxIdx|${adxTf}`, () => {
                const barsAdx = adxTf === '5m' ? bars5m : adxTf === '15m' ? bars15m : bars1h();
                return indiceHTF(barsAdx, calcADX(barsAdx.map(b => parseFloat(b[2])), barsAdx.map(b => parseFloat(b[3])), barsAdx.map(b => parseFloat(b[4]))));
            });
            adxByTs = idx.map; tsAdx = idx.ts;
        }
    }

    // Angulación de EMA — pendiente normalizada por ATR, con temporalidad propia.
    // gate = umbral mínimo de pendiente; 'strong' exige la banda fuerte. Long pide
    // slope >= +gate (alcista), Short pide slope <= -gate (bajista). Default OFF.
    const useEmaAngFilter = p.useEmaAngFilter === true;
    const emaAngTf        = p.emaAngTf        || '15m';
    const emaAngLen       = p.emaAngLen       || 200;
    const emaAngSlopeBars = p.emaAngSlopeBars || 10;
    const emaAngAtr       = p.emaAngAtr       || 14;
    const emaAngGate      = p.emaAngMode === 'strong'
        ? (p.emaAngStrongSlope ?? 0.60)
        : (p.emaAngMinSlope    ?? 0.25);
    let emaAngDirect = null, emaAngByTs = null, tsEmaAng = null;
    const angClave = `${emaAngLen}|${emaAngAtr}|${emaAngSlopeBars}`;
    if (useEmaAngFilter) {
        if (emaAngTf === '1m') {
            emaAngDirect = memo(`emaAng|1m|${angClave}`, () => calcEMAangSlope(c1m, h1m(), l1m(), emaAngLen, emaAngAtr, emaAngSlopeBars));
        } else if (emaAngTf === '5m') {
            const idx = memo(`emaAngIdx|5m|${angClave}`, () => {
                const h5m = memo('high|5m', () => bars5m.map(b => parseFloat(b[2])));
                const l5m = memo('low|5m',  () => bars5m.map(b => parseFloat(b[3])));
                return indiceHTF(bars5m, calcEMAangSlope(c5m_pb, h5m, l5m, emaAngLen, emaAngAtr, emaAngSlopeBars));
            });
            emaAngByTs = idx.map; tsEmaAng = idx.ts;
        } else {
            const idx = memo(`emaAngIdx|15m|${angClave}`, () => indiceHTF(bars15m, calcEMAangSlope(c15m, h15m, l15m, emaAngLen, emaAngAtr, emaAngSlopeBars)));
            emaAngByTs = idx.map; tsEmaAng = idx.ts;
        }
    }

    // EMA de Tendencia — sesgo direccional: precio arriba de la EMA habilita long, debajo
    // habilita short. Temporalidad propia (incluye 1h vía agregarVelas1m, igual que ADX).
    const useEmaTrendFilter = p.useEmaTrendFilter === true;
    const emaTrendTf  = p.emaTrendTf  || '1h';
    const emaTrendLen = p.emaTrendLen || 21;
    let emaTrendDirect = null, emaTrendByTs = null, tsEmaTrend = null;
    if (useEmaTrendFilter) {
        if (emaTrendTf === '1m') {
            emaTrendDirect = memo(`ema|1m|${emaTrendLen}`, () => calcEMA(c1m, emaTrendLen));
        } else {
            const idx = memo(`emaIdx|${emaTrendTf}|${emaTrendLen}`, () => {
                const barsTrend = emaTrendTf === '5m' ? bars5m : emaTrendTf === '15m' ? bars15m : bars1h();
                return indiceHTF(barsTrend, calcEMA(barsTrend.map(b => parseFloat(b[4])), emaTrendLen));
            });
            emaTrendByTs = idx.map; tsEmaTrend = idx.ts;
        }
    }

    // VWAP — configurable timeframe y sesión
    const vwapTf_bt      = p.vwapTf      || '5m';
    const vwapSession_bt = p.vwapSession || 'daily';
    let vwapDirect_bt = null, vwapByTs_bt = null, tsVwap_bt = null;
    if (p.useVwapFilter) {
        if (vwapTf_bt === '1m') {
            vwapDirect_bt = memo(`vwap|1m|${vwapSession_bt}`, () => calcVWAP(bars1m, vwapSession_bt));
        } else if (vwapTf_bt === '5m') {
            const idx = memo(`vwapIdx|5m|${vwapSession_bt}`, () => indiceHTF(bars5m, calcVWAP(bars5m, vwapSession_bt)));
            vwapByTs_bt = idx.map; tsVwap_bt = idx.ts;
        } else {
            const idx = memo(`vwapIdx|15m|${vwapSession_bt}`, () => indiceHTF(bars15m, calcVWAP(bars15m, vwapSession_bt)));
            vwapByTs_bt = idx.map; tsVwap_bt = idx.ts;
        }
    }

    // Open Interest — "OI subiendo confirma": solo se permite entrar (long o short) si el OI
    // creció ≥ oiThreshold% en las últimas oiLookbackMin minutos = entra dinero nuevo respaldando
    // el movimiento. El OI se muestrea cada 1m (live) / 5m (backfill histórico); lookupHTF toma la
    // última muestra ≤ tsClose, sin look-ahead. Si no hay dato (período sin cobertura) → no entra.
    const useOIFilter  = p.useOIFilter === true;
    const oiLookbackMs = (p.oiLookbackMin || 30) * 60000;
    const oiThreshold  = Number.isFinite(p.oiThreshold) ? p.oiThreshold : 0.5;
    let oiTs = null, oiByTs = null;
    if (useOIFilter && Array.isArray(oiArr) && oiArr.length) {
        oiByTs = new Map();
        for (const o of oiArr) oiByTs.set(Number(o.tiempo) * 1000, parseFloat(o.valor));
        oiTs = [...oiByTs.keys()].sort((a, b) => a - b);
    }

    // Posicionamiento de traders — a diferencia del OI, importa el NIVEL actual del ratio, no el
    // cambio: lookupHTF toma la última muestra (5m) ≤ tsClose. Top traders = seguir smart money
    // (long si están netos long); retail (global) = fade contrarian (no comprar si el retail está
    // sobrecargado long). Umbrales simétricos alrededor del neutro 1.0 (ratio long/short).
    const useTopTraderFilter  = p.useTopTraderFilter === true;
    const useRetailFilter     = p.useRetailFilter === true;
    // Pendiente del ratio — smart money ACUMULANDO (slope > threshold) vs DISTRIBUYENDO.
    // Independiente del filtro de nivel: podés usar solo el slope, solo el nivel, o ambos.
    const useTopSlopeFilter   = p.useTopSlopeFilter === true;
    const topSlopeLookbackMs  = (p.topSlopeLookbackMin ?? 15) * 60000;
    const topTraderRatio = Number.isFinite(p.topTraderRatio) ? p.topTraderRatio : 1.05;
    const retailExtreme  = Number.isFinite(p.retailExtreme)  ? p.retailExtreme  : 2.0;
    let topTs = null, topByTs = null, globTs = null, globByTs = null;
    if ((useTopTraderFilter || useRetailFilter || useTopSlopeFilter) && Array.isArray(lsArr) && lsArr.length) {
        topByTs = new Map(); globByTs = new Map();
        for (const r of lsArr) {
            const tms = Number(r.tiempo) * 1000;
            if (r.top_pos    != null) topByTs.set(tms, parseFloat(r.top_pos));
            if (r.global_acc != null) globByTs.set(tms, parseFloat(r.global_acc));
        }
        topTs  = [...topByTs.keys()].sort((a, b) => a - b);
        globTs = [...globByTs.keys()].sort((a, b) => a - b);
    }

    // Delta de volumen — prefix sum para rolling sum O(1) por barra.
    // b[9] null (BingX sin taker_buy_base para esa vela) aporta delta 0: ni NaN (que
    // bloquearía silenciosamente todo filtro de delta/CVD) ni "todo ventas" (bv=0 → -tv).
    const deltaPfx = memo('deltaPfx', () => {
        const pfx = new Array(bars1m.length + 1).fill(0);
        for (let i = 0; i < bars1m.length; i++) {
            const bv = bars1m[i][9] == null ? null : parseFloat(bars1m[i][9]); // takerBuyBaseAssetVolume
            const tv = parseFloat(bars1m[i][5]); // total volume
            pfx[i + 1] = pfx[i] + (Number.isFinite(bv) ? bv - (tv - bv) : 0);
        }
        return pfx;
    });

    // CVD (Cumulative Volume Delta) — reutiliza deltaPfx. Slope = diferencia del acumulado
    // entre i y i-cvdLookback: positivo = presión compradora neta, negativo = vendedora.
    // No necesita estructura extra; lookup O(1) en el loop.
    const useCVDFilter = p.useCVDFilter === true;
    const cvdLookback  = p.cvdLookback || 20;

    // Ballenas — ordenadas por tiempo para sliding window O(n)
    const whaleTrades = (whalesArr || [])
        .map(w => ({ ts: parseFloat(w.ts_sec) * 1000, btc: parseFloat(w.cantidad), isSell: w.es_venta }))
        .sort((a, b) => a.ts - b.ts);
    const whaleWindowMs = p.whaleWindow * 60000;
    let wLeft = 0, wRight = -1, wBuys = 0, wSells = 0;

    // Stop EMAs configurables (modo Ruptura EMA): pre-compute una vez, lookup por barra en el loop
    const stopEMACfgs = p.stopType === 'Ruptura EMA'
        ? (Array.isArray(p.stopEMAs) && p.stopEMAs.length ? p.stopEMAs : [{ period:200, tf:'1m' }, { period:500, tf:'1m' }])
        : [];
    const stopEmaArrays = stopEMACfgs.map(({ period, tf }) => {
        if (tf === '5m') {
            const idx = memo(`emaIdx|5m|${period}`, () => indiceHTF(bars5m, calcEMA(c5m_pb, period)));
            return { arr: null, tsArr: idx.ts, map: idx.map };
        } else if (tf === '15m') {
            const idx = memo(`emaIdx|15m|${period}`, () => indiceHTF(bars15m, calcEMA(c15m, period)));
            return { arr: null, tsArr: idx.ts, map: idx.map };
        }
        return { arr: memo(`ema|1m|${period}`, () => calcEMA(c1m, period)), tsArr: null, map: null };
    });

    let capital = p.initialCapital;
    let posiciones = []; // { side, entry, entryBarIdx, tp, sl, capitalAtEntry }
    let lastClosedBarIdx = null;
    const trades = [];
    const equity = [{ ts: parseInt(bars1m[0][0]), v: capital }];
    // Drawdown marcado a mercado: se actualiza en cada vela (no solo al cerrar trade),
    // capturando la peor excursión adversa de todas las posiciones abiertas.
    let ddPeak = capital, maxDDPerc = 0;
    // Velas iniciales que solo sirven para que los indicadores converjan (no se opera en ellas).
    // El optimizador lo sobreescribe: para backtestear la ventana [D, D+30d] le antepone unos
    // días de velas previas y pone warmupBars = largo de ese prefijo, de modo que el primer
    // trade posible caiga exactamente en D y las métricas de la ventana no se contaminen.
    const WARMUP = Number.isFinite(p.warmupBars) ? Math.max(0, Math.min(p.warmupBars, bars1m.length)) : 500;
    // Última vela a procesar (exclusivo). El optimizador la usa para evaluar un PREFIJO de la
    // ventana y decidir si vale la pena terminar la corrida (pruning), reusando los indicadores
    // ya memoizados. Por defecto se recorre la ventana entera.
    const HASTA = Number.isFinite(p.hastaBarIdx) ? Math.max(0, Math.min(p.hastaBarIdx, bars1m.length)) : bars1m.length;
    // Curva de equity marcada a mercado (incluye PnL no realizado), submuestreada a ~600
    // puntos para que coincida con el max drawdown reportado y no pese de más en payload.
    const eqStep = Math.max(1, Math.floor((HASTA - WARMUP) / 600));

    for (let i = WARMUP; i < HASTA; i++) {
        const bar = bars1m[i];
        const ts = parseInt(bar[0]);
        const tsClose = parseInt(bar[6]); // instante real de la decisión (cierre de la vela 1m)
        const high = parseFloat(bar[2]), low = parseFloat(bar[3]), close = parseFloat(bar[4]);
        const alignVals = pbEMAConfig.map(({ period, tf }) => {
            if (tf === '1m')  return pbArr1m[period]?.[i] ?? null;
            if (tf === '5m')  return pbArr5m[period]  ? lookupHTF(pbArr5m[period].ts,  pbArr5m[period].map,  tsClose) : null;
            if (tf === '15m') return pbArr15m[period] ? lookupHTF(pbArr15m[period].ts, pbArr15m[period].map, tsClose) : null;
            return null;
        });
        if (alignVals.some(v => !v)) continue;
        // El alineamiento se evalúa por período (de la EMA más chica a la más grande),
        // no por el orden en que el usuario cargó la lista en la UI.
        const alignValsOrdered = pbEMAConfig
            .map(({ period }, idx) => ({ period, val: alignVals[idx] }))
            .sort((a, b) => a.period - b.period)
            .map(x => x.val);
        const stopEmaVals = stopEmaArrays.map(s => s.arr ? s.arr[i] : lookupHTF(s.tsArr, s.map, tsClose)).filter(v => v != null);

        const rsiRaw  = rsiTf === '1m'  ? rsiDirect[i]  : lookupHTF(tsRsi,  rsiByTs,  tsClose);
        const macdRaw = macdTf === '1m' ? macdDirect[i] : lookupHTF(tsMACD, macdByTs, tsClose);
        if (rsiRaw == null || !macdRaw || macdRaw.macd === null || macdRaw.sig === null) continue;

        const rsiVal = rsiRaw;
        const { macd: macd5, sig: sig5 } = macdRaw;

        // SALIDAS — iterar en reversa para poder hacer splice sin afectar índices
        for (let j = posiciones.length - 1; j >= 0; j--) {
            const pos = posiciones[j];
            const barsIn = i - pos.entryBarIdx;
            let exitPrice = null, exitReason = null;

            // Liquidación por apalancamiento — tiene prioridad sobre TP/SL.
            // El margen aislado se agota un poco ANTES de 1/palanca por el margen de
            // mantenimiento y la comisión de cierre, así que la liquidación ocurre antes.
            if (p.palancaActivo && p.palancaValor > 1) {
                const MMR = 0.005; // margen de mantenimiento aprox. (BTC perp)
                const liqDelta = Math.max(0.0001, 1 / p.palancaValor - MMR - (p.commission / 100));
                if (pos.side === 1 && low <= pos.entry * (1 - liqDelta)) {
                    exitPrice = pos.entry * (1 - liqDelta); exitReason = 'LIQ';
                } else if (pos.side === -1 && high >= pos.entry * (1 + liqDelta)) {
                    exitPrice = pos.entry * (1 + liqDelta); exitReason = 'LIQ';
                }
            }

            // Breakeven: el stop ya fue movido a entrada ± offset en una vela ANTERIOR (ver
            // disparo más abajo). Se evalúa antes que TP/SL/EMA: es un nivel duro intra-vela
            // que ejecuta antes que cualquier chequeo al cierre. Si la misma vela toca TP y
            // breakeven, prioriza breakeven (misma convención conservadora que el caso TP+SL).
            if (!exitPrice && pos.beAplicado) {
                if      (pos.side === 1  && low  <= pos.sl) { exitPrice = pos.sl; exitReason = 'BE'; }
                else if (pos.side === -1 && high >= pos.sl) { exitPrice = pos.sl; exitReason = 'BE'; }
            }

            if (!exitPrice) {
                if (pos.side === 1) {
                    if (p.stopType === 'Porcentaje') {
                        if      (high >= pos.tp && low <= pos.sl) { exitPrice = pos.sl; exitReason = 'SL'; }
                        else if (high >= pos.tp)                   { exitPrice = pos.tp; exitReason = 'TP'; }
                        else if (low  <= pos.sl)                   { exitPrice = pos.sl; exitReason = 'SL'; }
                    } else {
                        // Si rompe alguna stop EMA al cierre y también toca TP, prioriza la ruptura.
                        if      (stopEmaVals.some(v => close < v)) { exitPrice = close;  exitReason = 'EMA'; }
                        else if (high >= pos.tp)                    { exitPrice = pos.tp; exitReason = 'TP'; }
                    }
                    if (!exitPrice && p.useMaxTradeTime && barsIn >= p.maxTradeMinutes) { exitPrice = close; exitReason = 'Tiempo'; }
                } else {
                    if (p.stopType === 'Porcentaje') {
                        if      (low <= pos.tp && high >= pos.sl) { exitPrice = pos.sl; exitReason = 'SL'; }
                        else if (low  <= pos.tp)                   { exitPrice = pos.tp; exitReason = 'TP'; }
                        else if (high >= pos.sl)                   { exitPrice = pos.sl; exitReason = 'SL'; }
                    } else {
                        // Si rompe alguna stop EMA al cierre y también toca TP, prioriza la ruptura.
                        if      (stopEmaVals.some(v => close > v)) { exitPrice = close;   exitReason = 'EMA'; }
                        else if (low <= pos.tp)                     { exitPrice = pos.tp;  exitReason = 'TP'; }
                    }
                    if (!exitPrice && p.useMaxTradeTime && barsIn >= p.maxTradeMinutes) { exitPrice = close; exitReason = 'Tiempo'; }
                }
            }

            if (exitPrice) {
                const raw = pos.side === 1
                    ? (exitPrice - pos.entry) / pos.entry
                    : (pos.entry - exitPrice) / pos.entry;
                const palanca = p.palancaActivo ? (p.palancaValor || 1) : 1;
                const net = raw * palanca - costoOperacion(p, palanca, barsIn, pos.side);
                // En margen aislado la liquidación consume todo el margen (el de
                // mantenimiento restante se lo lleva el fee de liquidación): pérdida = -margen.
                // Sin este caso especial, a palanca alta se sub-contabiliza la pérdida.
                const pnlAbs = exitReason === 'LIQ'
                    ? -pos.capitalAtEntry
                    : Math.max(pos.capitalAtEntry * net, -pos.capitalAtEntry);
                capital += pnlAbs;
                trades.push({ type: pos.side === 1 ? 'Long' : 'Short', entryTs: parseInt(bars1m[pos.entryBarIdx][0]), exitTs: ts, entryPrice: pos.entry, exitPrice, tp: pos.tp, sl: pos.sl, pnlPerc: (pnlAbs / pos.capitalAtEntry) * 100, pnlAbs, reason: exitReason, capital, oiSlope: pos.oiSlope, topRatio: pos.topRatio, globalRatio: pos.globalRatio });
                lastClosedBarIdx = i;
                posiciones.splice(j, 1);
            } else if (p.useBreakeven && !pos.beAplicado) {
                // Disparo del breakeven: si la vela avanzó el % de trigger a favor, mover el
                // stop a entrada ± offset (el offset cubre comisión + slippage de ida y vuelta).
                // Rige recién desde la PRÓXIMA vela: dentro de una vela 1m no se sabe si el
                // extremo favorable ocurrió antes o después del retroceso, así que asumir
                // disparo-y-salida en la misma vela sería look-ahead optimista.
                const disparo = pos.side === 1
                    ? high >= pos.entry * (1 + p.breakevenTrigger / 100)
                    : low  <= pos.entry * (1 - p.breakevenTrigger / 100);
                if (disparo) {
                    pos.beAplicado = true;
                    pos.sl = pos.side === 1
                        ? pos.entry * (1 + p.breakevenOffset / 100)
                        : pos.entry * (1 - p.breakevenOffset / 100);
                }
            }
        }

        // Actualizar sliding window de ballenas (se hace siempre, no solo en entrada).
        // Anclada al CIERRE de la vela 1m (tsClose): la decisión ocurre al cierre, así que
        // se incluyen las ballenas de la propia vela (igual que el filtro de delta) sin
        // look-ahead, y queda alineado con el modo en vivo (que usa el momento actual).
        while (wRight + 1 < whaleTrades.length && whaleTrades[wRight + 1].ts <= tsClose) {
            wRight++;
            if (whaleTrades[wRight].isSell) wSells += whaleTrades[wRight].btc; else wBuys += whaleTrades[wRight].btc;
        }
        while (wLeft <= wRight && whaleTrades[wLeft].ts < tsClose - whaleWindowMs) {
            if (whaleTrades[wLeft].isSell) wSells -= whaleTrades[wLeft].btc; else wBuys -= whaleTrades[wLeft].btc;
            wLeft++;
        }
        const whaleDelta = wBuys - wSells;

        // ENTRADAS — permitir si no hay posición abierta, o si múltiples entradas está habilitado.
        // Filtro opcional: si ya hay sub-posiciones abiertas con PnL no realizado negativo,
        // bloquear nuevas entradas múltiples (filtra rachas perdedoras al precio actual).
        const hayPosicionEnPerdida = p.blockMultipleIfLosing && posiciones.some(pos =>
            (pos.side === 1 ? close - pos.entry : pos.entry - close) < 0
        );
        if ((posiciones.length === 0 || p.allowMultipleEntries) && !hayPosicionEnPerdida) {
            const argDate = new Date(ts - 3 * 3600000); // horario Argentina (UTC-3)
            const barHour = argDate.getUTCHours();
            const argDay  = argDate.getUTCDay(); // 0=Dom, 6=Sáb
            const barsSinceClose = lastClosedBarIdx !== null ? i - lastClosedBarIdx : 999999;

            if (
                barHour >= p.startHour && barHour < p.endHour &&
                (p.operaFinDeSemana || (argDay !== 0 && argDay !== 6)) &&
                !(p.useCooldown && barsSinceClose < p.cooldownMinutes)
            ) {
                const above = alignVals.every(v => close > v);
                const below = alignVals.every(v => close < v);
                const bullAlign = alignValsOrdered.length < 2 || alignValsOrdered.every((v, j) => j === 0 || alignValsOrdered[j-1] > v);
                const bearAlign = alignValsOrdered.length < 2 || alignValsOrdered.every((v, j) => j === 0 || alignValsOrdered[j-1] < v);
                const pbVals = alignVals;
                const nearEMA = pbVals.some(e => Math.abs(close - e) / close * 100 <= (p.pullbackPerc ?? 0.2));
                const pullOK     = !p.usePullbackFilter || (pbVals.length > 0 && nearEMA);
                const alignLong  = !p.useEmaAlignment || bullAlign;
                const alignShort = !p.useEmaAlignment || bearAlign;

                // Delta de volumen rolling (últimas N velas)
                const dStart       = Math.max(0, i - p.deltaVelas + 1);
                const deltaRolling = deltaPfx[i + 1] - deltaPfx[dStart];
                const deltaOkLong  = !p.useDeltaFilter || deltaRolling > 0;
                const deltaOkShort = !p.useDeltaFilter || deltaRolling < 0;

                // Ballenas: delta en ventana reciente
                const whaleOkLong  = !p.useWhaleFilter || whaleDelta > 0;
                const whaleOkShort = !p.useWhaleFilter || whaleDelta < 0;

                // ADX: mide fuerza de tendencia (>= umbral = tendencia fuerte), en su temporalidad propia
                const adxRaw = !p.useADXFilter ? null
                    : adxTf === '1m' ? adxDirect[i] : lookupHTF(tsAdx, adxByTs, tsClose);
                const adxOk  = !p.useADXFilter || (adxRaw != null && adxRaw >= (p.adxThreshold ?? 25));

                // VWAP — dirección y/o pullback
                const vwapVal_bt = !p.useVwapFilter ? null
                    : vwapTf_bt === '1m' ? vwapDirect_bt[i] : lookupHTF(tsVwap_bt, vwapByTs_bt, tsClose);
                const vwapOkLong  = !p.useVwapFilter || (vwapVal_bt !== null &&
                    (!p.vwapUseDirection || close > vwapVal_bt) &&
                    (!p.vwapUsePullback  || Math.abs(close - vwapVal_bt) / close * 100 <= (p.vwapPullbackPerc ?? 0.3))
                );
                const vwapOkShort = !p.useVwapFilter || (vwapVal_bt !== null &&
                    (!p.vwapUseDirection || close < vwapVal_bt) &&
                    (!p.vwapUsePullback  || Math.abs(close - vwapVal_bt) / close * 100 <= (p.vwapPullbackPerc ?? 0.3))
                );

                // Angulación de EMA — pendiente de la EMA en su temporalidad propia
                const emaAngSlope = !useEmaAngFilter ? null
                    : emaAngTf === '1m' ? emaAngDirect[i] : lookupHTF(tsEmaAng, emaAngByTs, tsClose);
                const emaAngOkLong  = !useEmaAngFilter || (emaAngSlope !== null && emaAngSlope >=  emaAngGate);
                const emaAngOkShort = !useEmaAngFilter || (emaAngSlope !== null && emaAngSlope <= -emaAngGate);

                // EMA de Tendencia — sesgo direccional según la posición del precio respecto a la EMA
                const emaTrendVal = !useEmaTrendFilter ? null
                    : emaTrendTf === '1m' ? emaTrendDirect[i] : lookupHTF(tsEmaTrend, emaTrendByTs, tsClose);
                const emaTrendOkLong  = !useEmaTrendFilter || (emaTrendVal !== null && close > emaTrendVal);
                const emaTrendOkShort = !useEmaTrendFilter || (emaTrendVal !== null && close < emaTrendVal);

                // Open Interest — confirma que el OI viene subiendo (dinero nuevo). Aplica a ambos lados.
                let oiOk = !useOIFilter, oiSlopeVal = null;
                if (useOIFilter && oiTs) {
                    const oiNow  = lookupHTF(oiTs, oiByTs, tsClose);
                    const oiPast = lookupHTF(oiTs, oiByTs, tsClose - oiLookbackMs);
                    if (oiNow != null && oiPast != null && oiPast > 0) {
                        oiSlopeVal = (oiNow - oiPast) / oiPast * 100;
                        oiOk = oiSlopeVal >= oiThreshold;
                    } else {
                        oiOk = false;
                    }
                }

                // Posicionamiento — top traders (seguir) y retail global (fade), por nivel.
                const topRatioVal  = (useTopTraderFilter || useTopSlopeFilter) && topTs  ? lookupHTF(topTs,  topByTs,  tsClose) : null;
                const globRatioVal = useRetailFilter    && globTs ? lookupHTF(globTs, globByTs, tsClose) : null;
                const topOkLong    = !useTopTraderFilter || (topRatioVal  != null && topRatioVal  >= topTraderRatio);
                const topOkShort   = !useTopTraderFilter || (topRatioVal  != null && topRatioVal  <= 1 / topTraderRatio);
                const retailOkLong  = !useRetailFilter || (globRatioVal != null && globRatioVal <= retailExtreme);
                const retailOkShort = !useRetailFilter || (globRatioVal != null && globRatioVal >= 1 / retailExtreme);

                // Pendiente del ratio de top traders — smart money acumulando (slope > 0) ahora.
                const topRatioPast    = useTopSlopeFilter && topTs ? lookupHTF(topTs, topByTs, tsClose - topSlopeLookbackMs) : null;
                const topSlopeVal     = topRatioVal != null && topRatioPast != null ? topRatioVal - topRatioPast : null;
                const topSlopeOkLong  = !useTopSlopeFilter || (topSlopeVal !== null && topSlopeVal >  (p.topSlopeMin ?? 0));
                const topSlopeOkShort = !useTopSlopeFilter || (topSlopeVal !== null && topSlopeVal < -(p.topSlopeMin ?? 0));

                // CVD slope — net taker flow en los últimos cvdLookback velas (O(1) vía deltaPfx).
                const cvdSlope   = i >= cvdLookback ? deltaPfx[i + 1] - deltaPfx[i + 1 - cvdLookback] : null;
                const cvdOkLong  = !useCVDFilter || (cvdSlope !== null && cvdSlope > 0);
                const cvdOkShort = !useCVDFilter || (cvdSlope !== null && cvdSlope < 0);

                if (p.enableLongs && above && alignLong && (!useRsiFilter || rsiVal >= rsiLongMin) && (!useMacdFilter || macd5 > sig5) && pullOK && deltaOkLong && whaleOkLong && adxOk && vwapOkLong && emaAngOkLong && emaTrendOkLong && oiOk && topOkLong && retailOkLong && topSlopeOkLong && cvdOkLong) {
                    const capEntrada = calcCapitalEntrada(p, capital, posiciones);
                    if (capEntrada <= 0) { /* sin capital disponible, no entrar */ }
                    else {
                        const tp = close * (1 + p.tpPerc / 100);
                        const sl = p.stopType === 'Porcentaje' ? close * (1 - p.slPerc / 100) : (stopEmaVals.length ? Math.max(...stopEmaVals) : close * 0.99);
                        posiciones.push({ side: 1, entry: close, entryBarIdx: i, tp, sl, capitalAtEntry: capEntrada, oiSlope: oiSlopeVal, topRatio: topRatioVal, globalRatio: globRatioVal });
                    }
                } else if (p.enableShorts && below && alignShort && (!useRsiFilter || rsiVal <= rsiShortMax) && (!useMacdFilter || macd5 < sig5) && pullOK && deltaOkShort && whaleOkShort && adxOk && vwapOkShort && emaAngOkShort && emaTrendOkShort && oiOk && topOkShort && retailOkShort && topSlopeOkShort && cvdOkShort) {
                    const capEntrada = calcCapitalEntrada(p, capital, posiciones);
                    if (capEntrada <= 0) { /* sin capital disponible, no entrar */ }
                    else {
                        const tp = close * (1 - p.tpPerc / 100);
                        const sl = p.stopType === 'Porcentaje' ? close * (1 + p.slPerc / 100) : (stopEmaVals.length ? Math.min(...stopEmaVals) : close * 1.01);
                        posiciones.push({ side: -1, entry: close, entryBarIdx: i, tp, sl, capitalAtEntry: capEntrada, oiSlope: oiSlopeVal, topRatio: topRatioVal, globalRatio: globRatioVal });
                    }
                }
            }
        }

        // Marcado a mercado de la vela: equity realizada + PnL no realizado de todas las posiciones abiertas
        let markedEquity = capital;
        for (const pos of posiciones) {
            const raw = pos.side === 1 ? (close - pos.entry) / pos.entry : (pos.entry - close) / pos.entry;
            const palanca = p.palancaActivo ? (p.palancaValor || 1) : 1;
            markedEquity += Math.max(pos.capitalAtEntry * (raw * palanca - costoOperacion(p, palanca, i - pos.entryBarIdx, pos.side)), -pos.capitalAtEntry);
        }
        if (markedEquity > ddPeak) ddPeak = markedEquity;
        const ddNow = (ddPeak - markedEquity) / ddPeak * 100;
        if (ddNow > maxDDPerc) maxDDPerc = ddNow;

        // Muestreo de la curva de equity a mercado (último bar siempre incluido)
        if ((i - WARMUP) % eqStep === 0 || i === HASTA - 1) {
            equity.push({ ts, v: markedEquity });
        }
    }

    // Cierre forzado de posiciones que quedan abiertas al final del período: se liquidan
    // al cierre de la última vela ('Fin') para que stats (netProfit/winRate/...) y la curva
    // de equity reflejen lo mismo y no quede PnL no realizado fuera de las métricas.
    if (posiciones.length > 0) {
        const lastIdx   = HASTA - 1;
        const lastClose = parseFloat(bars1m[lastIdx][4]);
        const lastTs    = parseInt(bars1m[lastIdx][0]);
        for (let j = posiciones.length - 1; j >= 0; j--) {
            const pos = posiciones[j];
            const barsIn = lastIdx - pos.entryBarIdx;
            const raw = pos.side === 1
                ? (lastClose - pos.entry) / pos.entry
                : (pos.entry - lastClose) / pos.entry;
            const palanca = p.palancaActivo ? (p.palancaValor || 1) : 1;
            const net = raw * palanca - costoOperacion(p, palanca, barsIn, pos.side);
            const pnlAbs = Math.max(pos.capitalAtEntry * net, -pos.capitalAtEntry);
            capital += pnlAbs;
            trades.push({ type: pos.side === 1 ? 'Long' : 'Short', entryTs: parseInt(bars1m[pos.entryBarIdx][0]), exitTs: lastTs, entryPrice: pos.entry, exitPrice: lastClose, tp: pos.tp, sl: pos.sl, pnlPerc: (pnlAbs / pos.capitalAtEntry) * 100, pnlAbs, reason: 'Fin', capital, oiSlope: pos.oiSlope, topRatio: pos.topRatio, globalRatio: pos.globalRatio });
        }
        posiciones = [];
    }

    const wins   = trades.filter(t => t.pnlPerc > 0);
    const losses = trades.filter(t => t.pnlPerc <= 0);
    const grossW = wins.reduce((s, t) => s + Math.abs(t.pnlAbs), 0);
    const grossL = losses.reduce((s, t) => s + Math.abs(t.pnlAbs), 0);

    // Desglose de razones de salida sobre TODOS los trades (no solo los devueltos)
    const exitReasons = {};
    trades.forEach(t => { exitReasons[t.reason] = (exitReasons[t.reason] || 0) + 1; });

    // Máxima racha de ganadoras / perdedoras consecutivas
    let maxWinStreak = 0, maxLossStreak = 0, curWin = 0, curLoss = 0;
    trades.forEach(t => {
        if (t.pnlPerc > 0) {
            curWin++; curLoss = 0;
            if (curWin > maxWinStreak) maxWinStreak = curWin;
        } else {
            curLoss++; curWin = 0;
            if (curLoss > maxLossStreak) maxLossStreak = curLoss;
        }
    });

    return {
        stats: {
            totalTrades: trades.length,
            winners: wins.length, losers: losses.length,
            winRate: trades.length > 0 ? wins.length / trades.length * 100 : 0,
            netProfit: capital - p.initialCapital,
            netProfitPerc: (capital - p.initialCapital) / p.initialCapital * 100,
            finalCapital: capital,
            profitFactor: grossL > 0 ? grossW / grossL : grossW > 0 ? Infinity : 0,
            maxDrawdownPerc: maxDDPerc,
            avgWinPerc:  wins.length   > 0 ? wins.reduce((s, t) => s + t.pnlPerc, 0)   / wins.length   : 0,
            avgLossPerc: losses.length > 0 ? Math.abs(losses.reduce((s, t) => s + t.pnlPerc, 0) / losses.length) : 0,
            longsCount:  trades.filter(t => t.type === 'Long').length,
            shortsCount: trades.filter(t => t.type === 'Short').length,
            maxWinStreak, maxLossStreak,
            exitReasons,
        },
        // La UI solo grafica los últimos 300; el optimizador pide todos porque el Sharpe
        // se calcula sobre la serie completa de retornos por trade.
        trades: trades.slice(-(Number.isFinite(p.maxTradesDevueltos) ? p.maxTradesDevueltos : 300)),
        equity
    };
}

// Normaliza el body de POST /api/backtest a los params que espera runBacktest: aplica
// defaults, castea tipos y acota rangos. El optimizador lo usa para que una combinación
// de parámetros signifique lo mismo por CLI que por la UI.
function normalizarParams(body) {
    const p = {
        enableLongs:       body.enableLongs !== false,
        enableShorts:      body.enableShorts !== false,
        tpPerc:            parseFloat(body.tpPerc)  || 0.5,
        stopType:          (() => { const t = body.stopType || 'Porcentaje'; return (t === 'Ruptura EMA 200' || t === 'Ruptura EMA 500') ? 'Ruptura EMA' : t; })(),
        slPerc:            parseFloat(body.slPerc)  || 1.0,
        useBreakeven:      body.useBreakeven === true,
        breakevenTrigger:  Number.isFinite(parseFloat(body.breakevenTrigger)) ? parseFloat(body.breakevenTrigger) : 0.3,
        breakevenOffset:   Number.isFinite(parseFloat(body.breakevenOffset))  ? parseFloat(body.breakevenOffset)  : 0.12,
        stopEMAs:          (() => {
            const t = body.stopType;
            if (t === 'Ruptura EMA 200') return [{ period:200, tf:'1m' }, { period:200, tf:'1m' }];
            if (t === 'Ruptura EMA 500') return [{ period:500, tf:'1m' }, { period:500, tf:'1m' }];
            return Array.isArray(body.stopEMAs) && body.stopEMAs.length
                ? body.stopEMAs.slice(0,2).map(e => ({ period: Math.max(1, parseInt(e.period)||200), tf: ['1m','5m','15m'].includes(e.tf) ? e.tf : '1m' }))
                : [{ period:200, tf:'1m' }, { period:500, tf:'1m' }];
        })(),
        startHour:         Number.isFinite(parseInt(body.startHour)) ? parseInt(body.startHour) : 9,
        endHour:           Number.isFinite(parseInt(body.endHour))   ? parseInt(body.endHour)   : 20,
        usePullbackFilter: body.usePullbackFilter !== false,
        pullbackPerc:      parseFloat(body.pullbackPerc) || 0.20,
        pullbackEMAs:      Array.isArray(body.pullbackEMAs)
                               ? body.pullbackEMAs.map(e => ({
                                   period: parseInt(e.period) || 200,
                                   tf:     ['1m','5m','15m'].includes(e.tf) ? e.tf : '1m'
                               }))
                               : [{ period:50,tf:'1m' },{ period:100,tf:'1m' },{ period:200,tf:'1m' },{ period:500,tf:'1m' }],
        useRsiFilter:      body.useRsiFilter !== false,
        rsiTf:             ['1m','5m','15m'].includes(body.rsiTf) ? body.rsiTf : '15m',
        rsiPeriod:         parseInt(body.rsiPeriod) || 14,
        rsiLongMin:        body.rsiLongMin  != null ? parseFloat(body.rsiLongMin)  : 60,
        rsiShortMax:       body.rsiShortMax != null ? parseFloat(body.rsiShortMax) : 40,
        useEmaAngFilter:   body.useEmaAngFilter === true,
        emaAngTf:          ['1m','5m','15m'].includes(body.emaAngTf) ? body.emaAngTf : '15m',
        emaAngLen:         parseInt(body.emaAngLen)       || 200,
        emaAngSlopeBars:   parseInt(body.emaAngSlopeBars) || 10,
        emaAngAtr:         parseInt(body.emaAngAtr)       || 14,
        emaAngMinSlope:    Number.isFinite(parseFloat(body.emaAngMinSlope))    ? parseFloat(body.emaAngMinSlope)    : 0.25,
        emaAngStrongSlope: Number.isFinite(parseFloat(body.emaAngStrongSlope)) ? parseFloat(body.emaAngStrongSlope) : 0.60,
        emaAngMode:        body.emaAngMode === 'strong' ? 'strong' : 'min',
        useOIFilter:       body.useOIFilter === true,
        oiLookbackMin:     Math.min(Math.max(parseInt(body.oiLookbackMin) || 30, 5), 1440),
        oiThreshold:       Number.isFinite(parseFloat(body.oiThreshold)) ? parseFloat(body.oiThreshold) : 0.5,
        useTopTraderFilter: body.useTopTraderFilter === true,
        topTraderRatio:     Number.isFinite(parseFloat(body.topTraderRatio)) ? parseFloat(body.topTraderRatio) : 1.05,
        useRetailFilter:    body.useRetailFilter === true,
        retailExtreme:      Number.isFinite(parseFloat(body.retailExtreme)) ? parseFloat(body.retailExtreme) : 2.0,
        useTopSlopeFilter:  body.useTopSlopeFilter === true,
        topSlopeLookbackMin: Math.min(Math.max(parseInt(body.topSlopeLookbackMin) || 15, 5), 1440),
        topSlopeMin:        Number.isFinite(parseFloat(body.topSlopeMin)) ? parseFloat(body.topSlopeMin) : 0,
        useCVDFilter:       body.useCVDFilter === true,
        cvdLookback:        Math.min(Math.max(parseInt(body.cvdLookback) || 20, 2), 1440),
        useMacdFilter:     body.useMacdFilter !== false,
        macdTf:            ['1m','5m','15m'].includes(body.macdTf) ? body.macdTf : '5m',
        macdFast:          parseInt(body.macdFast)   || 12,
        macdSlow:          parseInt(body.macdSlow)   || 26,
        macdSignal:        parseInt(body.macdSignal) || 9,
        useEmaAlignment:   body.useEmaAlignment !== false,
        useMaxTradeTime:   body.useMaxTradeTime !== false,
        maxTradeMinutes:   parseInt(body.maxTradeMinutes) || 15,
        useCooldown:       body.useCooldown !== false,
        cooldownMinutes:   parseInt(body.cooldownMinutes) || 45,
        operaFinDeSemana:  body.operaFinDeSemana === true,
        useDeltaFilter:    body.useDeltaFilter === true,
        deltaVelas:        parseInt(body.deltaVelas) || 3,
        useWhaleFilter:    body.useWhaleFilter === true,
        whaleWindow:       parseInt(body.whaleWindow) || 30,
        whaleMinBTC:       parseFloat(body.whaleMinBTC) || 5,
        useADXFilter:        body.useADXFilter === true,
        adxTf:               ['1m','5m','15m','1h'].includes(body.adxTf) ? body.adxTf : '15m',
        adxThreshold:        parseInt(body.adxThreshold) || 25,
        useEmaTrendFilter:   body.useEmaTrendFilter === true,
        emaTrendTf:          ['1m','5m','15m','1h'].includes(body.emaTrendTf) ? body.emaTrendTf : '1h',
        emaTrendLen:         parseInt(body.emaTrendLen) || 21,
        useVwapFilter:       body.useVwapFilter === true,
        vwapTf:              ['1m','5m','15m'].includes(body.vwapTf) ? body.vwapTf : '5m',
        vwapSession:         ['daily','weekly','monthly'].includes(body.vwapSession) ? body.vwapSession : 'daily',
        vwapUseDirection:    body.vwapUseDirection === true,
        vwapUsePullback:     body.vwapUsePullback === true,
        vwapPullbackPerc:    parseFloat(body.vwapPullbackPerc) || 0.30,
        allowMultipleEntries: body.allowMultipleEntries === true,
        blockMultipleIfLosing: body.blockMultipleIfLosing === true,
        posicionTipo:         body.posicionTipo || 'porc_capital_actual',
        posicionValor:        parseFloat(body.posicionValor) || 100,
        palancaActivo:        body.palancaActivo === true,
        palancaValor:         parseFloat(body.palancaValor) || 1,
        commission:           Number.isFinite(parseFloat(body.commission))   ? parseFloat(body.commission)   : 0.04,
        slippagePerc:         Number.isFinite(parseFloat(body.slippagePerc))  ? parseFloat(body.slippagePerc)  : 0.02,
        fundingPerc:          Number.isFinite(parseFloat(body.fundingPerc))   ? parseFloat(body.fundingPerc)   : 0.01,
        initialCapital:       parseFloat(body.initialCapital) || 1000,
    };
    return p;
}

module.exports = {
    calcEMA, calcRSI, calcMACDArr, calcADX, calcATR, calcEMAangSlope, calcVWAP,
    agregarVelas1m, lookupHTF, calcCapitalEntrada, costoOperacion,
    runBacktest, normalizarParams,
};
