'use strict';
const TW = require('../public/tw-pivots');
const MINUTE = 60000;
const bounded = (v, fallback, min, max) => Number.isFinite(Number(v)) && v !== null && v !== '' ? Math.max(min, Math.min(max, Number(v))) : fallback;
function normalizeTW(body) {
    return {
        strategyType: body.strategyType === 'tw_mtf' ? 'tw_mtf' : 'classic',
        twMode: ['rejection', 'double', 'both'].includes(body.twMode) ? body.twMode : 'rejection',
        twConfig: TW.normalize(body.twConfig),
        twRewardRisk: bounded(body.twRewardRisk, 2, .1, 20),
        twStopBufferPerc: bounded(body.twStopBufferPerc, .1, 0, 10),
        twMinStopPerc: bounded(body.twMinStopPerc, .05, .001, 50),
        twMaxStopPerc: bounded(body.twMaxStopPerc, 5, .001, 50),
        twMaxHoldMinutes: Math.round(bounded(body.twMaxHoldMinutes, 0, 0, 525600)),
        twValidationPct: bounded(body.twValidationPct, 30, 0, 80)
    };
}
function warmupMs(p) {
    const c = TW.normalize(p.twConfig);
    return (c.pivotLeft + c.pivotRight + 2 + (p.twMode === 'rejection' ? 0 : c.doubleMaxSeparation)) * TW.TF[c.pivotTimeframe];
}
// Only complete UTC buckets from consecutive 1m candles are valid input to MTF.
function aggregateComplete(rows, ms) {
    const out = [];
    let bucket = null, count = 0, last = null;
    const flush = () => { if (bucket && count === ms / MINUTE && last + MINUTE === bucket.timestamp + ms) out.push(bucket); };
    for (const r of rows) {
        const timestamp = Math.floor(r[0] / ms) * ms;
        if (!bucket || timestamp !== bucket.timestamp) {
            flush();
            bucket = { timestamp, open: r[1], high: r[2], low: r[3], close: r[4] };
            count = r[0] === timestamp ? 1 : -Infinity;
        } else {
            count = r[0] === last + MINUTE ? count + 1 : -Infinity;
            bucket.high = Math.max(bucket.high, r[2]); bucket.low = Math.min(bucket.low, r[3]); bucket.close = r[4];
        }
        last = r[0];
    }
    flush();
    return out;
}
function summarize(trades) {
    const wins = trades.filter(t => t.pnlAbs > 0), losses = trades.filter(t => t.pnlAbs < 0);
    const grossW = wins.reduce((s, t) => s + t.pnlAbs, 0), grossL = -losses.reduce((s, t) => s + t.pnlAbs, 0);
    const reasons = {};
    let w = 0, l = 0, maxWinStreak = 0, maxLossStreak = 0;
    for (const t of trades) {
        reasons[t.reason] = (reasons[t.reason] || 0) + 1;
        w = t.pnlAbs > 0 ? w + 1 : 0; l = t.pnlAbs < 0 ? l + 1 : 0;
        maxWinStreak = Math.max(w, maxWinStreak); maxLossStreak = Math.max(l, maxLossStreak);
    }
    return { totalTrades: trades.length, winners: wins.length, losers: losses.length,
        winRate: trades.length ? wins.length / trades.length * 100 : 0,
        netProfit: grossW - grossL, profitFactor: grossL ? grossW / grossL : grossW ? Infinity : 0,
        avgWinPerc: wins.length ? wins.reduce((s,t) => s + t.pnlPerc, 0) / wins.length : 0,
        avgLossPerc: losses.length ? -losses.reduce((s,t) => s + t.pnlPerc, 0) / losses.length : 0,
        longsCount: trades.filter(t => t.type === 'Long').length, shortsCount: trades.filter(t => t.type === 'Short').length,
        maxWinStreak, maxLossStreak, exitReasons: reasons };
}
function runTWBacktest(inputRows, inputParams, sizePosition) {
    const p = { ...inputParams, ...normalizeTW(inputParams) };
    if (p.twMinStopPerc > p.twMaxStopPerc) throw new Error('TW: el stop mínimo no puede superar al máximo.');
    const capitalInitial = bounded(p.initialCapital, 1000, 1, 1e12);
    const leverage = p.palancaActivo ? bounded(p.palancaValor, 1, 1, 100) : 1;
    const commission = bounded(p.commission, .04, 0, 10) / 100;
    const slip = bounded(p.slippagePerc, .02, 0, 10) / 100;
    const funding = bounded(p.fundingPerc, .01, -10, 10) / 100;
    const end = Math.min(Number.isFinite(p._endTs) ? p._endTs : Date.now(), Date.now());
    const rows = [], warnings = [], seen = new Map();
    let malformed = 0;
    const limit = Number.isFinite(p.hastaBarIdx) ? p.hastaBarIdx : inputRows.length;
    for (const raw of inputRows.slice(0, limit)) {
        const r = raw.slice(0, 7).map(Number);
        if (r[0] + MINUTE > end) continue;
        if (!r.slice(0, 5).every(Number.isFinite) || r[0] % MINUTE || r[3] <= 0 || r[2] < Math.max(r[1], r[4]) || r[3] > Math.min(r[1], r[4])) { malformed++; continue; }
        if (seen.has(r[0]) && JSON.stringify(seen.get(r[0]).slice(0,5)) !== JSON.stringify(r.slice(0,5))) throw new Error('TW: velas duplicadas con precios distintos. Corregí la fuente de datos.');
        seen.set(r[0], r);
    }
    // Sorting is explicit: callers may supply exchange pages in any order.
    for (const r of seen.values()) rows.push(r);
    rows.sort((a,b) => a[0] - b[0]);
    if (malformed) warnings.push(`TW: ${malformed} velas inválidas descartadas; sus huecos no se agregan como velas completas.`);
    const first = rows.length ? rows[0][0] : 0, lastEnd = rows.length ? rows.at(-1)[0] + MINUTE : 0;
    const tradeStart = Number.isFinite(p._tradeStartTs) ? p._tradeStartTs : Number.isFinite(p.warmupBars) ? (rows[p.warmupBars]?.[0] ?? lastEnd) : first + warmupMs(p);
    const splitTs = lastEnd - Math.max(0, lastEnd - tradeStart) * p.twValidationPct / 100;
    const segments = [];
    for (const r of rows) {
        if (!segments.length || r[0] !== segments.at(-1).at(-1)[0] + MINUTE) segments.push([]);
        segments.at(-1).push(r);
    }
    if (segments.length > 1) warnings.push(`TW: ${segments.length - 1} huecos en velas 1m. Se reinician pivots y zonas después de cada hueco; no se permite una operación que atraviese datos faltantes.`);
    if (first > tradeStart - warmupMs(p)) warnings.push('TW: falta parte del historial previo solicitado para calentar pivots y dobles extremos. El inicio puede tener menos señales.');
    if (!rows.length || lastEnd <= tradeStart) warnings.push('TW: historial insuficiente para completar calentamiento y período de evaluación.');
    if (p.twMode !== 'double' && !p.twConfig.usePivotZones && p.twConfig.requirePatternNearZone) warnings.push('TW: los rechazos no pueden generar señales con zonas desactivadas y cercanía obligatoria.');
    const trades = [], equity = [], rejected = { risk: 0, conflict: 0, capital: 0, occupied: 0, stale: 0 };
    let capital = capitalInitial, peak = capital, maxDD = 0, signalCount = 0;
    let pivotBars = 0, patternBars = 0, evaluatedMinutes = 0;
    const step = Math.max(1, Math.floor(rows.length / 600));
    function mark(value, ts, save) {
        peak = Math.max(peak, value);
        maxDD = Math.max(maxDD, peak > 0 ? (peak - value) / peak * 100 : 0);
        if (save) equity.push({ ts, v: value });
    }
    for (let segmentIndex = 0; segmentIndex < segments.length; segmentIndex++) {
        const part = segments[segmentIndex];
        const c = { ...p.twConfig, confirmDoubleWithNeckline: true, showEarlyDouble: false };
        const h = aggregateComplete(part, TW.TF[c.pivotTimeframe]);
        const pattern = c.patternTimeframe === c.pivotTimeframe ? h : aggregateComplete(part, TW.TF[c.patternTimeframe]);
        pivotBars += h.length; patternBars += pattern.length;
        const result = TW.calculate(h, pattern, c, part.at(-1)[0] + MINUTE);
        const events = result.signals.filter(s => !s.early && s.category && (p.twMode === 'both' || s.category === p.twMode));
        // All event metadata is captured at confirmation, never read from final active zones.
        let eventIndex = 0, position = null;
        const ready = Number.isFinite(p.warmupBars) ? tradeStart : Math.max(tradeStart, (h[0]?.timestamp ?? Infinity) + (c.pivotLeft + c.pivotRight + 1) * TW.TF[c.pivotTimeframe]);
        function pnlAt(pos, rawPrice, ts) {
            const fill = rawPrice * (1 - pos.side * slip);
            const fees = pos.qty * (pos.entryPrice + fill) * commission;
            const fundingCost = pos.side * pos.qty * pos.entryPrice * funding * Math.max(0, ts - pos.entryTs) / (480 * MINUTE);
            const gross = pos.side * pos.qty * (fill - pos.entryPrice);
            return { fill, fees, fundingCost, pnl: Math.max(-pos.margin, gross - fees - fundingCost) };
        }
        function close(rawPrice, ts, reason) {
            const pos = position, money = pnlAt(pos, rawPrice, ts);
            capital += money.pnl;
            trades.push({ ...pos, type: pos.side === 1 ? 'Long' : 'Short', exitTs: ts, exitPrice: money.fill,
                pnlAbs: money.pnl, pnlPerc: money.pnl / pos.margin * 100, reason, capital,
                fees: money.fees, fundingCost: money.fundingCost,
                realizedR: money.pnl / (pos.qty * Math.abs(pos.entryPrice - pos.sl)) });
            position = null;
        }
        for (let i = 0; i < part.length; i++) {
            const [ts, open, high, low, closing] = part[i];
            const candidates = [];
            while (eventIndex < events.length && events[eventIndex].time <= ts) {
                const e = events[eventIndex++];
                if (e.time < ts) { if (e.time >= ready) rejected.stale++; continue; }
                if (ts >= ready) candidates.push(e);
            }
            if (ts < ready) continue;
            evaluatedMinutes++;
            const eligible = candidates.filter(s => s.side === 'bull' ? p.enableLongs !== false : p.enableShorts !== false);
            signalCount += eligible.length;
            if (position) rejected.occupied += eligible.length;
            else if (eligible.length && capital > 0) {
                if (new Set(eligible.map(e => e.side)).size > 1) rejected.conflict += eligible.length;
                else {
                    const e = eligible.find(e => e.category === 'double') || eligible[0];
                    const side = e.side === 'bull' ? 1 : -1;
                    const entryPrice = open * (1 + side * slip);
                    const sl = e.stopReference * (1 - side * p.twStopBufferPerc / 100);
                    const risk = side * (entryPrice - sl), riskPerc = risk / entryPrice * 100;
                    const tp = entryPrice + side * risk * p.twRewardRisk;
                    const liquidation = entryPrice * (1 - side / leverage);
                    if (!Number.isFinite(risk) || risk <= 0 || sl <= 0 || tp <= 0 || riskPerc < p.twMinStopPerc || riskPerc > p.twMaxStopPerc || (leverage > 1 && side * (sl - liquidation) <= 0)) rejected.risk++;
                    else {
                        const margin = sizePosition({ ...p, initialCapital: capitalInitial }, capital, []);
                        if (!Number.isFinite(margin) || margin <= 0) rejected.capital++;
                        else position = { side, entryTs: ts, entryPrice, sl, tp, margin, qty: margin * leverage / entryPrice,
                            signalTs: e.time, entryReason: e.text, mode: e.category, zone: e.zone || null,
                            neckline: e.neckline ?? null, stopReference: e.stopReference, liquidation,
                            sample: ts >= splitTs && p.twValidationPct > 0 ? 'validation' : 'development' };
                    }
                }
            }
            if (position) {
                const pos = position, long = pos.side === 1;
                // Opening gaps execute at the available open, never at a fictitious stop fill.
                if (leverage > 1 && (long ? open <= pos.liquidation : open >= pos.liquidation)) close(open, ts, 'LIQ');
                else if (long ? open <= pos.sl : open >= pos.sl) close(open, ts, 'SL');
                else if (long ? open >= pos.tp : open <= pos.tp) close(pos.tp, ts, 'TP');
                else if (long ? low <= pos.sl : high >= pos.sl) close(pos.sl, ts + MINUTE, 'SL');
                else if (long ? high >= pos.tp : low <= pos.tp) close(pos.tp, ts + MINUTE, 'TP');
                else if (p.twMaxHoldMinutes > 0 && ts + MINUTE - pos.entryTs >= p.twMaxHoldMinutes * MINUTE) close(closing, ts + MINUTE, 'Tiempo');
            }
            const value = capital + (position ? pnlAt(position, closing, ts + MINUTE).pnl : 0);
            mark(value, ts + MINUTE, i % step === 0 || i === part.length - 1);
        }
        if (position) {
            if (segmentIndex < segments.length - 1) throw new Error(`TW: una operación abierta el ${new Date(position.entryTs).toISOString()} atraviesa un hueco de datos 1m. No se calculan métricas con una salida desconocida; elegí otra fuente o período.`);
            close(part.at(-1)[4], part.at(-1)[0] + MINUTE, 'Fin');
            mark(capital, part.at(-1)[0] + MINUTE, true);
        }
    }
    const stats = { ...summarize(trades), netProfit: capital - capitalInitial, netProfitPerc: (capital - capitalInitial) / capitalInitial * 100, finalCapital: capital, maxDrawdownPerc: maxDD };
    const breakdown = {};
    for (const mode of ['rejection', 'double']) for (const type of ['Long', 'Short']) breakdown[`${mode}_${type}`] = summarize(trades.filter(t => t.mode === mode && t.type === type));
    return { stats, trades: trades.slice(-Math.max(1, p.maxTradesDevueltos || 300)), equity, warnings,
        tw: { mode: p.twMode, breakdown, rejected, signalCount, pivotBars, patternBars, evaluatedMinutes,
            tradeStart, dataEnd: lastEnd, splitTs, validationPct: p.twValidationPct,
            samples: { development: summarize(trades.filter(t => t.sample === 'development')), validation: summarize(trades.filter(t => t.sample === 'validation')) },
            assumptions: 'Entrada en apertura 1m posterior al cierre; stop primero si SL y TP coinciden; slippage en ambos fills; funding constante prorrateado; liquidación aproximada sin mantenimiento; drawdown a cierre 1m.' } };
}
module.exports = { normalizeTW, warmupMs, aggregateComplete, summarize, runTWBacktest };
