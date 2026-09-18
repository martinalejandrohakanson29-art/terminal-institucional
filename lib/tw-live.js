'use strict';
const TW = require('../public/tw-pivots');
const { normalizeTW } = require('./tw-backtest');

function levels(event, entry, input) {
    const p = { ...input, ...normalizeTW(input) };
    const side = event.side === 'bull' ? 1 : -1;
    const sl = event.stopReference * (1 - side * p.twStopBufferPerc / 100);
    const risk = side * (entry - sl), riskPerc = risk / entry * 100;
    const tp = entry + side * risk * p.twRewardRisk;
    const leverage = p.palancaActivo ? Number(p.palancaValor) : 1;
    if (!(entry > 0) || !Number.isFinite(entry) || !Number.isFinite(risk) || risk <= 0 || sl <= 0 || tp <= 0 ||
        riskPerc < p.twMinStopPerc || riskPerc > p.twMaxStopPerc ||
        (leverage > 1 && riskPerc >= 100 / leverage)) return null;
    return { entry, sl, tp, riskPerc };
}

// Native closed candles keep long HTF configurations affordable (including daily pivots).
// Restart both streams after any hole, as the backtest does for missing 1m data.
function closed(rows, ms, now) {
    const map = new Map();
    for (const raw of rows || []) {
        const r = raw.slice(0, 5).map(Number);
        if (r[0] + ms > now) continue;
        if (!r.every(Number.isFinite) || r[0] % ms || r[3] <= 0 ||
            r[2] < Math.max(r[1], r[4]) || r[3] > Math.min(r[1], r[4])) continue;
        if (map.has(r[0]) && JSON.stringify(map.get(r[0])) !== JSON.stringify(r)) throw new Error('TW: velas duplicadas contradictorias');
        map.set(r[0], r);
    }
    return [...map.values()].sort((a, b) => a[0] - b[0]);
}
function evaluateTW(velas, input, now = Date.now()) {
    const p = { ...input, ...normalizeTW(input) }, c = p.twConfig;
    let entry = null, timestamp = null;
    const none = reason => ({ signal: null, reason, entry, timestamp, indicadores: {} });
    if (p.twMinStopPerc > p.twMaxStopPerc) return none('TW: stop mínimo mayor al máximo');
    const minute = Math.floor(now / 60000) * 60000;
    const streams = [...new Set(['1m', c.pivotTimeframe, c.patternTimeframe])];
    const data = {};
    let start = 0;
    for (const tf of streams) {
        const ms = TW.TF[tf], rows = closed(velas['bars' + tf], ms, now);
        if (!rows.length || rows.at(-1)[0] + ms !== Math.floor(now / ms) * ms) return none('TW: datos atrasados o insuficientes');
        for (let i = 1; i < rows.length; i++) if (rows[i][0] !== rows[i - 1][0] + ms) start = Math.max(start, rows[i][0]);
        data[tf] = rows;
    }
    entry = data['1m'].at(-1)[4]; timestamp = data['1m'].at(-1)[0];
    const h = data[c.pivotTimeframe].filter(r => r[0] >= start);
    const patterns = data[c.patternTimeframe].filter(r => r[0] >= start);
    if (h.length < c.pivotLeft + c.pivotRight + 2 || patterns.length < 2) return none('TW: calentamiento insuficiente');
    const result = TW.calculate(h, patterns, { ...c, confirmDoubleWithNeckline: true, showEarlyDouble: false }, now);
    const candidates = result.signals.filter(s => !s.early && s.category && s.time === minute &&
        (p.twMode === 'both' || s.category === p.twMode) &&
        (s.side === 'bull' ? p.enableLongs !== false : p.enableShorts !== false));
    if (!candidates.length) return none('TW: sin nueva señal confirmada');
    if (new Set(candidates.map(s => s.side)).size > 1) return none('TW: señales opuestas simultáneas');
    const event = candidates.find(s => s.category === 'double') || candidates[0];
    const prices = levels(event, data['1m'].at(-1)[4], p);
    if (!prices) return none('TW: riesgo fuera de límites');
    return { signal: event.side === 'bull' ? 'long' : 'short', ...prices, timestamp,
        signalTs: event.time, event, reason: event.text, indicadores: { strategyType: 'tw_mtf', mode: event.category } };
}
module.exports = { evaluateTW, levels };
