/* TW MTF: pure, causal engine shared by the browser and Node tests. */
(function (root) {
    'use strict';
    const TF = { '1m': 60000, '5m': 300000, '15m': 900000, '1h': 3600000, '4h': 14400000, '1d': 86400000 };
    // [label, default, min, max, step]; booleans and timeframes need no bounds.
    const fields = {
        usePivotZones: ['Usar zonas Pivot', true], pivotTimeframe: ['Temporalidad pivots', '1h'],
        pivotLeft: ['Velas izquierda', 5, 1, 100, 1], pivotRight: ['Velas derecha', 5, 1, 100, 1],
        maxActiveZones: ['Zonas por lado', 8, 1, 20, 1], zoneWidthPerc: ['Ancho zona ± %', .25, 0, 10, .05],
        breakMarginPerc: ['Margen ruptura %', .1, 0, 10, .05], invalidateOnClose: ['Invalidar por cierre HTF', true],
        showResistanceZones: ['Mostrar resistencias', true], showSupportZones: ['Mostrar soportes', true],
        patternTimeframe: ['Temporalidad patrones', '5m'], requirePatternNearZone: ['Exigir cercanía a zona', true],
        useExtraProximityFilter: ['Margen adicional de cercanía', true], patternProximityPerc: ['Cercanía adicional %', .2, 0, 10, .05],
        oneSignalPerVisit: ['Una señal por visita', true], useBullEngulfing: ['Envolvente alcista', true],
        useBearEngulfing: ['Envolvente bajista', true], useHammer: ['Martillo', true], useShootingStar: ['Estrella fugaz', true],
        minEngulfBodyPerc: ['Cuerpo mínimo envolvente %', 40, 0, 100, 5], wickBodyRatio: ['Mecha principal / cuerpo', 2, 1, 20, .1],
        oppositeWickMaxRatio: ['Mecha opuesta / cuerpo máximo', .5, 0, 20, .1], maxBodyRangePerc: ['Cuerpo máximo martillo %', 40, 1, 100, 1],
        useDoubleTop: ['Doble techo', true], useDoubleBottom: ['Doble piso', true],
        doubleTolerancePerc: ['Tolerancia extremos %', .5, 0, 10, .05], doubleMinSeparation: ['Separación mínima HTF', 5, 1, 500, 1],
        doubleMaxSeparation: ['Separación máxima HTF', 50, 2, 1000, 1], doubleMinRetracePerc: ['Retroceso mínimo %', 1, 0, 100, .1],
        showEarlyDouble: ['Mostrar candidatos DT/DP', true], confirmDoubleWithNeckline: ['Confirmar por neckline', true],
        showDoubleNeckline: ['Mostrar neckline activa', true], doubleInvalidationPerc: ['Invalidación DT/DP %', .2, 0, 10, .05]
    };
    const defaults = Object.fromEntries(Object.entries(fields).map(([k, f]) => [k, f[1]]));
    function normalize(input = {}) {
        if (!input || typeof input !== 'object') input = {};
        const c = {};
        for (const [k, f] of Object.entries(fields)) {
            const v = input[k];
            c[k] = typeof f[1] === 'boolean' ? (typeof v === 'boolean' ? v : f[1])
                : typeof f[1] === 'string' ? (TF[v] ? v : f[1])
                : Number.isFinite(Number(v)) && v !== null && v !== '' ? Math.max(f[2], Math.min(f[3], f[4] === 1 ? Math.round(Number(v)) : Number(v))) : f[1];
        }
        c.doubleMaxSeparation = Math.max(c.doubleMinSeparation, c.doubleMaxSeparation);
        return c;
    }
    function candles(rows, tf, now) {
        const map = new Map();
        for (const r of rows) {
            const b = Array.isArray(r) ? { timestamp: +r[0], open: +r[1], high: +r[2], low: +r[3], close: +r[4] } : r;
            if (!b || !['timestamp','open','high','low','close'].every(k => Number.isFinite(b[k]))) continue;
            if (b.low <= 0 || b.high < Math.max(b.open, b.close) || b.low > Math.min(b.open, b.close)) continue;
            if (b.timestamp + tf <= now) map.set(b.timestamp, b);
        }
        return [...map.values()].sort((a, b) => a.timestamp - b.timestamp);
    }
    function calculate(pivotRows, patternRows, input = {}, now = Date.now()) {
        const c = normalize(input), hms = TF[c.pivotTimeframe], pms = TF[c.patternTimeframe];
        const h = candles(pivotRows, hms, now), p = candles(patternRows, pms, now);
        const zones = { support: [], resistance: [] }, signals = [], active = { top: null, bottom: null };
        const previous = { high: null, low: null };
        const emit = (time, price, side, text, early = false, detail = {}) => signals.push({ time, price, side, text, early, ...detail });
        function pivot(i, high) {
            const k = i - c.pivotRight, key = high ? 'high' : 'low';
            if (k < c.pivotLeft) return null;
            for (let j = k - c.pivotLeft; j <= i; j++) {
                if (j > k - c.pivotLeft && h[j].timestamp - h[j - 1].timestamp !== hms) return null;
                // Resolve plateaus to their rightmost extreme, as Pine pivots do.
                if (j !== k && (high ? (j < k ? h[j][key] > h[k][key] : h[j][key] >= h[k][key]) : (j < k ? h[j][key] < h[k][key] : h[j][key] <= h[k][key]))) return null;
            }
            return { price: h[k][key], time: h[k].timestamp, index: k };
        }
        function processPivot(v, high, time) {
            if (!v) return;
            const side = high ? 'resistance' : 'support', kind = high ? 'top' : 'bottom', key = high ? 'high' : 'low';
            if (c.usePivotZones) {
                zones[side].push({ price: v.price, lower: v.price * (1 - c.zoneWidthPerc / 100), upper: v.price * (1 + c.zoneWidthPerc / 100), start: time, pivotTime: v.time, side, used: false });
                if (zones[side].length > c.maxActiveZones) zones[side].shift();
            }
            const old = previous[key];
            if (old && (high ? c.useDoubleTop : c.useDoubleBottom)) {
                const sep = (v.time - old.time) / hms;
                const between = h.slice(old.index + 1, v.index);
                if (sep >= c.doubleMinSeparation && sep <= c.doubleMaxSeparation && between.length && Math.abs(v.price - old.price) / old.price * 100 <= c.doubleTolerancePerc) {
                    // Actual intervening extreme, not a possibly unrelated last opposite pivot.
                    const neck = high ? Math.min(...between.map(b => b.low)) : Math.max(...between.map(b => b.high));
                    const ref = high ? Math.min(v.price, old.price) : Math.max(v.price, old.price);
                    const retrace = (high ? ref - neck : neck - ref) / ref * 100;
                    if (retrace >= c.doubleMinRetracePerc) {
                        const a = { neckline: neck, level: high ? Math.max(v.price, old.price) : Math.min(v.price, old.price), start: time, side: high ? 'bear' : 'bull', kind };
                        const label = high ? 'Doble techo' : 'Doble piso';
                        if (!c.confirmDoubleWithNeckline) emit(time, v.price, a.side, label);
                        else if (c.showEarlyDouble) emit(time, v.price, a.side, label + ' candidato', true);
                        active[kind] = c.confirmDoubleWithNeckline ? a : null;
                    }
                }
            }
            previous[key] = v;
        }
        function processHTF(i) {
            const b = h[i], time = b.timestamp + hms;
            processPivot(pivot(i, true), true, time);
            processPivot(pivot(i, false), false, time);
            if (c.invalidateOnClose) {
                zones.resistance = zones.resistance.filter(z => b.close <= z.upper * (1 + c.breakMarginPerc / 100));
                zones.support = zones.support.filter(z => b.close >= z.lower * (1 - c.breakMarginPerc / 100));
            }
            for (const kind of ['top', 'bottom']) {
                const a = active[kind];
                if (!a) continue;
                const top = kind === 'top';
                const invalid = top ? b.close > a.level * (1 + c.doubleInvalidationPerc / 100) : b.close < a.level * (1 - c.doubleInvalidationPerc / 100);
                if (invalid) { active[kind] = null; continue; }
                if (top ? b.close < a.neckline : b.close > a.neckline) {
                    emit(time, b.close, a.side, top ? 'Doble techo confirmado' : 'Doble piso confirmado', false,
                        { category: 'double', stopReference: a.level, neckline: a.neckline, patternStart: a.start });
                    active[kind] = null;
                }
            }
        }
        function nearest(b, side) {
            let match = null, distance = Infinity;
            for (const z of zones[side]) {
                const extra = c.useExtraProximityFilter ? z.price * c.patternProximityPerc / 100 : 0;
                const near = b.high >= z.lower - extra && b.low <= z.upper + extra;
                if (!near) z.used = false;
                const d = Math.abs((side === 'support' ? b.low : b.high) - z.price) / z.price;
                if (near && d < distance) { match = z; distance = d; }
            }
            return match;
        }
        let hi = 0;
        for (let i = 0; i < p.length; i++) {
            const b = p[i], time = b.timestamp + pms;
            while (hi < h.length && h[hi].timestamp + hms <= time) processHTF(hi++);
            const s = nearest(b, 'support'), r = nearest(b, 'resistance');
            const prev = i && p[i - 1].timestamp + pms === b.timestamp ? p[i - 1] : null;
            const body = Math.abs(b.close - b.open), range = b.high - b.low, pct = range > 0 ? body / range * 100 : 0;
            const upper = b.high - Math.max(b.open, b.close), lower = Math.min(b.open, b.close) - b.low;
            const bull = [], bear = [];
            if (prev && pct >= c.minEngulfBodyPerc) {
                if (c.useBullEngulfing && prev.close < prev.open && b.close > b.open && b.open <= prev.close && b.close >= prev.open) bull.push('Envolvente alcista');
                if (c.useBearEngulfing && prev.close > prev.open && b.close < b.open && b.open >= prev.close && b.close <= prev.open) bear.push('Envolvente bajista');
            }
            if (body > 0 && pct <= c.maxBodyRangePerc) {
                if (c.useHammer && lower >= body * c.wickBodyRatio && upper <= body * c.oppositeWickMaxRatio) bull.push('Martillo');
                if (c.useShootingStar && upper >= body * c.wickBodyRatio && lower <= body * c.oppositeWickMaxRatio) bear.push('Estrella fugaz');
            }
            for (const [labels, z, side, price] of [[bull, s, 'bull', b.low], [bear, r, 'bear', b.high]]) {
                if (labels.length && (!c.requirePatternNearZone || z) && (!c.oneSignalPerVisit || !z || !z.used)) {
                    emit(time, price, side, labels.join(' + '), false, {
                        category: 'rejection', stopReference: side === 'bull' ? Math.min(b.low, z ? z.lower : b.low) : Math.max(b.high, z ? z.upper : b.high),
                        zone: z ? { price: z.price, lower: z.lower, upper: z.upper, start: z.start, side: z.side } : null
                    });
                    if (z) z.used = true;
                }
            }
        }
        while (hi < h.length) processHTF(hi++);
        signals.sort((a, b) => a.time - b.time);
        return { zones: [...zones.support, ...zones.resistance], signals, necklines: Object.values(active).filter(Boolean), pivotBars: h.length, patternBars: p.length };
    }
    const api = { TF, fields, defaults, normalize, calculate };
    if (typeof module !== 'undefined' && module.exports) module.exports = api;
    else root.TWPivots = api;
})(typeof globalThis !== 'undefined' ? globalThis : this);
