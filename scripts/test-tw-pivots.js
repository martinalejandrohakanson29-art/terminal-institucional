const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const TW = require('../public/tw-pivots');
const M = 60000;
const cfg = { pivotTimeframe: '1m', patternTimeframe: '1m', pivotLeft: 1, pivotRight: 1, useDoubleTop: false, useDoubleBottom: false };
const bar = (i, open, high, low, close) => ({ timestamp: i * M, open, high, low, close });
const pivotBars = [bar(0, 100, 102, 98, 100), bar(1, 100, 110, 90, 100), bar(2, 100, 103, 97, 100)];

test('pivots are available only after right confirmation candle closes', () => {
    assert.equal(TW.calculate(pivotBars, [], cfg, 3 * M - 1).zones.length, 0);
    const r = TW.calculate(pivotBars, [], cfg, 3 * M);
    assert.equal(r.zones.length, 2);
    assert.ok(r.zones.every(z => z.start === 3 * M && z.pivotTime === M));
});
test('invalidation uses closed HTF close, not a wick or live candle', () => {
    const rows = [...pivotBars, bar(3, 100, 120, 99, 105), bar(4, 105, 120, 104, 115)];
    assert.ok(TW.calculate(rows, [], cfg, 5 * M - 1).zones.some(z => z.price === 110));
    assert.ok(!TW.calculate(rows, [], cfg, 5 * M).zones.some(z => z.price === 110));
    assert.ok(TW.calculate(rows, [], { ...cfg, invalidateOnClose: false }, 5 * M).zones.some(z => z.price === 110));
});
test('engulfing, hammer, shooting star and closed-candle filtering', () => {
    const p = [bar(0, 101, 102, 98, 99), bar(1, 98, 104, 97, 103), bar(2, 100, 101.2, 96, 101), bar(3, 101, 106, 99.8, 100)];
    const r = TW.calculate([], p, { ...cfg, requirePatternNearZone: false }, 4 * M);
    assert.deepEqual(r.signals.map(s => s.text), ['Envolvente alcista', 'Martillo', 'Estrella fugaz']);
    assert.equal(TW.calculate([], p, { ...cfg, requirePatternNearZone: false }, 4 * M - 1).signals.length, 2);
    assert.equal(TW.calculate([], p, cfg, 4 * M).signals.length, 0);
});
test('one signal per visit resets only when price leaves the zone', () => {
    const p = [bar(3, 90, 91.1, 87, 91), bar(4, 90, 91.1, 87, 91), bar(5, 100, 101, 99, 100), bar(6, 90, 91.1, 87, 91)];
    const r = TW.calculate(pivotBars, p, cfg, 7 * M);
    assert.deepEqual(r.signals.map(s => s.time), [4 * M, 7 * M]);
    assert.equal(TW.calculate(pivotBars, p, { ...cfg, oneSignalPerVisit: false }, 7 * M).signals.length, 3);
});
const tops = [bar(0, 100, 102, 98, 100), bar(1, 101, 110, 99, 102), bar(2, 101, 103, 95, 100), bar(3, 101, 110, 99, 102), bar(4, 101, 103, 97, 100), bar(5, 96, 98, 92, 94), bar(6, 94, 97, 92, 95)];
const doubleCfg = { ...cfg, useDoubleTop: true, useDoubleBottom: true, doubleMinSeparation: 2 };
test('double top candidate, neckline and single confirmation', () => {
    const early = TW.calculate(tops, [], doubleCfg, 5 * M);
    assert.equal(early.necklines[0].neckline, 95);
    assert.equal(early.signals[0].early, true);
    const r = TW.calculate(tops, [], doubleCfg, 7 * M);
    assert.equal(r.necklines.length, 0);
    assert.equal(r.signals.filter(s => s.text === 'Doble techo confirmado').length, 1);
    assert.equal(r.signals.at(-1).time, 6 * M);
});
test('double bottom is symmetric; invalidation prevents confirmation', () => {
    const bottoms = tops.map(b => ({ timestamp: b.timestamp, open: 200 - b.open, high: 200 - b.low, low: 200 - b.high, close: 200 - b.close }));
    assert.ok(TW.calculate(bottoms, [], doubleCfg, 7 * M).signals.some(s => s.text === 'Doble piso confirmado'));
    const broken = [...tops.slice(0, 5), bar(5, 105, 115, 104, 114), bar(6, 96, 98, 92, 94)];
    const r = TW.calculate(broken, [], doubleCfg, 7 * M);
    assert.ok(!r.signals.some(s => s.text.endsWith('confirmado')));
    assert.equal(r.necklines.length, 0);
});
test('switches, tolerance and retracement control double patterns', () => {
    assert.equal(TW.calculate(tops, [], { ...doubleCfg, doubleMinRetracePerc: 20 }, 7 * M).signals.length, 0);
    const r = TW.calculate(tops, [], { ...doubleCfg, confirmDoubleWithNeckline: false }, 7 * M);
    assert.equal(r.signals.length, 1);
    assert.equal(r.signals[0].text, 'Doble techo');
    assert.equal(r.necklines.length, 0);
});
test('future data cannot change past signals; inputs remain untouched', () => {
    const snapshot = JSON.stringify(tops);
    const past = TW.calculate(tops.slice(0, 5), [], doubleCfg, 5 * M);
    const future = TW.calculate(tops, [], doubleCfg, 7 * M);
    assert.deepEqual(future.signals.filter(s => s.time <= 5 * M), past.signals);
    assert.equal(JSON.stringify(tops), snapshot);
});
test('MTF ordering does not expose a pivot before HTF confirmation', () => {
    const h = pivotBars.map(b => ({ ...b, timestamp: b.timestamp * 5 }));
    const p = [bar(13, 90, 91.1, 87, 91), bar(15, 90, 91.1, 87, 91)];
    const r = TW.calculate(h, p, { ...cfg, pivotTimeframe: '5m' }, 16 * M);
    assert.deepEqual(r.signals.map(s => s.time), [16 * M]);
});
test('gaps, duplicates, invalid bars and bounded configuration', () => {
    const gap = [pivotBars[0], pivotBars[1], { ...pivotBars[2], timestamp: 4 * M }];
    assert.equal(TW.calculate(gap, [], cfg, 5 * M).zones.length, 0);
    assert.deepEqual(TW.calculate([...pivotBars, pivotBars[0], { timestamp: NaN }], [], cfg, 3 * M), TW.calculate(pivotBars, [], cfg, 3 * M));
    assert.equal(TW.normalize({ pivotLeft: -10, zoneWidthPerc: 0 }).pivotLeft, 1);
    assert.equal(TW.normalize({ zoneWidthPerc: 0 }).zoneWidthPerc, 0);
    assert.equal(TW.calculate(tops, [], { ...cfg, maxActiveZones: 1 }, 7 * M).zones.filter(z => z.side === 'resistance').length, 1);
});
test('browser integration: polling, drawing, exchange reset and stale request cancellation', async () => {
    const overlays = [], registrations = [], requests = [];
    let resolver;
    const rows = pivotBars.map(b => [b.timestamp, b.open, b.high, b.low, b.close]);
    const status = { style: {}, textContent: '' };
    const context = vm.createContext({ TWPivots: TW, console, AbortController,
        setTimeout: () => 1, clearTimeout: () => {},
        document: { getElementById: () => status },
        klinecharts: { registerOverlay: o => registrations.push(o) },
        chart: { removeOverlay: () => { overlays.length = 0; }, createOverlay: o => overlays.push(o) },
        selectExchange: { value: 'binance' }, velasActuales: [bar(0,100,110,90,100), bar(100,100,110,90,100)],
        renderChips: () => {},
        fetch: async url => { requests.push(url); return { ok: true, json: async () => rows }; }
    });
    vm.runInContext(fs.readFileSync(require.resolve('../public/tw-pivots-ui.js'), 'utf8'), context);
    vm.runInContext('twState.visible = true; twState.config = TWPivots.normalize({pivotLeft:1,pivotRight:1,pivotTimeframe:"1m",patternTimeframe:"1m"})', context);
    await vm.runInContext('twRefresh(0)', context);
    assert.equal(registrations.length, 3);
    assert.equal(overlays.filter(o => o.name === 'twZone').length, 2);
    assert.equal(requests.length, 1); // Same TF shares one fetch.
    assert.match(requests[0], /exchange=binance/);
    context.fetch = () => new Promise(resolve => { resolver = resolve; });
    const pending = vm.runInContext('twRefresh(0)', context);
    vm.runInContext('twStop()', context);
    resolver({ ok: true, json: async () => rows });
    await pending;
    assert.equal(overlays.length, 0);
    assert.equal(vm.runInContext('twState.result', context), null);
    context.fetch = async url => { requests.push(url); return { ok: true, json: async () => rows }; };
    vm.runInContext('selectExchange.value = "bingx"; twState.rows = {}', context);
    await vm.runInContext('twRefresh(twState.generation)', context);
    assert.match(requests.at(-1), /exchange=bingx/);
});
test('terminal inline scripts parse and TW is wired into list and layout', () => {
    const html = fs.readFileSync(require.resolve('../public/index.html'), 'utf8');
    for (const match of html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g)) new vm.Script(match[1]);
    assert.match(html, /id: 'TW_MTF'/);
    assert.match(html, /twVisible: twState.visible/);
    assert.match(html, /if \(twState.visible\) twStart\(\)/);
});
