const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const { runBacktest, normalizarParams } = require('../lib/backtest-core');
const { aggregateComplete, warmupMs } = require('../lib/tw-backtest');
const TW = require('../public/tw-pivots');
const M = 60000;
const b = (i,o,h,l,c) => [i*M,o,h,l,c,1,(i+1)*M-1,0,0,.5];
const cfg = { pivotTimeframe:'1m', patternTimeframe:'1m', pivotLeft:1, pivotRight:1, requirePatternNearZone:false, useHammer:false, useShootingStar:false, useBearEngulfing:false };
const params = extra => ({ ...normalizarParams({ strategyType:'tw_mtf', twConfig:cfg, twStopBufferPerc:0, twMinStopPerc:.01, twMaxStopPerc:50, commission:0, slippagePerc:0, fundingPerc:0, ...extra }), warmupBars:0 });
const run = (rows, extra = {}) => runBacktest(rows, [], [], [], params(extra), [], []);
const start = [b(0,101,102,98,99),b(1,98,104,97,103)];

test('next open fill, structural stop, R target and conservative intrabar ordering', () => {
    const r = run([...start,b(2,100,107,96,101)]);
    assert.equal(r.trades.length,1);
    const t = r.trades[0];
    assert.equal(t.signalTs,2*M); assert.equal(t.entryTs,2*M); assert.equal(t.entryPrice,100);
    assert.equal(t.sl,97); assert.equal(t.tp,106); assert.equal(t.reason,'SL');
    assert.equal(t.pnlAbs,-30); assert.equal(r.stats.finalCapital,970);
    assert.equal(t.entryReason,'Envolvente alcista');
});
test('TP, opening stop gap and opening target gap use chronological fills', () => {
    assert.equal(run([...start,b(2,100,107,98,102)]).trades[0].reason,'TP');
    const stopGap = run([...start,b(2,100,101,99,100),b(3,90,92,89,91)]).trades[0];
    assert.equal(stopGap.exitPrice,90); assert.equal(stopGap.reason,'SL');
    const targetGap = run([...start,b(2,100,101,99,100),b(3,108,110,96,100)]).trades[0];
    assert.equal(targetGap.exitPrice,106); assert.equal(targetGap.reason,'TP');
});
test('fees and slippage are charged exactly once per side, funding by elapsed time', () => {
    const r = run([...start,b(2,100,101,99,100)],{ commission:.1,slippagePerc:.1,fundingPerc:.48 });
    const t = r.trades[0];
    assert.equal(t.entryPrice,100.1); assert.equal(t.exitPrice,99.9);
    const qty = 1000/100.1, fees = qty*200*.001, funding = 1000*.0048/480;
    assert.ok(Math.abs(t.fees-fees)<1e-9);
    assert.ok(Math.abs(t.fundingCost-funding)<1e-9);
    assert.ok(Math.abs(t.pnlAbs - (qty*(99.9-100.1)-fees-funding))<1e-9);
    assert.equal(r.equity.at(-1).v,r.stats.finalCapital);
});
test('single position and risk/direction filters reject entries', () => {
    assert.equal(run([...start,b(2,100,101,99,100)], { enableLongs:false }).trades.length,0);
    const r = run([...start,b(2,100,101,99,100)], { twMaxStopPerc:1 });
    assert.equal(r.trades.length,0); assert.equal(r.tw.rejected.risk,1);
    const leveraged = run([...start,b(2,100,101,99,100)], { palancaActivo:true,palancaValor:50 });
    assert.equal(leveraged.tw.rejected.risk,1); // Stop beyond approximate liquidation.
    const rows = [...start,b(2,100,101,99,100),b(3,100,101,98,99),b(4,98,102,98,101),b(5,101,102,100,101)];
    const one = run(rows);
    assert.equal(one.trades.length,1); assert.equal(one.tw.rejected.occupied,1);
});
test('missing data cannot manufacture a profitable close or forward-fill a signal', () => {
    assert.throws(() => run([...start,b(2,100,101,99,100),b(4,100,101,99,100)]), /hueco de datos/);
    const skipped = run([...start,b(3,100,107,98,102)]);
    assert.equal(skipped.trades.length,0);
    assert.ok(skipped.warnings.some(w=>w.includes('huecos')));
});
test('MTF aggregation rejects partial buckets at boundaries and gaps', () => {
    const rows = Array.from({length:10},(_,i)=>b(i,100,102,98,101));
    assert.equal(aggregateComplete(rows,5*M).length,2);
    assert.equal(aggregateComplete(rows.slice(1),5*M).length,1);
    assert.equal(aggregateComplete(rows.filter(r=>r[0]!==2*M),5*M).length,1);
    assert.equal(aggregateComplete(rows.slice(0,9),5*M).length,1);
});
test('double patterns enter only after neckline confirmation with frozen extreme', () => {
    const rows = [b(0,100,102,98,100),b(1,101,110,99,102),b(2,101,103,95,100),b(3,101,110,99,102),b(4,101,103,97,100),b(5,96,98,92,94),b(6,94,96,60,80)];
    const options = { twMode:'double',twConfig:{...cfg,useDoubleTop:true,doubleMinSeparation:2,confirmDoubleWithNeckline:false} };
    assert.equal(run(rows.slice(0,5),options).trades.length,0);
    const r = run(rows,options), t = r.trades[0];
    assert.equal(t.mode,'double'); assert.equal(t.type,'Short'); assert.equal(t.entryTs,6*M);
    assert.equal(t.sl,110); assert.equal(t.neckline,95); assert.equal(t.reason,'TP');
    assert.equal(r.tw.breakdown.double_Short.totalTrades,1);
});
test('zone snapshot survives later invalidation and bounds structural stop', () => {
    const h = [b(0,100,102,98,100),b(1,100,110,90,100),b(2,100,103,97,100),b(3,91,92,90,90.5),b(4,90,93,89.9,92),b(5,92,93,85,86)];
    const config = {...cfg,requirePatternNearZone:true};
    const events = TW.calculate(h,h,config,6*M);
    const signal = events.signals.find(s=>s.category==='rejection');
    assert.ok(signal.zone); assert.equal(signal.zone.price,90);
    assert.equal(signal.stopReference,90*(1-.25/100));
    assert.ok(!events.zones.some(z=>z.price===90));
    assert.equal(run(h,{twConfig:config}).trades[0].zone.price,90);
});
test('future suffix cannot modify a completed trade or its signal', () => {
    const prefix = [...start,b(2,100,107,98,102)];
    const t1 = run(prefix).trades[0], t2 = run([...prefix,b(3,102,104,100,103),b(4,103,104,101,102)]).trades[0];
    for (const key of ['entryTs','signalTs','entryPrice','sl','tp','exitPrice','pnlAbs','reason']) assert.equal(t1[key],t2[key]);
});
test('normalization preserves TW and does not silently apply classic filters', () => {
    const p = normalizarParams({strategyType:'tw_mtf',useRsiFilter:true,allowMultipleEntries:true,twRewardRisk:3});
    assert.equal(p.twRewardRisk,3); assert.equal(p.useRsiFilter,false); assert.equal(p.allowMultipleEntries,false);
    assert.equal(normalizarParams({}).strategyType,'classic');
    assert.equal(normalizarParams({}).useRsiFilter,true);
    assert.ok(warmupMs({...p,twMode:'double'})>warmupMs({...p,twMode:'rejection'}));
    assert.throws(()=>run(start,{twMinStopPerc:10,twMaxStopPerc:1}),/stop mínimo/);
});
test('time exit, end close and temporal sample totals reconcile', () => {
    const r = run([...start,b(2,100,101,99,100),b(3,100,101,99,100)],{twMaxHoldMinutes:1,twValidationPct:50});
    assert.equal(r.trades[0].reason,'Tiempo');
    assert.equal(r.tw.samples.development.totalTrades+r.tw.samples.validation.totalTrades,r.stats.totalTrades);
    assert.equal(run([...start,b(2,100,101,99,100)]).trades[0].reason,'Fin');
});
test('saved strategy scripts parse and integration guards live execution', () => {
    const html=fs.readFileSync(require.resolve('../public/estrategias.html'),'utf8');
    for(const m of html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g)) new vm.Script(m[1]);
    new vm.Script(fs.readFileSync(require.resolve('../public/tw-strategy-ui.js'),'utf8'));
    assert.match(html,/restoreTWStrategy\(p\)/);
    assert.match(html,/\.\.\.readTWStrategy\(\)/);
    assert.match(fs.readFileSync(require.resolve('../server.js'),'utf8'),/TW MTF disponible solo para backtest/);
});
