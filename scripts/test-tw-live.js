const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const { evaluateTW, levels } = require('../lib/tw-live');
const { normalizarParams, runBacktest } = require('../lib/backtest-core');
const M = 60000;
const b = (i,o=100,h=101,l=99,c=100) => [i*M,o,h,l,c,1,(i+1)*M-1];
const rows = [b(0),b(1),b(2),b(3),b(4,101,102,98,99),b(5,98,104,97,103)];
const config = { pivotTimeframe:'1m',patternTimeframe:'1m',pivotLeft:1,pivotRight:1,requirePatternNearZone:false,useHammer:false,useShootingStar:false,useBearEngulfing:false };
const params = extra => normalizarParams({strategyType:'tw_mtf',twConfig:config,twStopBufferPerc:0,twMinStopPerc:.01,twMaxStopPerc:50,...extra});

test('live event and structural levels agree with backtest, excluding unclosed candles', () => {
    const p = params({commission:0,slippagePerc:0,fundingPerc:0});
    const live = evaluateTW({bars1m:[...rows,b(6,103,200,1,150)]},p,6*M+1000);
    const back = runBacktest([...rows,b(6,103,120,98,110)],[],[],[],{...p,warmupBars:0},[],[]).trades.at(-1);
    assert.equal(live.signal,'long');
    for (const key of ['sl','tp','signalTs']) assert.equal(live[key],back[key]);
    assert.equal(live.event.text,back.entryReason);
    assert.equal(evaluateTW({bars1m:rows},p,7*M).signal,null);
    assert.equal(evaluateTW({bars1m:rows},params({enableLongs:false}),6*M).signal,null);
    assert.equal(evaluateTW({bars1m:rows},params({twMode:'double'}),6*M).signal,null);
    assert.equal(evaluateTW({bars1m:rows},params({twMaxStopPerc:1}),6*M).signal,null);
});

test('gaps, contradictory candles and missing timeframe data cannot create trades', () => {
    assert.equal(evaluateTW({bars1m:rows.filter(r=>r[0]!==3*M)},params(),6*M).signal,null);
    assert.throws(()=>evaluateTW({bars1m:[...rows,b(5)]},params(),6*M),/contradictorias/);
    assert.equal(evaluateTW({bars1m:rows},params({twConfig:{...config,pivotTimeframe:'1h'}}),6*M).signal,null);
});

test('fill risk preserves stop and R; rejects crossed stops and liquidation', () => {
    const event = {side:'bull',stopReference:97};
    assert.deepEqual(levels(event,100,params()),{entry:100,sl:97,tp:106,riskPerc:3});
    assert.equal(levels(event,96,params()),null);
    assert.equal(levels(event,100,params({palancaActivo:true,palancaValor:50})),null);
    const short = levels({side:'bear',stopReference:110},100,params());
    assert.equal(short.sl,110); assert.equal(short.tp,80);
});

// Execute the actual server functions without booting its DB, timers, sockets or APIs.
const source = fs.readFileSync(require.resolve('../server'),'utf8');
function section(start,end) { return source.slice(source.indexOf(start),source.indexOf(end,source.indexOf(start))); }
function harness(exchange='binance', extra={}) {
    const positions=[], calls=[], sql=[];
    let claimed=0, saved=params(extra), clock=6*M+1000;
    const trade = {
        qtyStep:.001,minNotionalUsdt:5,
        placeEntryOrder:async(ctx,order)=>{calls.push({kind:'entry',exchange:ctx.exchange,...order});return {ok:true,avgPrice:104,orderId:'entry'};},
        placeStopOrder:async(ctx,order)=>{calls.push({kind:'stop',exchange:ctx.exchange,...order});return {ok:true,orderId:order.tipo};}
    };
    const context = vm.createContext({
        console:{log(){},error(){},warn(){}}, Date:{now:()=>clock}, setTimeout,
        normalizarParams,twLevels:levels,evaluateTW,
        evaluarSenal:(v,w,p)=>evaluateTW(v,p,clock),
        ctxDeCuenta:r=>({uid:r.usuario_id,exchange:r.exchange,exchangeDatos:r.exchange_datos||r.exchange}),
        ctxActivos:new Map(),posDe:()=>positions,exch:{name:exchange},
        getExchange:()=>({trade}),
        pool:{query:async(q,args)=>{
            sql.push({q,args});
            if(q.startsWith('SELECT params'))return {rows:saved?[{params:saved}]:[]};
            if(q.includes('SET ultima_tw_signal_ts')){if(claimed>=args[0])return {rows:[]};claimed=args[0];return {rows:[{usuario_id:1}]};}
            if(q.includes('INSERT INTO auto_trading_entradas'))return {rows:[{id:1}]};
            return {rows:[]};
        }},
        fetchKlinesBatch:async()=>rows,
        precioLiveDeCtx:async()=>103,precioDeCtx:()=>103,precioEjecucionDeCtx:async()=>104,
        esEjecucionCruzada:c=>c.exchangeDatos!==c.exchange,
        calcularNocionalEntrada:async(ctx,p)=>{calls.push({kind:'sizing',p});return {ok:true,nocional:1000};},
        setBinanceLeverage:async(ctx,lev)=>{calls.push({kind:'leverage',exchange:ctx.exchange,lev});return true;},
        asegurarConfiguracionCuenta:async()=>{},sincronizarPosicionBD:async()=>{},
        nivelProteccionExchange:(ctx,lado,nivel)=>nivel,
        cerrarSubPosicion:async(ctx,pos,reason)=>calls.push({kind:'close',reason}),
        N8N_WEBHOOK_URL:null,
    });
    vm.runInContext(section('function tradeDe(ctx)', '// Cancela una orden'),context);
    vm.runInContext(section('async function colocarProteccionExchange(', '// Cierra UNA sub-posición'),context);
    vm.runInContext(section('async function gestionarPosicionAbierta(', '// Devuelve el NOCIONAL'),context);
    vm.runInContext(section('async function procesarCuenta(', '// Arrancar loop'),context);
    const row={usuario_id:1,habilitado:true,exchange,estrategia_nombre:'Mi TW',ultima_senal:'long'};
    return {context,positions,calls,sql,row,run:()=>context.procesarCuenta(row,{bars1m:rows}),
        setSaved:p=>{saved=p;},setClock:n=>{clock=n;}};
}

for(const exchange of ['binance','bingx']) test(`AutoBot ${exchange}: saved parameters, fill, protection and persistent event claim`,async()=>{
    const h=harness(exchange,{twRewardRisk:3,twMaxHoldMinutes:15,useBreakeven:true,allowMultipleEntries:true,stopType:'Ruptura EMA'});
    await h.run();
    assert.equal(h.positions.length,1);
    const pos=h.positions[0];
    assert.equal(pos.stopType,'TW');assert.equal(pos.sl,97);assert.equal(pos.tp,125);
    assert.equal(pos.entry,104);assert.equal(pos.twMaxHoldMinutes,15);assert.equal(pos.beTrigger,null);
    assert.equal(h.calls.find(c=>c.kind==='sizing').p.allowMultipleEntries,false);
    assert.equal(h.calls.find(c=>c.kind==='leverage').lev,1);
    assert.equal(h.calls.filter(c=>c.kind==='entry').length,1);
    const stops=h.calls.filter(c=>c.kind==='stop');
    assert.equal(stops.length,2);assert.ok(stops.every(c=>c.exchange===exchange));
    assert.ok(stops.some(c=>c.tipo==='STOP_MARKET'&&c.stopPrice===97));
    assert.ok(h.sql.some(s=>s.q.includes('INSERT INTO auto_trading_entradas')&&s.args.at(-1)===15));
    await h.run(); // occupied
    h.positions.length=0; // exchange closed it, ultima_senal was reset, but persistent claim remains
    await h.run();
    assert.equal(h.calls.filter(c=>c.kind==='entry').length,1);
});

test('TW time exit survives strategy change/deletion and disabled account',async()=>{
    const h=harness();await h.run();
    h.positions[0].twMaxHoldMinutes=1;
    h.row.habilitado=false;h.setSaved(null);h.setClock(8*M);
    await h.run();
    assert.equal(h.positions.length,0);assert.equal(h.calls.at(-1).reason,'Tiempo');
});

test('classic exit settings do not close a TW position with no time limit',async()=>{
    const h=harness();await h.run();h.setClock(100*M);
    h.setSaved({strategyType:'classic',useMaxTradeTime:true,maxTradeMinutes:1});
    await h.run();assert.equal(h.positions.length,1);
});

test('leverage failure and expired signals never send an entry',async()=>{
    const h=harness();h.context.setBinanceLeverage=async()=>false;await h.run();
    assert.equal(h.calls.filter(c=>c.kind==='entry').length,0);
    const stale=harness();stale.context.setBinanceLeverage=async()=>{stale.setClock(7*M);return true;};await stale.run();
    assert.equal(stale.calls.filter(c=>c.kind==='entry').length,0);
});

test('cross-exchange execution keeps levels in decision market',async()=>{
    const h=harness('bingx');h.row.exchange_datos='binance';await h.run();
    assert.equal(h.positions[0].entry,104);assert.equal(h.positions[0].entryRef,103);
    assert.equal(h.positions[0].sl,97);assert.equal(h.positions[0].tp,115);
    assert.equal(h.calls.find(c=>c.kind==='entry').exchange,'bingx');
});

test('fill outside configured risk triggers immediate close',async()=>{
    const h=harness('binance',{twMaxStopPerc:6});await h.run();
    assert.equal(h.positions.length,0);
    assert.equal(h.calls.at(-1).reason,'TW Riesgo');
});

test('failed exchange protection triggers close instead of leaving an unprotected TW trade',async()=>{
    const h=harness();h.context.colocarOrdenStop=async()=>({ok:false});await h.run();
    assert.equal(h.positions.length,0);assert.equal(h.calls.at(-1).reason,'TW Protec');
});

test('double tops require neckline and use the original extreme for short protection',()=>{
    const data=[b(0,100,102,98,100),b(1,101,110,99,102),b(2,101,103,95,100),b(3,101,110,99,102),b(4,101,103,97,100),b(5,96,98,92,94)];
    const p=params({twMode:'double',twConfig:{...config,doubleMinSeparation:2,confirmDoubleWithNeckline:false}});
    assert.equal(evaluateTW({bars1m:data.slice(0,5)},p,5*M).signal,null);
    const r=evaluateTW({bars1m:data},p,6*M);
    assert.equal(r.signal,'short');assert.equal(r.sl,110);assert.equal(r.tp,62);
    assert.equal(r.event.neckline,95);
    assert.equal(evaluateTW({bars1m:data}, {...p,enableShorts:false},6*M).signal,null);
});

test('independent native timeframes and HTF closed boundaries are honored',()=>{
    const patterns=rows.map(r=>[r[0]*5,...r.slice(1,6),r[0]*5+5*M-1]);
    const minutes=Array.from({length:30},(_,i)=>b(i,103,104,102,103));
    const p=params({twConfig:{...config,pivotTimeframe:'5m',patternTimeframe:'5m'}});
    assert.equal(evaluateTW({bars1m:minutes,bars5m:patterns},p,30*M).signal,'long');
    assert.equal(evaluateTW({bars1m:minutes.slice(0,-1),bars5m:patterns},p,29*M).signal,null);
});

test('tick stop closes a structural TW position',async()=>{
    const h=harness();await h.run();
    h.context.ultimoPrecioPorMercado={};h.context.posicionesPorCuenta=new Map([[1,h.positions]]);
    h.context.mercadoDeCtx=()=> 'binance:real';
    vm.runInContext(section('async function chequearSalida(', '// Persiste el breakeven'),h.context);
    await h.context.chequearSalida(96,'bingx');assert.equal(h.positions.length,1);
    await h.context.chequearSalida(96,'binance:real');assert.equal(h.positions.length,0);
    assert.equal(h.calls.at(-1).reason,'SL');
});

test('AutoBot config accepts TW for each exchange and validates saved risk settings',async()=>{
    for(const exchange of ['binance','bingx']) {
        const h=harness(exchange);const writes=[];
        h.context.pool.query=async(q,args)=>{
            if(q.includes('SELECT api_key'))return {rows:[{api_key:'fake',exchange}]};
            if(q.startsWith('SELECT params'))return {rows:[{params:params()}]};
            writes.push({q,args});return {rows:[]};
        };
        h.context.descifrarSecreto=()=> 'fake';h.context.balanceDeCuenta=async()=>({wallet:1000});
        vm.runInContext(section('async function actualizarConfigAutotrading(', "app.put('/api/autotrading'"),h.context);
        let response,status=200;
        const res={status:n=>{status=n;return res;},json:r=>{response=r;}};
        await h.context.actualizarConfigAutotrading({usuario:{id:1},body:{habilitado:true,estrategia_nombre:'Mi TW',exchange_datos:exchange}},res);
        assert.equal(status,200);assert.equal(response.ok,true);
        assert.ok(writes.some(w=>w.q.includes('habilitado        = COALESCE')&&w.args[1]==='Mi TW'));
        h.context.normalizarParams=()=>params({twMinStopPerc:10,twMaxStopPerc:1});
        await h.context.actualizarConfigAutotrading({usuario:{id:1},body:{habilitado:true,estrategia_nombre:'Mi TW'}},res);
        assert.equal(status,400);assert.match(response.error,/stop mínimo/);
    }
});

test('real sizing function uses saved margin and leverage, with available balance checks',async()=>{
    const h=harness();
    h.context.balanceDeCuenta=async()=>({wallet:1000,disponible:500});
    vm.runInContext(section('async function calcularNocionalEntrada(', '// WebSocket de precio futuros'),h.context);
    const calculate=h.context.calcularNocionalEntrada;
    const ctx={exchange:'bingx'},row={capital_inicial_ref:2000,position_usdt:9999};
    for(const [tipo,valor,expected] of [['monto_fijo',100,300],['porc_capital_actual',10,300],['porc_capital_inicial',10,600]]) {
        const p=params({posicionTipo:tipo,posicionValor:valor,palancaActivo:true,palancaValor:3});
        const r=await calculate(ctx,p,row);assert.equal(r.ok,true);assert.equal(r.nocional,expected);
    }
    assert.equal((await calculate(ctx,params({posicionTipo:'monto_fijo',posicionValor:600}),row)).ok,false);
});
