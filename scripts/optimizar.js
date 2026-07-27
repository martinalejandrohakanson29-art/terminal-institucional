#!/usr/bin/env node
// ============================================================
// OPTIMIZADOR DE ESTRATEGIAS — CLI
// ============================================================
// Reemplaza la prueba manual de condicionales en /estrategias: en vez de tocar toggles de a
// uno y mirar el resultado, se define el espacio de búsqueda (lib/optim/espacio.js) y esto
// barre combinaciones con búsqueda bayesiana, valida fuera de muestra y devuelve un ranking
// de las que se sostienen.
//
// Uso:
//   node scripts/optimizar.js --dias 150 --trials 300
//   node scripts/optimizar.js --dias 150 --trials 400 --bloques completo --modo anclado
//   node scripts/optimizar.js --ayuda
//
// Lo que devuelve NO es "la mejor combinación del histórico" —esa siempre existe y casi
// siempre es una casualidad— sino las que siguen funcionando en ventanas que el optimizador
// nunca vio. Es normal y esperable que ese ranking venga vacío: significa que en ese espacio
// de búsqueda y ese período no hay nada que se sostenga, y saberlo vale más que operar la
// combinación que mejor calzó con el ruido.

require('dotenv').config({ quiet: true });
const path = require('path');
const { Pool } = require('pg');

const { construirEspacio, PRESETS_BLOQUES, fijosPorDefecto, aParamsBacktest } = require('../lib/optim/espacio');
const { cargarHistorico, generarVentanas, avisosDeCobertura, DIAS_CALENTAMIENTO } = require('../lib/optim/datos');
const { PoolEvaluadores } = require('../lib/optim/pool');
const { correrWalkForward } = require('../lib/optim/walkforward');
const { RegistroCSV, guardarReporte } = require('../lib/optim/registro');
const { OBJETIVOS } = require('../lib/optim/metricas');

// ── Argumentos ──────────────────────────────────────────────────────────────────────────
function parsearArgs(argv) {
    const args = {};
    for (let i = 0; i < argv.length; i++) {
        if (!argv[i].startsWith('--')) continue;
        const clave = argv[i].slice(2);
        const sig = argv[i + 1];
        if (sig === undefined || sig.startsWith('--')) args[clave] = true;
        else { args[clave] = sig; i++; }
    }
    return args;
}

const AYUDA = `
Optimizador de estrategias — búsqueda bayesiana + validación walk-forward

  --dias N          Días de histórico a usar             (default 150)
  --trials N        Evaluaciones por ventana in-sample   (default 300)
  --dias-is N       Largo de la ventana de optimización  (default 45)
  --dias-oos N      Largo de la ventana de validación    (default 15)
  --modo M          rolling | anclado                    (default rolling)
  --bloques B       ${Object.keys(PRESETS_BLOQUES).join(' | ')}  (default sin-aux)
                    o una lista separada por comas de bloques sueltos
  --objetivo O      ${Object.keys(OBJETIVOS).join(' | ')}   (default sharpe_dd)
  --top-n N         Combinaciones que pasan a validación por fold (default 12)
  --min-trades-mes N  Trades por cada 30 días por debajo de los cuales
                      se penaliza (escala al largo de cada ventana) (default 20)
  --dd-objetivo N   Drawdown % a partir del cual se penaliza (default 20)
  --palanca N       Palanca fija (no se optimiza)         (default 5)
  --sizing S        porc_capital_actual | porc_capital_inicial | monto_fijo
                    (default porc_capital_actual). Con porc_capital_inicial y
                    un % chico, las entradas múltiples pueden acumular posiciones
                    en pirámide; con 100% del capital actual nunca queda margen
                    libre para una segunda entrada y allowMultipleEntries no hace
                    nada.
  --posicion N      % del capital (o monto) por entrada    (default 100)
  --capital N       Capital inicial                       (default 1000)
  --exchange E      binance | bingx                       (default binance)
  --semilla N       Semilla del muestreo (reproducible)   (default 12345)
  --workers N       Hilos de evaluación                   (default núcleos-1)
  --sin-pruning     No abandonar trials a mitad de camino
  --salida DIR      Carpeta de resultados                 (default resultados/)
  --ayuda           Esto
`;

// ── Formato de salida por consola ───────────────────────────────────────────────────────
const dia = ms => new Date(ms).toISOString().slice(0, 10);
const pct = v => (Number.isFinite(v) ? `${v >= 0 ? '+' : ''}${v.toFixed(1)}%` : '—');
const n2 = v => (Number.isFinite(v) ? v.toFixed(2) : '—');

// Resume una combinación legible: qué filtros usa y con qué valores. Un dump de 60 parámetros
// no se lee; lo que importa es qué condicionales quedaron encendidos.
//
// Recibe los parámetros EFECTIVOS (los que salen de normalizarParams), no la config buscada.
// La diferencia no es cosmética: los bloques que un estudio no busca no aparecen en la config,
// pero el backtest igual corre con sus defaults, y varios están encendidos por default
// (alineamiento de EMAs, pullback, RSI y MACD). Describir la config buscada haría que un
// estudio con --bloques salidas reportara "sin filtros" una estrategia que corre con cuatro.
function describir(cfg) {
    const partes = [];
    partes.push(`TP ${cfg.tpPerc?.toFixed(2)}%`);
    partes.push(cfg.stopType === 'Porcentaje'
        ? `SL ${cfg.slPerc?.toFixed(2)}%`
        : `SL ruptura EMA ${(cfg.stopEMAs ?? []).map(e => `${e.period}${e.tf}`).join('+')}`);
    if (cfg.useBreakeven) partes.push(`BE ${cfg.breakevenTrigger?.toFixed(2)}/${cfg.breakevenOffset?.toFixed(2)}`);
    if (cfg.useMaxTradeTime) partes.push(`máx ${cfg.maxTradeMinutes}min`);
    if (cfg.startHour != null) partes.push(`${cfg.startHour}-${cfg.endHour}h`);
    if (cfg.useCooldown) partes.push(`cooldown ${cfg.cooldownMinutes}min`);
    const lados = [cfg.enableLongs && 'L', cfg.enableShorts && 'S'].filter(Boolean).join('+');
    if (lados) partes.push(lados);

    const filtros = [];
    const emas = (cfg.pullbackEMAs ?? []).map(e => `${e.period}${e.tf}`).join('/');
    if (cfg.useEmaAlignment) filtros.push(`align EMA ${emas}`);
    if (cfg.usePullbackFilter) filtros.push(`pullback ${cfg.pullbackPerc?.toFixed(2)}%`);
    if (cfg.useEmaTrendFilter) filtros.push(`tendencia EMA${cfg.emaTrendLen} ${cfg.emaTrendTf}`);
    if (cfg.useRsiFilter) filtros.push(`RSI${cfg.rsiPeriod} ${cfg.rsiTf} ${cfg.rsiLongMin}/${cfg.rsiShortMax}`);
    if (cfg.useMacdFilter) filtros.push(`MACD ${cfg.macdFast}/${cfg.macdSlow}/${cfg.macdSignal} ${cfg.macdTf}`);
    if (cfg.useADXFilter) filtros.push(`ADX>${cfg.adxThreshold} ${cfg.adxTf}`);
    if (cfg.useEmaAngFilter) filtros.push(`ang EMA${cfg.emaAngLen} ${cfg.emaAngTf}`);
    if (cfg.useVwapFilter) filtros.push(`VWAP ${cfg.vwapTf}/${cfg.vwapSession}`);
    if (cfg.useDeltaFilter) filtros.push(`delta ${cfg.deltaVelas}v`);
    if (cfg.useCVDFilter) filtros.push(`CVD ${cfg.cvdLookback}v`);
    if (cfg.useWhaleFilter) filtros.push(`ballenas ${cfg.whaleMinBTC?.toFixed(1)}BTC/${cfg.whaleWindow}min`);
    if (cfg.useOIFilter) filtros.push(`OI ${cfg.oiThreshold?.toFixed(2)}%/${cfg.oiLookbackMin}min`);

    return `${partes.join(' · ')}\n      filtros: ${filtros.length ? filtros.join(' · ') : '(ninguno)'}`;
}

(async () => {
    const args = parsearArgs(process.argv.slice(2));
    if (args.ayuda || args.help) { console.log(AYUDA); process.exit(0); }

    const cfg = {
        dias:     parseInt(args.dias) || 150,
        trials:   parseInt(args.trials) || 300,
        diasIS:   parseInt(args['dias-is']) || 45,
        diasOOS:  parseInt(args['dias-oos']) || 15,
        modo:     args.modo === 'anclado' ? 'anclado' : 'rolling',
        bloques:  args.bloques || 'sin-aux',
        objetivo: args.objetivo || 'sharpe_dd',
        topN:     parseInt(args['top-n']) || 12,
        minTradesPorMes: parseInt(args['min-trades-mes']) || 20,
        ddObjetivo: parseFloat(args['dd-objetivo']) || 20,
        palanca:  parseFloat(args.palanca) || 5,
        sizing:   ['porc_capital_actual','porc_capital_inicial','monto_fijo'].includes(args.sizing) ? args.sizing : 'porc_capital_actual',
        posicion: parseFloat(args.posicion) || 100,
        capital:  parseFloat(args.capital) || 1000,
        exchange: args.exchange === 'bingx' ? 'bingx' : 'binance',
        semilla:  parseInt(args.semilla) || 12345,
        workers:  args.workers ? parseInt(args.workers) : undefined,
        pruning:  !args['sin-pruning'],
        salida:   args.salida || path.join(__dirname, '..', 'resultados'),
    };

    if (!OBJETIVOS[cfg.objetivo]) {
        console.error(`Objetivo desconocido: ${cfg.objetivo}. Disponibles: ${Object.keys(OBJETIVOS).join(', ')}`);
        process.exit(1);
    }
    const bloques = PRESETS_BLOQUES[cfg.bloques] ?? cfg.bloques.split(',').map(s => s.trim());
    const espacio = construirEspacio(bloques);

    const fijos = { ...fijosPorDefecto(), palancaValor: cfg.palanca, posicionTipo: cfg.sizing, posicionValor: cfg.posicion, initialCapital: cfg.capital };
    const cfgObjetivo = { objetivo: cfg.objetivo, minTradesPorMes: cfg.minTradesPorMes, ddObjetivo: cfg.ddObjetivo, lambdaDD: 2 };

    const pool = new Pool({
        connectionString: process.env.DATABASE_URL,
        ssl: process.env.DATABASE_URL?.includes('sslmode=disable') ? false : { rejectUnauthorized: false },
    });

    console.log('\n═══ OPTIMIZADOR DE ESTRATEGIAS ═══\n');
    console.log(`  Período      ${cfg.dias} días de ${cfg.exchange}`);
    console.log(`  Walk-forward ${cfg.diasIS}d optimizar → ${cfg.diasOOS}d validar, modo ${cfg.modo}`);
    console.log(`  Presupuesto  ${cfg.trials} trials por ventana`);
    console.log(`  Objetivo     ${cfg.objetivo} (mín ${cfg.minTradesPorMes} trades/mes, DD objetivo ${cfg.ddObjetivo}%)`);
    console.log(`  Fijo         palanca ${cfg.palanca}x · ${cfg.posicion}% (${cfg.sizing}) · capital ${cfg.capital}`);
    if (cfg.sizing === 'porc_capital_actual' && cfg.posicion >= 100) {
        console.log('               ojo: con 100% del capital actual nunca queda margen libre para una');
        console.log('               segunda entrada, así que allowMultipleEntries no puede hacer nada.');
    }
    console.log(`  Bloques      ${bloques.join(', ')} → ${espacio.length} parámetros buscados`);

    console.log('\nCargando histórico…');
    const t0 = Date.now();
    const historico = await cargarHistorico(pool, { exchange: cfg.exchange, dias: cfg.dias });
    await pool.end();
    console.log(`  ${historico.bars1m.length} velas 1m, ${historico.ballenas.length} ballenas, ${historico.oi.length} muestras de OI  (${((Date.now() - t0) / 1000).toFixed(1)}s)`);
    console.log(`  Cada ventana usa ${DIAS_CALENTAMIENTO} días previos solo para calentar indicadores.`);

    const avisos = avisosDeCobertura(historico, bloques);
    if (avisos.length) {
        console.log('\n  ⚠ AVISOS DE COBERTURA DE DATOS');
        avisos.forEach(a => console.log(`    · ${a}`));
    }

    const ventanas = generarVentanas({
        desde: historico.desdeVentanas, hasta: historico.hasta,
        diasIS: cfg.diasIS, diasOOS: cfg.diasOOS, modo: cfg.modo,
    });
    console.log(`\n  ${ventanas.length} ventanas walk-forward:`);
    ventanas.forEach(v => console.log(
        `    fold ${v.indice}: optimizar ${dia(v.is.desde)} → ${dia(v.is.hasta)}  |  validar ${dia(v.oos.desde)} → ${dia(v.oos.hasta)}`
    ));

    const marca = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
    const rutaCSV = path.join(cfg.salida, `trials-${marca}.csv`);
    const rutaJSON = path.join(cfg.salida, `reporte-${marca}.json`);
    const registro = new RegistroCSV(rutaCSV, espacio);

    const evaluadores = new PoolEvaluadores({ fijos, cfgObjetivo, nWorkers: cfg.workers });
    console.log(`\n  ${evaluadores.n} hilos de evaluación. Estimado: ~${((ventanas.length * cfg.trials * 0.3) / evaluadores.n / 60).toFixed(0)} min.\n`);

    const tInicio = Date.now();
    let ultimoLog = 0;
    const resultado = await correrWalkForward({
        pool: evaluadores, espacio, historico, ventanas,
        trials: cfg.trials, topN: cfg.topN, semilla: cfg.semilla, usarPruning: cfg.pruning,
        alRegistrar: (t) => {
            registro.escribir(t);
            if (t.fase === 'is' && Date.now() - ultimoLog > 3000) {
                ultimoLog = Date.now();
                process.stdout.write(`\r    fold ${t.fold} · trial ${t.nroTrial}/${cfg.trials} · score ${n2(t.score)}${t.podado ? ' (podado)' : ''}            `);
            }
        },
        alProgreso: (p) => {
            if (p.fase === 'optimizando') process.stdout.write(`\n  [fold ${p.fold + 1}/${p.total}] optimizando ${dia(p.ventana.is.desde)} → ${dia(p.ventana.is.hasta)}\n`);
            if (p.fase === 'validando')   process.stdout.write(`\r    → validando top-${cfg.topN} en ${dia(p.ventana.oos.desde)} → ${dia(p.ventana.oos.hasta)}          \n`);
            if (p.fase === 'revalidando') process.stdout.write(`\n  Revalidando ${p.total} candidatos en las ventanas posteriores…\n`);
            if (p.fase === 'auditando')   process.stdout.write(`  Auditando ${p.total} finalistas sobre el período completo…\n`);
        },
    });
    await evaluadores.cerrar();
    await registro.cerrar();
    const minutos = ((Date.now() - tInicio) / 60000).toFixed(1);

    // ── Reporte ──
    console.log('\n\n═══ RESULTADO POR VENTANA ═══\n');
    console.log('  fold  validación              score IS   score OOS   eficiencia   ret.OOS    DD.OOS   trades');
    for (const f of resultado.folds) {
        const mejor = f.top[0];
        console.log(
            `  ${String(f.indice).padEnd(5)} ${dia(f.periodoOOS.desde)}→${dia(f.periodoOOS.hasta)}` +
            `  ${n2(f.mejorIS?.score).padStart(8)}` +
            `  ${n2(mejor?.scoreOOS).padStart(10)}` +
            `  ${(f.eficiencia !== null ? `${(f.eficiencia * 100).toFixed(0)}%` : '—').padStart(11)}` +
            `  ${pct(mejor?.metricasOOS?.retornoPerc).padStart(8)}` +
            `  ${pct(mejor?.metricasOOS?.maxDDPerc).padStart(8)}` +
            `  ${String(mejor?.metricasOOS?.trades ?? '—').padStart(6)}`
        );
    }
    const ef = resultado.resumen.eficienciaMediana;
    console.log(`\n  Eficiencia walk-forward mediana: ${ef !== null ? `${(ef * 100).toFixed(0)}%` : '—'}`);
    console.log('  (cuánto del desempeño in-sample sobrevive fuera de muestra; por debajo de 0 el');
    console.log('   proceso de optimización no está generalizando en absoluto)');

    console.log('\n═══ RANKING FINAL — combinaciones validadas fuera de muestra ═══\n');
    const aceptadas = resultado.ranking.filter(r => r.aceptado);
    if (!aceptadas.length) {
        console.log('  NINGUNA combinación pasó la validación out-of-sample.');
        console.log('  No es un error del script: es el resultado. En este espacio de búsqueda y este');
        console.log('  período no hay una combinación cuya ventaja se sostenga fuera de la muestra con');
        console.log('  la que fue ajustada. Operar la "mejor" de todos modos sería operar el ruido.');
        // Solo se muestran las que efectivamente se midieron fuera de muestra: las del último
        // fold no tienen ventanas posteriores, así que no estuvieron "cerca" de nada.
        const medidas = resultado.ranking.filter(r => r.nVentanasOOS > 0);
        if (medidas.length) {
            console.log('\n  Las 5 que más cerca estuvieron:\n');
            medidas.slice(0, 5).forEach((r, i) => {
                console.log(`  ${i + 1}. score OOS mediano ${n2(r.medianaOOS)} · ${r.nVentanasOOS} ventana(s) · ret. mediano ${pct(r.retornoOOSMediano)} · DD máx ${pct(r.ddOOSMaximo)}`);
                console.log(`      ${describir(aParamsBacktest(r.cfg, fijos))}`);
                console.log(`      rechazada por: ${r.motivos.join('; ')}\n`);
            });
        }
    } else {
        aceptadas.slice(0, 10).forEach((r, i) => {
            console.log(`  ${i + 1}. score OOS mediano ${n2(r.medianaOOS)}  (in-sample ${n2(r.scoreIS)}, degradación ${(r.degradacion * 100).toFixed(0)}%)`);
            console.log(`      ${r.nVentanasOOS} ventanas OOS · ${(r.pctPositivas * 100).toFixed(0)}% positivas · ret. mediano ${pct(r.retornoOOSMediano)} · DD máx ${pct(r.ddOOSMaximo)} · ${r.tradesOOSMediano} trades · WR ${n2(r.winRateOOSMediano)}%`);
            if (r.periodoCompleto) {
                const c = r.periodoCompleto;
                console.log(`      período completo: ${pct(c.retornoPerc)} · DD ${pct(c.maxDDPerc)} · PF ${n2(c.profitFactor)} · ${c.trades} trades`);
            }
            console.log(`      ${describir(aParamsBacktest(r.cfg, fijos))}\n`);
        });
    }

    guardarReporte(rutaJSON, {
        configuracion: cfg, bloques, fijos, cfgObjetivo,
        periodo: { desde: historico.desdeVentanas, hasta: historico.hasta },
        avisos, ventanas, resumen: resultado.resumen,
        folds: resultado.folds,
        ranking: resultado.ranking.map(r => ({
            ...r,
            // Los params tal cual los consume el backtest: se pegan directo en /estrategias
            // (POST /api/estrategias) o se comparan contra una estrategia ya guardada.
            paramsBacktest: aParamsBacktest(r.cfg, fijos),
        })),
    });

    console.log('═══ ARCHIVOS ═══\n');
    console.log(`  Trials  ${rutaCSV}  (${registro.filas} filas)`);
    console.log(`  Reporte ${rutaJSON}`);
    console.log(`\n  ${resultado.resumen.candidatosEvaluados} candidatos revalidados · ${resultado.resumen.aceptados} aceptados · ${minutos} min\n`);
    console.log('  El reporte trae `paramsBacktest` por combinación: es el body exacto de');
    console.log('  POST /api/estrategias, así que se puede guardar y verla en /estrategias.\n');
})().catch(err => {
    console.error('\nError:', err.message);
    console.error(err.stack);
    process.exit(1);
});
