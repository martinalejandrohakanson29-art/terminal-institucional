// ============================================================
// EVALUADOR — de una combinación de parámetros a un score
// ============================================================
// Es la "función pura" del pipeline: mismos parámetros + mismos datos → mismo resultado,
// siempre. Esa determinación no es un detalle: el optimizador construye su modelo asumiendo
// que un score refleja la combinación y no el momento en que se evaluó.

const { runBacktest } = require('../backtest-core');
const { aParamsBacktest } = require('./espacio');
const { calcularMetricas, calcularScore } = require('./metricas');

// Prepara una ventana para ser evaluada muchas veces: le cuelga la cache de indicadores, que
// vive lo que vive la ventana. Los indicadores dependen de los parámetros, pero muchos se
// repiten entre combinaciones (la EMA 200 de 1m es la misma para las cientos de configs que
// la usan), así que se calculan una vez por ventana en lugar de una vez por trial.
function prepararVentana(ventana) {
    return { ...ventana, cache: new Map() };
}

/**
 * Evalúa una combinación sobre una ventana.
 * @param fraccion  porción de la ventana a recorrer (1 = entera). Con < 1 se corre solo un
 *                  prefijo para decidir si vale la pena terminar la corrida — ver pruning.
 */
function evaluar(ventana, cfg, fijos, cfgObjetivo, fraccion = 1) {
    const p = aParamsBacktest(cfg, fijos);
    p.warmupBars = ventana.warmupBars;
    p._cache = ventana.cache;
    // El Sharpe se calcula sobre la serie completa de retornos por trade, no sobre los
    // últimos 300 que le alcanzan a la UI para graficar.
    p.maxTradesDevueltos = 1e9;

    const operables = ventana.bars1m.length - ventana.warmupBars;
    if (fraccion < 1) p.hastaBarIdx = ventana.warmupBars + Math.floor(operables * fraccion);

    const diasEfectivos = ventana.dias * (fraccion < 1 ? fraccion : 1);
    const resultado = runBacktest(
        ventana.bars1m, ventana.bars5m, ventana.bars15m,
        ventana.ballenas, p, ventana.oi, []
    );
    const metricas = calcularMetricas(resultado, {
        diasVentana: diasEfectivos,
        inicioVentana: ventana.desde,
        capitalInicial: p.initialCapital,
    });
    return { metricas, score: calcularScore(metricas, cfgObjetivo, diasEfectivos) };
}

module.exports = { prepararVentana, evaluar };
