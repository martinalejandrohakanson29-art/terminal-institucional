// ============================================================
// MÉTRICAS Y FUNCIÓN OBJETIVO
// ============================================================
// Optimizar por retorno total encuentra combinaciones que ganan mucho con drawdowns
// inaceptables, y sobre todo encuentra las que tuvieron suerte en tres trades. Acá se calcula
// un set de métricas de riesgo sobre el resultado del backtest y se combinan en un score
// único que es lo que el optimizador maximiza.

// Sharpe anualizado sobre retornos DIARIOS de la cuenta.
//
// La tentación es calcularlo sobre los retornos por trade y anualizar con sqrt(trades por
// año), pero eso da números disparatados (Sharpe de 70) y premia la alta frecuencia por sí
// misma: dos estrategias con la misma curva de equity puntúan distinto solo porque una parte
// sus entradas en más pedazos. Sobre retornos diarios el número es comparable entre
// estrategias, comparable con cualquier otro activo, y los días sin operar cuentan como
// retorno cero — que es exactamente lo que corresponde: el capital estuvo ahí, quieto.
function sharpeAnualizado(trades, capitalInicial, inicioVentana, diasVentana) {
    const nDias = Math.max(1, Math.round(diasVentana));
    if (!trades.length || !(capitalInicial > 0)) return 0;

    // PnL por día calendario de la ventana (los días sin trades quedan en 0).
    const pnlDiario = new Array(nDias).fill(0);
    for (const t of trades) {
        const d = Math.floor((t.exitTs - inicioVentana) / 86400000);
        if (d >= 0 && d < nDias) pnlDiario[d] += t.pnlAbs;
    }

    // Retorno diario sobre el capital con el que arrancó ese día: componer importa cuando el
    // sizing es un porcentaje del capital actual, que es el default del terminal.
    const retornos = [];
    let capital = capitalInicial;
    for (const pnl of pnlDiario) {
        if (capital <= 0) break; // cuenta liquidada: no hay más retornos que medir
        retornos.push(pnl / capital);
        capital += pnl;
    }
    if (retornos.length < 2) return 0;

    const media = retornos.reduce((s, r) => s + r, 0) / retornos.length;
    const varianza = retornos.reduce((s, r) => s + (r - media) ** 2, 0) / (retornos.length - 1);
    const desvio = Math.sqrt(varianza);
    if (!(desvio > 0)) return 0;
    return (media / desvio) * Math.sqrt(365);
}

// Calcula todas las métricas a partir del resultado crudo de runBacktest.
// `trades` tiene que venir COMPLETO (maxTradesDevueltos alto), no los últimos 300: el Sharpe
// sobre una muestra truncada de los trades finales no es el Sharpe de la estrategia.
function calcularMetricas(resultado, { diasVentana, inicioVentana, capitalInicial }) {
    const s = resultado.stats;
    const sharpe = sharpeAnualizado(resultado.trades, capitalInicial, inicioVentana, diasVentana);
    const dd = s.maxDrawdownPerc;

    return {
        trades:      s.totalTrades,
        winRate:     s.winRate,
        retornoPerc: s.netProfitPerc,
        maxDDPerc:   dd,
        profitFactor: Number.isFinite(s.profitFactor) ? s.profitFactor : (s.totalTrades > 0 ? 99 : 0),
        sharpe,
        // Calmar: cuánto retorno se obtiene por cada punto de drawdown sufrido. Con DD casi
        // nulo el cociente se dispara, así que se pisa el denominador con un piso de 0.5%.
        calmar: s.netProfitPerc / Math.max(dd, 0.5),
        retornoAnualizado: diasVentana > 0 ? s.netProfitPerc * (365 / diasVentana) : 0,
        longs:  s.longsCount,
        shorts: s.shortsCount,
        maxLossStreak: s.maxLossStreak,
    };
}

// ── Objetivos ───────────────────────────────────────────────────────────────────────────
// Todos devuelven "más alto es mejor". La penalización por pocos trades es GRADUAL a
// propósito: un muro plano ("si trades < 30 entonces -1000") no le da al optimizador ninguna
// pista de hacia dónde moverse, y se queda dando vueltas en la zona muerta. Con una rampa,
// una combinación de 25 trades puntúa mejor que una de 3, y el muestreo aprende que soltar
// filtros en esa dirección ayuda.
function penalizacionPocosTrades(trades, minTrades) {
    if (trades >= minTrades) return 0;
    return 50 * (1 - trades / minTrades) + 10;
}

// El mínimo de trades se expresa como TASA (trades por cada 30 días), no como número
// absoluto, y se escala al largo de cada ventana. Con un absoluto, una ventana de validación
// de 15 días quedaría descalificada casi por definición contra un umbral pensado para 45, y
// el score fuera de muestra terminaría midiendo cuánto opera la estrategia en vez de qué tan
// bien lo hace. El piso de 5 evita que ventanas muy cortas acepten muestras sin significado.
function minTradesDeVentana(minTradesPorMes, diasVentana) {
    return Math.max(5, Math.round(minTradesPorMes * (diasVentana / 30)));
}

const OBJETIVOS = {
    // Sharpe penalizado por drawdown. El drawdown solo penaliza cuando pasa el objetivo,
    // así que entre dos combinaciones igual de consistentes y ambas por debajo del umbral no
    // se prefiere artificialmente la de menos DD (que suele ser la que opera menos).
    sharpe_dd: (m, cfg) => {
        const exceso = Math.max(0, m.maxDDPerc - cfg.ddObjetivo) / cfg.ddObjetivo;
        return m.sharpe - cfg.lambdaDD * exceso;
    },
    // Retorno sobre el peor drawdown. Directo de interpretar, más ruidoso con pocos trades.
    calmar: (m) => m.calmar,
    // Robustez de la señal, ignorando por completo el riesgo de la curva.
    profit_factor: (m) => Math.min(m.profitFactor, 10),
};

// Score final de una combinación en una ventana. `null` si la combinación es inservible por
// definición (voló la cuenta), en cuyo caso se le da un score muy bajo pero finito para que
// el optimizador la modele como zona mala en vez de ignorarla.
function calcularScore(metricas, cfgObjetivo, diasVentana) {
    const cfg = { objetivo: 'sharpe_dd', minTradesPorMes: 20, ddObjetivo: 20, lambdaDD: 2, ...cfgObjetivo };
    const fn = OBJETIVOS[cfg.objetivo];
    if (!fn) throw new Error(`Objetivo desconocido: ${cfg.objetivo}. Disponibles: ${Object.keys(OBJETIVOS).join(', ')}`);

    // Cuenta liquidada o casi: no es una estrategia "mala", es una que no se puede operar.
    if (metricas.maxDDPerc >= 95 || metricas.retornoPerc <= -95) {
        return -100 + metricas.retornoPerc / 100;
    }
    const minTrades = minTradesDeVentana(cfg.minTradesPorMes, diasVentana);
    return fn(metricas, cfg) - penalizacionPocosTrades(metricas.trades, minTrades);
}

// Mediana — se usa para resumir el desempeño out-of-sample a través de varias ventanas.
// Mediana y no promedio: una sola ventana excepcional no debe tapar que las otras fueron malas.
function mediana(xs) {
    if (!xs.length) return 0;
    const o = [...xs].sort((a, b) => a - b);
    const m = Math.floor(o.length / 2);
    return o.length % 2 ? o[m] : (o[m - 1] + o[m]) / 2;
}

module.exports = { calcularMetricas, calcularScore, sharpeAnualizado, minTradesDeVentana, mediana, OBJETIVOS };
