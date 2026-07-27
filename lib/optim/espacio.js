// ============================================================
// ESPACIO DE BÚSQUEDA — qué parámetros se optimizan y en qué rangos
// ============================================================
// Este archivo es la ÚNICA parte que hay que tocar para cambiar qué se busca. El motor de
// optimización (tpe.js) y el walk-forward (walkforward.js) no saben nada de trading: reciben
// una lista de descriptores y una función de evaluación, y son agnósticos de la estrategia.
//
// Cada parámetro se describe con:
//   nombre    — clave en la config (no siempre coincide con la del backtest: ver aParamsBacktest)
//   tipo      — 'int' | 'float' | 'cat' | 'bool'
//   min/max   — rango, para int/float
//   escala    — 'lineal' (default) o 'log', para rangos que abarcan varios órdenes de magnitud
//               (p.ej. cooldown de 5 a 240 minutos: en escala lineal el muestreo casi nunca
//               probaría valores chicos, que son los que más cambian el comportamiento)
//   opciones  — lista de valores, para 'cat'
//   si        — (cfg) => bool. El parámetro solo existe si su filtro está encendido. Los
//               parámetros condicionales apagados no se muestrean ni entran al modelo del
//               optimizador, así la dimensionalidad efectiva baja sola.

const { normalizarParams } = require('../backtest-core');

// Combinaciones de EMAs de pullback/alineamiento. Buscar la lista completa (cuántas EMAs,
// qué período y qué temporalidad cada una) explota combinatoriamente y la mayoría de las
// combinaciones son basura, así que se busca sobre un catálogo de configuraciones sensatas.
// Agregar una fila acá la vuelve buscable sin tocar nada más.
const PRESETS_EMA_PULLBACK = {
    'ema 50/100/200/500 1m': [{ period: 50, tf: '1m' }, { period: 100, tf: '1m' }, { period: 200, tf: '1m' }, { period: 500, tf: '1m' }],
    'ema 20/50/200 1m':      [{ period: 20, tf: '1m' }, { period: 50, tf: '1m' }, { period: 200, tf: '1m' }],
    'ema 9/21/50 1m':        [{ period: 9,  tf: '1m' }, { period: 21, tf: '1m' }, { period: 50, tf: '1m' }],
    'ema 50/200 1m':         [{ period: 50, tf: '1m' }, { period: 200, tf: '1m' }],
    'ema 200/500 1m':        [{ period: 200, tf: '1m' }, { period: 500, tf: '1m' }],
    'ema 21/50 5m':          [{ period: 21, tf: '5m' }, { period: 50, tf: '5m' }],
    'ema 50/200 5m':         [{ period: 50, tf: '5m' }, { period: 200, tf: '5m' }],
    'ema 21/50 15m':         [{ period: 21, tf: '15m' }, { period: 50, tf: '15m' }],
    'ema 100 1m + 50 5m':    [{ period: 100, tf: '1m' }, { period: 50, tf: '5m' }],
};

// EMAs usadas como stop cuando stopType = 'Ruptura EMA' (se sale al cerrar del otro lado).
const PRESETS_EMA_STOP = {
    'stop ema 200 1m':        [{ period: 200, tf: '1m' }],
    'stop ema 500 1m':        [{ period: 500, tf: '1m' }],
    'stop ema 200+500 1m':    [{ period: 200, tf: '1m' }, { period: 500, tf: '1m' }],
    'stop ema 50 1m':         [{ period: 50, tf: '1m' }],
    'stop ema 100 1m':        [{ period: 100, tf: '1m' }],
    'stop ema 50 5m':         [{ period: 50, tf: '5m' }],
    'stop ema 200 5m':        [{ period: 200, tf: '5m' }],
};

const TF = ['1m', '5m', '15m'];
const TF_CON_1H = ['1m', '5m', '15m', '1h'];

// ── Los parámetros, agrupados por bloque ────────────────────────────────────────────────
// El agrupamiento sirve para dos cosas: los presets de --bloques, y que el reporte final
// pueda decir "qué filtros usa la estrategia ganadora" sin adivinar.

const BLOQUES = {

    // Dirección y gestión de la posición. Siempre activo: es el esqueleto de cualquier estrategia.
    salidas: [
        { nombre: 'enableLongs',  tipo: 'bool' },
        { nombre: 'enableShorts', tipo: 'bool' },
        { nombre: 'tpPerc',   tipo: 'float', min: 0.15, max: 2.5, escala: 'log' },
        { nombre: 'stopType', tipo: 'cat', opciones: ['Porcentaje', 'Ruptura EMA'] },
        { nombre: 'slPerc',   tipo: 'float', min: 0.2, max: 3.0, escala: 'log', si: c => c.stopType === 'Porcentaje' },
        { nombre: 'stopEmaPreset', tipo: 'cat', opciones: Object.keys(PRESETS_EMA_STOP), si: c => c.stopType === 'Ruptura EMA' },
        { nombre: 'useBreakeven',     tipo: 'bool' },
        { nombre: 'breakevenTrigger', tipo: 'float', min: 0.15, max: 1.2, escala: 'log', si: c => c.useBreakeven },
        { nombre: 'breakevenOffset',  tipo: 'float', min: 0.02, max: 0.4, escala: 'log', si: c => c.useBreakeven },
        { nombre: 'useMaxTradeTime', tipo: 'bool' },
        { nombre: 'maxTradeMinutes', tipo: 'int', min: 5, max: 480, escala: 'log', si: c => c.useMaxTradeTime },
    ],

    // Cuándo se permite operar y cómo se acumulan posiciones.
    sesion: [
        { nombre: 'startHour', tipo: 'int', min: 0,  max: 14 },
        { nombre: 'endHour',   tipo: 'int', min: 4,  max: 24 },
        { nombre: 'operaFinDeSemana', tipo: 'bool' },
        { nombre: 'useCooldown',     tipo: 'bool' },
        { nombre: 'cooldownMinutes', tipo: 'int', min: 3, max: 240, escala: 'log', si: c => c.useCooldown },
        { nombre: 'allowMultipleEntries',  tipo: 'bool' },
        { nombre: 'blockMultipleIfLosing', tipo: 'bool', si: c => c.allowMultipleEntries },
    ],

    // Estructura de precio: alineamiento de EMAs y pullback.
    tendencia: [
        { nombre: 'useEmaAlignment',   tipo: 'bool' },
        { nombre: 'emaPullbackPreset', tipo: 'cat', opciones: Object.keys(PRESETS_EMA_PULLBACK) },
        { nombre: 'usePullbackFilter', tipo: 'bool' },
        { nombre: 'pullbackPerc',      tipo: 'float', min: 0.04, max: 0.9, escala: 'log', si: c => c.usePullbackFilter },
        { nombre: 'useEmaTrendFilter', tipo: 'bool' },
        { nombre: 'emaTrendTf',        tipo: 'cat', opciones: TF_CON_1H, si: c => c.useEmaTrendFilter },
        { nombre: 'emaTrendLen',       tipo: 'cat', opciones: [9, 21, 50, 100, 200], si: c => c.useEmaTrendFilter },
    ],

    // Osciladores clásicos.
    osciladores: [
        { nombre: 'useRsiFilter', tipo: 'bool' },
        { nombre: 'rsiTf',        tipo: 'cat', opciones: TF, si: c => c.useRsiFilter },
        { nombre: 'rsiPeriod',    tipo: 'int', min: 5, max: 30, si: c => c.useRsiFilter },
        { nombre: 'rsiLongMin',   tipo: 'int', min: 45, max: 78, si: c => c.useRsiFilter },
        { nombre: 'rsiShortMax',  tipo: 'int', min: 22, max: 55, si: c => c.useRsiFilter },
        { nombre: 'useMacdFilter', tipo: 'bool' },
        { nombre: 'macdTf',        tipo: 'cat', opciones: TF, si: c => c.useMacdFilter },
        { nombre: 'macdFast',      tipo: 'int', min: 4,  max: 20, si: c => c.useMacdFilter },
        { nombre: 'macdSlow',      tipo: 'int', min: 18, max: 45, si: c => c.useMacdFilter },
        { nombre: 'macdSignal',    tipo: 'int', min: 4,  max: 16, si: c => c.useMacdFilter },
        { nombre: 'useADXFilter',  tipo: 'bool' },
        { nombre: 'adxTf',         tipo: 'cat', opciones: TF_CON_1H, si: c => c.useADXFilter },
        { nombre: 'adxThreshold',  tipo: 'int', min: 10, max: 45, si: c => c.useADXFilter },
    ],

    // Angulación de EMA y VWAP.
    estructura: [
        { nombre: 'useEmaAngFilter',   tipo: 'bool' },
        { nombre: 'emaAngTf',          tipo: 'cat', opciones: TF, si: c => c.useEmaAngFilter },
        { nombre: 'emaAngLen',         tipo: 'cat', opciones: [50, 100, 200], si: c => c.useEmaAngFilter },
        { nombre: 'emaAngSlopeBars',   tipo: 'int', min: 3, max: 30, si: c => c.useEmaAngFilter },
        { nombre: 'emaAngMode',        tipo: 'cat', opciones: ['min', 'strong'], si: c => c.useEmaAngFilter },
        { nombre: 'emaAngMinSlope',    tipo: 'float', min: 0.03, max: 1.2, escala: 'log', si: c => c.useEmaAngFilter && c.emaAngMode === 'min' },
        { nombre: 'emaAngStrongSlope', tipo: 'float', min: 0.3,  max: 2.5, escala: 'log', si: c => c.useEmaAngFilter && c.emaAngMode === 'strong' },
        { nombre: 'useVwapFilter',    tipo: 'bool' },
        { nombre: 'vwapTf',           tipo: 'cat', opciones: TF, si: c => c.useVwapFilter },
        { nombre: 'vwapSession',      tipo: 'cat', opciones: ['daily', 'weekly', 'monthly'], si: c => c.useVwapFilter },
        { nombre: 'vwapUseDirection', tipo: 'bool', si: c => c.useVwapFilter },
        { nombre: 'vwapUsePullback',  tipo: 'bool', si: c => c.useVwapFilter },
        { nombre: 'vwapPullbackPerc', tipo: 'float', min: 0.05, max: 1.2, escala: 'log', si: c => c.useVwapFilter && c.vwapUsePullback },
    ],

    // Flujo de órdenes: delta de taker, CVD.
    flujo: [
        { nombre: 'useDeltaFilter', tipo: 'bool' },
        { nombre: 'deltaVelas',     tipo: 'int', min: 1, max: 30, escala: 'log', si: c => c.useDeltaFilter },
        { nombre: 'useCVDFilter',   tipo: 'bool' },
        { nombre: 'cvdLookback',    tipo: 'int', min: 5, max: 240, escala: 'log', si: c => c.useCVDFilter },
    ],

    // Datos auxiliares: ballenas y open interest. OJO con la cobertura histórica — ver datos.js,
    // que avisa si la ventana pedida arranca antes de que existan estos datos.
    auxiliares: [
        { nombre: 'useWhaleFilter', tipo: 'bool' },
        { nombre: 'whaleWindow',    tipo: 'int', min: 5, max: 180, escala: 'log', si: c => c.useWhaleFilter },
        { nombre: 'whaleMinBTC',    tipo: 'float', min: 1, max: 25, escala: 'log', si: c => c.useWhaleFilter },
        { nombre: 'useOIFilter',    tipo: 'bool' },
        { nombre: 'oiLookbackMin',  tipo: 'int', min: 5, max: 240, escala: 'log', si: c => c.useOIFilter },
        { nombre: 'oiThreshold',    tipo: 'float', min: -1.0, max: 2.0, si: c => c.useOIFilter },
    ],
};

// Conjuntos de bloques listos para usar desde la CLI (--bloques).
const PRESETS_BLOQUES = {
    completo:   ['salidas', 'sesion', 'tendencia', 'osciladores', 'estructura', 'flujo', 'auxiliares'],
    'sin-aux':  ['salidas', 'sesion', 'tendencia', 'osciladores', 'estructura', 'flujo'],
    precio:     ['salidas', 'sesion', 'tendencia', 'osciladores', 'estructura'],
    base:       ['salidas', 'sesion', 'tendencia', 'osciladores'],
    salidas:    ['salidas', 'sesion'],
};

// Construye el espacio de búsqueda a partir de los bloques elegidos.
function construirEspacio(bloques) {
    const desconocidos = bloques.filter(b => !BLOQUES[b]);
    if (desconocidos.length) throw new Error(`Bloques desconocidos: ${desconocidos.join(', ')}. Disponibles: ${Object.keys(BLOQUES).join(', ')}`);
    return bloques.flatMap(b => BLOQUES[b].map(d => ({ ...d, bloque: b })));
}

// ── Reparación de combinaciones inválidas ───────────────────────────────────────────────
// El muestreo trata cada parámetro por separado, así que produce combinaciones que violan
// relaciones entre parámetros (ventana horaria invertida, MACD con rápida más lenta que la
// lenta...). Se REPARAN en vez de descartarse: descartar desperdicia el trial y sesga el
// muestreo hacia las zonas del espacio donde por casualidad no hay conflictos.
function reparar(cfg) {
    const c = { ...cfg };

    // Ventana horaria: el backtest exige startHour < endHour, si no nunca opera.
    if (c.endHour != null && c.startHour != null && c.endHour <= c.startHour) {
        c.endHour = Math.min(24, c.startHour + 1);
        if (c.endHour <= c.startHour) { c.startHour = c.endHour - 1; }
    }

    // MACD: la media rápida tiene que ser más corta que la lenta.
    if (c.macdFast != null && c.macdSlow != null && c.macdFast >= c.macdSlow) {
        const tmp = c.macdFast;
        c.macdFast = Math.max(2, Math.min(c.macdSlow - 1, tmp));
        if (c.macdFast >= c.macdSlow) c.macdSlow = c.macdFast + 1;
    }

    // Breakeven: un offset >= trigger deja el stop en o por encima del precio de disparo y la
    // posición cierra casi seguro en la vela siguiente (el server rechaza esa config con 400).
    if (c.useBreakeven && c.breakevenOffset != null && c.breakevenTrigger != null &&
        c.breakevenOffset >= c.breakevenTrigger) {
        c.breakevenOffset = +(c.breakevenTrigger * 0.6).toFixed(4);
    }

    // RSI: umbral de long por debajo del de short haría que ambos lados disparen a la vez en
    // la zona de solape, que no es lo que el filtro quiere decir.
    if (c.rsiLongMin != null && c.rsiShortMax != null && c.rsiShortMax >= c.rsiLongMin) {
        c.rsiShortMax = Math.max(1, c.rsiLongMin - 1);
    }

    // Sin ningún lado habilitado no hay estrategia: se prende el que quedó apagado por sorteo.
    if (c.enableLongs === false && c.enableShorts === false) {
        c[Math.random() < 0.5 ? 'enableLongs' : 'enableShorts'] = true;
    }

    return c;
}

// ── Traducción a los parámetros del backtest ────────────────────────────────────────────
// Convierte una config del espacio de búsqueda en el body que espera normalizarParams, y lo
// pasa por ahí. Esto último importa: garantiza que una combinación signifique EXACTAMENTE lo
// mismo por CLI que enviada desde /estrategias, defaults y acotaciones incluidos.
function aParamsBacktest(cfg, fijos) {
    const body = { ...cfg, ...fijos };

    // Los presets de EMAs son índices a un catálogo; el backtest quiere las listas.
    if (cfg.emaPullbackPreset) body.pullbackEMAs = PRESETS_EMA_PULLBACK[cfg.emaPullbackPreset];
    if (cfg.stopEmaPreset)     body.stopEMAs     = PRESETS_EMA_STOP[cfg.stopEmaPreset];
    delete body.emaPullbackPreset;
    delete body.stopEmaPreset;

    // Filtros de posicionamiento (long/short ratio de Binance): fuera del estudio salvo que se
    // pidan explícitamente. Su histórico es mucho más corto que el de las velas, así que
    // dejarlos entrar recortaría en silencio el período con el que se puede validar.
    body.useTopTraderFilter = fijos.useTopTraderFilter === true;
    body.useRetailFilter    = fijos.useRetailFilter === true;
    body.useTopSlopeFilter  = fijos.useTopSlopeFilter === true;

    return normalizarParams(body);
}

// Los parámetros que NO se buscan y quedan clavados en toda la corrida. Perfil de riesgo y
// costos: son datos de la cuenta real, no cosas que el optimizador deba "descubrir". Dejar
// que el optimizador suba la palanca es apalancar ruido, no encontrar señal.
function fijosPorDefecto() {
    return {
        posicionTipo:  'porc_capital_actual',
        posicionValor: 100,
        palancaActivo: true,
        palancaValor:  5,
        commission:    0.04,
        slippagePerc:  0.02,
        fundingPerc:   0.01,
        initialCapital: 1000,
    };
}

// Firma estable de una config, para detectar combinaciones repetidas. Solo incluye los
// parámetros ACTIVOS: dos configs que difieren en el período de un RSI apagado son la misma
// estrategia y no tiene sentido gastar un trial en volver a evaluarla.
function firma(cfg, espacio) {
    return espacio
        .filter(d => !d.si || d.si(cfg))
        .map(d => `${d.nombre}=${cfg[d.nombre]}`)
        .sort()
        .join('&');
}

module.exports = {
    BLOQUES, PRESETS_BLOQUES, PRESETS_EMA_PULLBACK, PRESETS_EMA_STOP,
    construirEspacio, reparar, aParamsBacktest, fijosPorDefecto, firma,
};
