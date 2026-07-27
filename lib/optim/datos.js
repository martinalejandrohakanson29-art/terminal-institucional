// ============================================================
// CARGA Y VENTANEO DE DATOS HISTÓRICOS
// ============================================================
// Trae UNA sola vez todo el histórico del período del estudio y lo rebana en ventanas. Volver
// a consultar la BD por cada trial sería absurdo: la consulta de 150 días tarda ~4 s y un
// backtest ~300 ms, así que el estudio entero pasaría el 90% del tiempo esperando a Postgres.

const { agregarVelas1m } = require('../backtest-core');

// Velas previas que se le anteponen a cada ventana para que los indicadores lleguen
// convergidos a su primera vela operable. La más lenta del espacio de búsqueda es una EMA de
// 200 sobre velas de 15m (= 3000 minutos de historia solo para arrancar); 10 días le dan casi
// 5 veces eso, que es margen de sobra sin que el prefijo pese en la corrida.
const DIAS_CALENTAMIENTO = 10;

async function cargarHistorico(pool, { exchange = 'binance', dias, hasta = Date.now() } = {}) {
    const desdeVentanas = hasta - dias * 86400000;
    // El prefijo de calentamiento se descarga junto con el resto: la primera ventana también
    // lo necesita, y pedirlo aparte sería una segunda consulta por el mismo rango.
    const desdeCarga = desdeVentanas - DIAS_CALENTAMIENTO * 86400000;

    const [velas, ballenas, oi, cobertura] = await Promise.all([
        pool.query(
            `SELECT open_time, open, high, low, close, volume, close_time, taker_buy_base
             FROM klines_1m WHERE open_time >= $1::bigint AND open_time <= $2::bigint AND exchange = $3
             ORDER BY open_time ASC`,
            [desdeCarga, hasta, exchange]
        ),
        // Se traen TODAS las ballenas desde el mínimo del rango de búsqueda de whaleMinBTC:
        // el umbral es un parámetro que se optimiza, así que el filtrado por cantidad se hace
        // por trial (en memoria) y no acá.
        pool.query(
            `SELECT EXTRACT(EPOCH FROM fecha) AS ts_sec, cantidad, es_venta
             FROM ballenas WHERE fecha >= $1 AND exchange = $2 ORDER BY fecha ASC`,
            [new Date(desdeCarga).toISOString(), exchange]
        ),
        // Margen extra hacia atrás: el filtro de OI compara contra el valor de hasta 240
        // minutos antes, y las primeras velas de cada ventana también necesitan esa muestra.
        pool.query(
            `SELECT tiempo, valor FROM open_interest
             WHERE tiempo >= $1 AND exchange = $2 ORDER BY tiempo ASC`,
            [Math.floor(desdeCarga / 1000) - 300 * 60, exchange]
        ),
        pool.query(
            `SELECT (SELECT MIN(fecha) FROM ballenas WHERE exchange = $1) AS ballenas_desde,
                    (SELECT MIN(tiempo) FROM open_interest WHERE exchange = $1) AS oi_desde,
                    (SELECT MIN(tiempo) FROM long_short_ratio) AS ls_desde`,
            [exchange]
        ),
    ]);

    const bars1m = velas.rows.map(f => [
        Number(f.open_time), Number(f.open), Number(f.high), Number(f.low),
        Number(f.close), Number(f.volume), Number(f.close_time), 0, 0,
        f.taker_buy_base == null ? null : Number(f.taker_buy_base),
    ]);
    if (bars1m.length === 0) {
        throw new Error(`No hay velas de ${exchange} en la BD para ese período. ¿Corrió el backfill inicial (klines_1m)?`);
    }

    const c = cobertura.rows[0];
    return {
        exchange,
        bars1m,
        ballenas: ballenas.rows.map(w => ({ ts_sec: Number(w.ts_sec), cantidad: Number(w.cantidad), es_venta: w.es_venta })),
        oi: oi.rows.map(o => ({ tiempo: Number(o.tiempo), valor: Number(o.valor) })),
        desdeVentanas,
        hasta,
        coberturaAux: {
            ballenasDesde: c.ballenas_desde ? new Date(c.ballenas_desde).getTime() : null,
            oiDesde:       c.oi_desde ? Number(c.oi_desde) * 1000 : null,
            lsDesde:       c.ls_desde ? Number(c.ls_desde) * 1000 : null,
        },
    };
}

// Avisos de calidad de datos. Un estudio que optimiza el filtro de ballenas sobre un período
// donde no hay ballenas guardadas no falla: simplemente no abre trades, y el optimizador
// "aprende" que ese filtro es malísimo. Eso es una conclusión inválida sacada de un hueco de
// datos, así que conviene verlo antes de leer el ranking.
function avisosDeCobertura(historico, bloques) {
    const avisos = [];
    const { coberturaAux: cov, desdeVentanas } = historico;
    const dias = ms => ((Date.now() - ms) / 86400000).toFixed(0);

    if (bloques.includes('auxiliares')) {
        if (!cov.ballenasDesde) {
            avisos.push('No hay NINGÚN dato de ballenas guardado: el filtro de Ballenas nunca dejará entrar un trade.');
        } else if (cov.ballenasDesde > desdeVentanas) {
            avisos.push(`Ballenas solo desde ${new Date(cov.ballenasDesde).toISOString().slice(0, 10)} (~${dias(cov.ballenasDesde)} días). Las ventanas anteriores a esa fecha no generan trades con el filtro activo.`);
        }
        if (!cov.oiDesde) {
            avisos.push('No hay NINGÚN dato de open interest guardado: el filtro de OI nunca dejará entrar un trade.');
        } else if (cov.oiDesde > desdeVentanas) {
            avisos.push(`Open Interest solo desde ${new Date(cov.oiDesde).toISOString().slice(0, 10)} (~${dias(cov.oiDesde)} días). Las ventanas anteriores no generan trades con el filtro activo.`);
        }
    }
    return avisos;
}

// Rebana una sub-ventana temporal lista para backtestear, con su prefijo de calentamiento.
// Devuelve las tres temporalidades ya derivadas y el índice donde termina el prefijo, que es
// lo que se le pasa a runBacktest como warmupBars para que el primer trade posible caiga
// exactamente en `desde` y las métricas correspondan al período pedido y a ningún otro.
function crearVentana(historico, desde, hasta) {
    const inicioPrefijo = desde - DIAS_CALENTAMIENTO * 86400000;
    const todas = historico.bars1m;

    // Búsqueda binaria del primer índice con open_time >= objetivo.
    const indiceDesde = (objetivo) => {
        let lo = 0, hi = todas.length;
        while (lo < hi) {
            const mid = (lo + hi) >> 1;
            if (todas[mid][0] < objetivo) lo = mid + 1; else hi = mid;
        }
        return lo;
    };

    const iPrefijo = indiceDesde(inicioPrefijo);
    const iInicio  = indiceDesde(desde);
    const iFin     = indiceDesde(hasta);
    const bars1m = todas.slice(iPrefijo, iFin);
    if (bars1m.length === 0) throw new Error(`Ventana vacía: ${new Date(desde).toISOString()} → ${new Date(hasta).toISOString()}`);

    return {
        desde, hasta,
        dias: (hasta - desde) / 86400000,
        bars1m,
        bars5m:  agregarVelas1m(bars1m, 300000),
        bars15m: agregarVelas1m(bars1m, 900000),
        // Velas de prefijo: runBacktest no opera en ellas, solo calienta indicadores.
        warmupBars: iInicio - iPrefijo,
        ballenas: historico.ballenas.filter(w => w.ts_sec * 1000 >= inicioPrefijo && w.ts_sec * 1000 <= hasta),
        oi: historico.oi.filter(o => o.tiempo * 1000 >= inicioPrefijo - 300 * 60000 && o.tiempo * 1000 <= hasta),
    };
}

// Divide el período en pares (in-sample, out-of-sample) consecutivos.
//   'rolling'  — la ventana de optimización se desliza: siempre mide los últimos N días.
//                Se adapta a cambios de régimen del mercado, pero usa menos datos por ajuste.
//   'anclado'  — el inicio queda fijo y el in-sample crece: usa toda la historia disponible,
//                pero diluye lo reciente si el mercado cambió de comportamiento.
// En ambos casos el out-of-sample es SIEMPRE el tramo inmediatamente siguiente al in-sample,
// que el optimizador no vio, y avanza sin solaparse con otros out-of-sample.
function generarVentanas({ desde, hasta, diasIS, diasOOS, modo = 'rolling' }) {
    const ventanas = [];
    const msIS = diasIS * 86400000, msOOS = diasOOS * 86400000;
    let inicioIS = desde;

    while (inicioIS + msIS + msOOS <= hasta + 1) {
        const finIS = inicioIS + msIS;
        ventanas.push({
            indice: ventanas.length,
            is:  { desde: modo === 'anclado' ? desde : inicioIS, hasta: finIS },
            oos: { desde: finIS, hasta: finIS + msOOS },
        });
        inicioIS += msOOS; // el paso es el largo del out-of-sample: así no se solapan entre sí
    }

    if (!ventanas.length) {
        throw new Error(
            `El período (${((hasta - desde) / 86400000).toFixed(0)} días) no alcanza para una ventana de ` +
            `${diasIS} días de optimización + ${diasOOS} de validación. Bajá --dias-is/--dias-oos o subí --dias.`
        );
    }
    return ventanas;
}

module.exports = { cargarHistorico, crearVentana, generarVentanas, avisosDeCobertura, DIAS_CALENTAMIENTO };
