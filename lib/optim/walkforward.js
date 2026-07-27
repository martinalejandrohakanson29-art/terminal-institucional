// ============================================================
// WALK-FORWARD — optimizar en un tramo, validar en el siguiente
// ============================================================
// Este es el paso que separa "encontré una combinación con buenos números" de "encontré una
// combinación que sirve". Optimizar sobre todo el histórico y quedarse con la mejor SIEMPRE
// da un resultado espectacular: con 45 parámetros libres y 300 intentos, alguna combinación
// va a calzar con la casualidad del período aunque no haya ninguna señal real detrás. La
// única forma de distinguirlas es medir en datos que el optimizador nunca vio.
//
// El proceso tiene dos fases:
//   A) Por cada fold: optimizar sobre el in-sample, quedarse con el top-N, medirlo en el
//      out-of-sample inmediatamente siguiente.
//   B) Revalidación cruzada: cada candidato se vuelve a medir en TODAS las ventanas
//      out-of-sample POSTERIORES al fold que lo descubrió. Una combinación que sobrevive a
//      una sola ventana puede haber tenido suerte; la exigencia es que aguante varias.

const { MuestreadorTPE } = require('./tpe');
const { crearVentana } = require('./datos');
const { firma } = require('./espacio');
const { mediana } = require('./metricas');

// Fracción de la ventana que se corre antes de decidir si abandonar el trial.
const FRACCION_PRUNING = 0.4;

// Mediana móvil de los scores parciales ya vistos: el umbral para abandonar un trial a mitad
// de camino. Optuna llama a esto MedianPruner. Como el prefijo reutiliza los indicadores ya
// memoizados, abandonar cuesta ~40% de una corrida en vez del 100%.
class PodadorMediana {
    constructor({ minTrials = 25 } = {}) {
        this.parciales = [];
        this.minTrials = minTrials;
    }
    // Con pocos datos no se poda nada: cortar contra una mediana de 3 observaciones descarta
    // combinaciones buenas por ruido.
    debePodar(scoreParcial) {
        if (this.parciales.length < this.minTrials) return false;
        return scoreParcial < mediana(this.parciales);
    }
    registrar(scoreParcial) {
        if (Number.isFinite(scoreParcial)) this.parciales.push(scoreParcial);
    }
}

/**
 * Optimiza una ventana in-sample y devuelve el top-N de combinaciones.
 * Despacha `pool.n` trials en paralelo: el TPE tolera bien el paralelismo porque cada
 * propuesta se genera con el historial disponible en ese momento (es lo que hace Optuna en
 * modo distribuido); a cambio de un poco de redundancia se usan todos los núcleos.
 */
async function optimizarVentana({ pool, espacio, ventana, trials, topN, semilla, alRegistrar, usarPruning = true }) {
    const muestreador = new MuestreadorTPE(espacio, { semilla, nArranque: Math.max(20, Math.floor(trials * 0.15)) });
    const podador = new PodadorMediana();
    await pool.cargarVentana(ventana);

    let lanzados = 0, completados = 0, podados = 0;
    const enVuelo = new Set();

    const correrUno = async () => {
        const cfg = muestreador.sugerir();
        const nroTrial = ++lanzados;

        if (usarPruning) {
            const parcial = await pool.evaluar(cfg, FRACCION_PRUNING);
            podador.registrar(parcial.score);
            if (podador.debePodar(parcial.score)) {
                podados++;
                // Un trial podado NO entra al modelo del muestreador: su score parcial no es
                // comparable con los completos (mide menos días), y mezclarlos corrompería el
                // ranking del que salen las distribuciones de "buenos" y "malos".
                if (alRegistrar) alRegistrar({ nroTrial, cfg, podado: true, metricas: parcial.metricas, score: parcial.score });
                return;
            }
        }

        const r = await pool.evaluar(cfg, 1);
        muestreador.registrar(cfg, r.score);
        completados++;
        if (alRegistrar) alRegistrar({ nroTrial, cfg, podado: false, metricas: r.metricas, score: r.score });
    };

    // Mantiene el pool lleno hasta agotar el presupuesto de trials.
    await new Promise((resolve, reject) => {
        const rellenar = () => {
            while (lanzados < trials && enVuelo.size < pool.n) {
                const t = correrUno().then(() => { enVuelo.delete(t); rellenar(); }, reject);
                enVuelo.add(t);
            }
            if (!enVuelo.size) resolve();
        };
        rellenar();
    });

    return { top: muestreador.top(topN), completados, podados, historial: muestreador.historial };
}

/**
 * Corre el estudio walk-forward completo.
 */
async function correrWalkForward({
    pool, espacio, historico, ventanas, trials, topN, semilla,
    // Tres ventanas y no dos: con dos, basta que a una combinación mediocre le toquen los dos
    // tramos donde el mercado la favoreció para que pase. Pasó de verdad en las pruebas —una
    // combinación aceptada con 2 ventanas daba -12.9% sobre el período completo—, y por eso
    // además de subir el mínimo se agrega la auditoría de período completo de más abajo.
    minVentanasOOS = 3, maxDegradacion = 0.5, minPctPositivas = 0.6,
    usarPruning = true, alRegistrar, alProgreso,
}) {
    const folds = [];
    const candidatos = new Map(); // firma -> { cfg, foldOrigen, scoreIS, metricasIS }

    // ── Fase A: optimizar cada in-sample y medir su out-of-sample ──
    for (const v of ventanas) {
        const ventanaIS = crearVentana(historico, v.is.desde, v.is.hasta);
        if (alProgreso) alProgreso({ fase: 'optimizando', fold: v.indice, total: ventanas.length, ventana: v });

        const res = await optimizarVentana({
            pool, espacio, ventana: ventanaIS, trials, topN, usarPruning,
            semilla: semilla + v.indice * 7919,
            alRegistrar: (t) => alRegistrar && alRegistrar({ ...t, fold: v.indice, fase: 'is', periodo: v.is }),
        });

        // El top-N se mide en el out-of-sample siguiente, SIN volver a optimizar nada.
        const ventanaOOS = crearVentana(historico, v.oos.desde, v.oos.hasta);
        await pool.cargarVentana(ventanaOOS);
        if (alProgreso) alProgreso({ fase: 'validando', fold: v.indice, total: ventanas.length, ventana: v });

        const oos = await Promise.all(res.top.map(t => pool.evaluar(t.cfg, 1)));
        const resultadosOOS = res.top.map((t, i) => ({
            cfg: t.cfg, scoreIS: t.score, scoreOOS: oos[i].score, metricasOOS: oos[i].metricas,
        }));
        resultadosOOS.forEach((r, i) => alRegistrar && alRegistrar({
            nroTrial: `top${i + 1}`, cfg: r.cfg, podado: false,
            metricas: r.metricasOOS, score: r.scoreOOS,
            fold: v.indice, fase: 'oos', periodo: v.oos,
        }));

        folds.push({
            indice: v.indice, periodoIS: v.is, periodoOOS: v.oos,
            completados: res.completados, podados: res.podados,
            mejorIS: res.top[0] ?? null,
            // Eficiencia walk-forward: cuánto del desempeño in-sample sobrevive fuera de la
            // muestra. Es el termómetro del PROCESO, no de una combinación puntual: si da
            // sistemáticamente negativo, lo que está roto es la búsqueda entera.
            eficiencia: res.top[0] && res.top[0].score > 0 ? (resultadosOOS[0].scoreOOS / res.top[0].score) : null,
            top: resultadosOOS,
        });

        for (const t of res.top) {
            const f = firma(t.cfg, espacio);
            if (!candidatos.has(f)) candidatos.set(f, { cfg: t.cfg, foldOrigen: v.indice, scoreIS: t.score });
        }
    }

    // ── Fase B: revalidar cada candidato en todas las ventanas OOS posteriores a su fold ──
    // "Posteriores" es la parte que importa: medir un candidato descubierto en el fold 5 sobre
    // el out-of-sample del fold 1 no prueba nada, porque esos datos ya estuvieron dentro del
    // in-sample con el que se lo optimizó. Solo cuenta lo que vino DESPUÉS.
    if (alProgreso) alProgreso({ fase: 'revalidando', total: candidatos.size });
    const listaCandidatos = [...candidatos.values()];

    for (const v of ventanas) {
        const aMedir = listaCandidatos.filter(c => c.foldOrigen < v.indice);
        if (!aMedir.length) continue;
        const ventanaOOS = crearVentana(historico, v.oos.desde, v.oos.hasta);
        await pool.cargarVentana(ventanaOOS);
        const rs = await Promise.all(aMedir.map(c => pool.evaluar(c.cfg, 1)));
        aMedir.forEach((c, i) => {
            (c.oosPosteriores ??= []).push({ fold: v.indice, periodo: v.oos, score: rs[i].score, metricas: rs[i].metricas });
            alRegistrar && alRegistrar({
                nroTrial: 'reval', cfg: c.cfg, podado: false, metricas: rs[i].metricas,
                score: rs[i].score, fold: v.indice, fase: 'revalidacion', periodo: v.oos,
            });
        });
    }

    // ── Criterio de aceptación ──
    const ranking = listaCandidatos.map(c => {
        const oos = c.oosPosteriores ?? [];
        const scores = oos.map(o => o.score);
        // Sin ventanas posteriores no hay nada que medir. Es distinto de "midió cero": los
        // candidatos del último fold no tienen futuro contra el cual validarse, y si se les
        // asignara un 0 quedarían ordenados por encima de los que sí se midieron y dieron
        // negativo — o sea, la falta de evidencia se leería como evidencia buena.
        const medianaOOS = scores.length ? mediana(scores) : null;
        const pctPositivas = scores.length ? scores.filter(s => s > 0).length / scores.length : 0;
        // Degradación: cuánto del score in-sample se perdió al salir de la muestra. Con score
        // in-sample <= 0 el cociente no significa nada, así que se marca como no evaluable.
        const degradacion = (c.scoreIS > 0 && medianaOOS !== null) ? 1 - medianaOOS / c.scoreIS : null;

        const aceptado =
            scores.length >= minVentanasOOS &&
            pctPositivas >= minPctPositivas &&
            degradacion !== null && degradacion <= maxDegradacion;

        const motivos = [];
        if (scores.length < minVentanasOOS) motivos.push(`solo ${scores.length} ventana(s) OOS posteriores (mínimo ${minVentanasOOS})`);
        if (pctPositivas < minPctPositivas) motivos.push(`${(pctPositivas * 100).toFixed(0)}% de ventanas OOS positivas (mínimo ${(minPctPositivas * 100).toFixed(0)}%)`);
        if (degradacion === null) motivos.push('score in-sample no positivo: la degradación no es interpretable');
        else if (degradacion > maxDegradacion) motivos.push(`degradación ${(degradacion * 100).toFixed(0)}% (máximo ${(maxDegradacion * 100).toFixed(0)}%)`);

        return {
            cfg: c.cfg, foldOrigen: c.foldOrigen, scoreIS: c.scoreIS,
            nVentanasOOS: scores.length, medianaOOS, pctPositivas, degradacion,
            aceptado, motivos, detalleOOS: oos,
            // Métricas agregadas sobre las ventanas OOS: es lo que de verdad se puede esperar
            // de la combinación, no lo que rindió en el período con el que se la ajustó.
            retornoOOSMediano: oos.length ? mediana(oos.map(o => o.metricas.retornoPerc)) : null,
            ddOOSMaximo: oos.length ? Math.max(...oos.map(o => o.metricas.maxDDPerc)) : null,
            tradesOOSMediano: oos.length ? mediana(oos.map(o => o.metricas.trades)) : null,
            winRateOOSMediano: oos.length ? mediana(oos.map(o => o.metricas.winRate)) : null,
        };
    });

    // Ordenar por lo que se sostiene fuera de la muestra, no por lo que brilló dentro. Los
    // candidatos sin ninguna ventana posterior van al final: no se los puede ni promover ni
    // descartar, y mezclarlos con los medidos ensuciaría la lectura del ranking.
    ranking.sort((a, b) =>
        (b.aceptado - a.aceptado) ||
        ((a.medianaOOS === null) - (b.medianaOOS === null)) ||
        ((b.medianaOOS ?? -Infinity) - (a.medianaOOS ?? -Infinity))
    );

    // ── Fase C: auditoría sobre el período completo ──
    // Las métricas out-of-sample son medianas de ventanas de pocas semanas, y una mediana de
    // tres ventanas puede tapar que la curva completa va hacia abajo. Correr cada finalista de
    // punta a punta no valida nada (el período incluye sus propios datos de ajuste), pero sí
    // DESMIENTE: si sobre 150 días la combinación pierde plata, no importa lo lindas que
    // hayan salido las ventanas sueltas. Es una prueba de descarte, no de promoción.
    const finalistas = ranking.filter(r => r.aceptado).slice(0, 20);
    if (finalistas.length) {
        if (alProgreso) alProgreso({ fase: 'auditando', total: finalistas.length });
        const periodoCompleto = { desde: ventanas[0].is.desde, hasta: ventanas[ventanas.length - 1].oos.hasta };
        const ventanaCompleta = crearVentana(historico, periodoCompleto.desde, periodoCompleto.hasta);
        await pool.cargarVentana(ventanaCompleta);
        const auditorias = await Promise.all(finalistas.map(r => pool.evaluar(r.cfg, 1)));
        finalistas.forEach((r, i) => {
            r.periodoCompleto = { periodo: periodoCompleto, ...auditorias[i].metricas };
            // Contradicción: las ventanas dijeron que sí y el período entero dice que no.
            r.contradice = auditorias[i].metricas.retornoPerc <= 0 || auditorias[i].metricas.profitFactor < 1;
            if (r.contradice) {
                r.aceptado = false;
                r.motivos.push(
                    `sobre el período completo rinde ${auditorias[i].metricas.retornoPerc.toFixed(1)}% ` +
                    `con profit factor ${auditorias[i].metricas.profitFactor.toFixed(2)}: las ventanas OOS ` +
                    `favorables no representan a la curva entera`
                );
            }
            alRegistrar && alRegistrar({
                nroTrial: 'auditoria', cfg: r.cfg, podado: false, metricas: auditorias[i].metricas,
                score: auditorias[i].score, fold: '', fase: 'periodo-completo', periodo: periodoCompleto,
            });
        });
        // El descarte puede cambiar el orden: se reordena con el mismo criterio.
        ranking.sort((a, b) =>
            (b.aceptado - a.aceptado) ||
            ((a.medianaOOS === null) - (b.medianaOOS === null)) ||
            ((b.medianaOOS ?? -Infinity) - (a.medianaOOS ?? -Infinity))
        );
    }

    const eficiencias = folds.map(f => f.eficiencia).filter(e => e !== null);
    return {
        folds, ranking,
        resumen: {
            ventanas: ventanas.length,
            candidatosEvaluados: listaCandidatos.length,
            aceptados: ranking.filter(r => r.aceptado).length,
            descartadosPorAuditoria: ranking.filter(r => r.contradice).length,
            eficienciaMediana: eficiencias.length ? mediana(eficiencias) : null,
        },
    };
}

module.exports = { correrWalkForward, optimizarVentana, PodadorMediana, FRACCION_PRUNING };
