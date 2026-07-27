// ============================================================
// MUESTREADOR TPE — búsqueda bayesiana
// ============================================================
// Tree-structured Parzen Estimator: el mismo algoritmo que usa Optuna por defecto, acá en JS
// para no tener que cruzar un puente Python↔Node en cada trial (con backtests de ~300 ms, el
// costo de serializar y despachar por subproceso/HTTP sería una fracción enorme del total).
//
// La idea, en una línea: en vez de modelar "qué score da esta combinación" (que es lo que
// haría un proceso gaussiano), se modelan DOS distribuciones sobre los parámetros — l(x), la
// de las combinaciones buenas, y g(x), la del resto — y se propone el candidato que maximiza
// l(x)/g(x): el más típico entre los buenos y el más raro entre los malos.
//
// Por qué no grid search: con ~45 parámetros, aunque solo se probaran 3 valores de cada uno,
// la grilla tiene 3^45 puntos. Con 300 evaluaciones el muestreo bayesiano converge a las zonas
// buenas del espacio; la grilla ni siquiera termina de recorrer los primeros dos parámetros.
//
// Este archivo no sabe nada de trading: recibe descriptores de parámetros y scores.

const { reparar, firma } = require('./espacio');

// ── Aleatoriedad reproducible ───────────────────────────────────────────────────────────
// Semilla explícita para que dos corridas con la misma configuración den el mismo resultado:
// sin eso no se puede distinguir "cambié el rango y mejoró" de "salió otro sorteo".
function crearRng(semilla) {
    let s = (semilla >>> 0) || 1;
    return () => {
        s ^= s << 13; s >>>= 0;
        s ^= s >> 17;
        s ^= s << 5;  s >>>= 0;
        return s / 4294967296;
    };
}

// Normal estándar por Box-Muller.
function normal(rng) {
    let u = 0, v = 0;
    while (u === 0) u = rng();
    while (v === 0) v = rng();
    return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
}

// Aproximación de la función error (Abramowitz & Stegun 7.1.26). Se usa para normalizar las
// gaussianas truncadas al rango del parámetro: sin eso, los valores pegados a un extremo del
// rango quedan sistemáticamente subvaluados, y los óptimos suelen vivir justo en los extremos.
function erf(x) {
    const signo = x < 0 ? -1 : 1;
    x = Math.abs(x);
    const t = 1 / (1 + 0.3275911 * x);
    const coefs = [0.254829592, -0.284496736, 1.421413741, -1.453152027, 1.061405429];
    let poly = 0, tp = t;
    for (const c of coefs) { poly += c * tp; tp *= t; }
    return signo * (1 - poly * Math.exp(-x * x));
}
const cdfNormal = (x, mu, sigma) => 0.5 * (1 + erf((x - mu) / (sigma * Math.SQRT2)));

// ── Transformación al espacio donde se muestrea ─────────────────────────────────────────
// Los parámetros con escala 'log' se modelan en logaritmo: un rango de 5 a 240 minutos en
// escala lineal casi nunca propone valores por debajo de 30, y ahí es donde el comportamiento
// de la estrategia realmente cambia.
const aTransformado = (d, v) => (d.escala === 'log' ? Math.log(v) : v);
const deTransformado = (d, x) => {
    const v = d.escala === 'log' ? Math.exp(x) : x;
    const acotado = Math.min(d.max, Math.max(d.min, v));
    return d.tipo === 'int' ? Math.round(acotado) : acotado;
};
const limites = (d) => (d.escala === 'log'
    ? { lo: Math.log(d.min), hi: Math.log(d.max) }
    : { lo: d.min, hi: d.max });

const opcionesDe = (d) => (d.tipo === 'bool' ? [false, true] : d.opciones);

// ── Estimador de Parzen para un parámetro numérico ──────────────────────────────────────
// Una mezcla de gaussianas: una por observación, más una "previa" ancha centrada en el medio
// del rango. La previa evita que con 3 observaciones el modelo colapse sobre ellas y deje de
// explorar; su peso se diluye solo a medida que llegan datos.
function parzenNumerico(d, valores) {
    const { lo, hi } = limites(d);
    const mus = [...valores.map(v => aTransformado(d, v)), (lo + hi) / 2];
    const pesoPrevia = 1;
    const pesos = [...valores.map(() => 1), pesoPrevia];

    // Ancho de banda por componente: la distancia al vecino más lejano entre sus dos vecinos.
    // Donde hay muchas observaciones juntas las gaussianas se angostan (el modelo afina), y
    // donde hay una sola queda ancha (el modelo admite que no sabe).
    const orden = mus.map((m, i) => i).sort((a, b) => mus[a] - mus[b]);
    const sigmaMin = (hi - lo) / Math.min(100, 1 + mus.length);
    const sigmas = new Array(mus.length);
    for (let k = 0; k < orden.length; k++) {
        const i = orden[k];
        const izq = k > 0 ? mus[i] - mus[orden[k - 1]] : 0;
        const der = k < orden.length - 1 ? mus[orden[k + 1]] - mus[i] : 0;
        sigmas[i] = Math.min(hi - lo, Math.max(sigmaMin, Math.max(izq, der)));
    }
    // La previa siempre queda ancha: es la que sostiene la exploración.
    sigmas[mus.length - 1] = hi - lo;

    const total = pesos.reduce((s, w) => s + w, 0);
    return {
        muestrear(rng) {
            // Elegir componente proporcional a su peso, y sortear dentro de esa gaussiana.
            let r = rng() * total, i = 0;
            while (i < pesos.length - 1 && (r -= pesos[i]) > 0) i++;
            for (let intento = 0; intento < 20; intento++) {
                const x = mus[i] + sigmas[i] * normal(rng);
                if (x >= lo && x <= hi) return x;
            }
            return Math.min(hi, Math.max(lo, mus[i]));
        },
        logDensidad(x) {
            let acum = 0;
            for (let i = 0; i < mus.length; i++) {
                // Normalización por truncamiento al rango [lo, hi].
                const z = cdfNormal(hi, mus[i], sigmas[i]) - cdfNormal(lo, mus[i], sigmas[i]);
                const dens = Math.exp(-((x - mus[i]) ** 2) / (2 * sigmas[i] ** 2)) / (sigmas[i] * Math.sqrt(2 * Math.PI));
                acum += (pesos[i] / total) * dens / Math.max(z, 1e-12);
            }
            return Math.log(Math.max(acum, 1e-300));
        },
    };
}

// ── Estimador para un parámetro categórico ──────────────────────────────────────────────
// Conteos suavizados: cada opción arranca con un voto "de oficio" para que una opción que
// todavía no salió nunca no quede con probabilidad cero y deje de proponerse.
function parzenCategorico(opciones, valores) {
    const pesoPrevia = 1;
    const conteo = new Map(opciones.map(o => [String(o), pesoPrevia]));
    for (const v of valores) conteo.set(String(v), (conteo.get(String(v)) ?? pesoPrevia) + 1);
    const total = [...conteo.values()].reduce((s, c) => s + c, 0);
    return {
        muestrear(rng) {
            let r = rng() * total;
            for (const o of opciones) { r -= conteo.get(String(o)); if (r <= 0) return o; }
            return opciones[opciones.length - 1];
        },
        logDensidad(v) { return Math.log((conteo.get(String(v)) ?? pesoPrevia) / total); },
    };
}

class MuestreadorTPE {
    /**
     * @param espacio  descriptores de parámetros (ver espacio.js)
     * @param opciones { nArranque, nCandidatos, semilla }
     */
    constructor(espacio, opciones = {}) {
        this.espacio = espacio;
        this.nArranque = opciones.nArranque ?? 40;   // trials aleatorios antes de modelar
        this.nCandidatos = opciones.nCandidatos ?? 24; // propuestas por parámetro, se queda la mejor
        this.rng = crearRng(opciones.semilla ?? 12345);
        this.historial = [];         // { cfg, score }
        this.vistas = new Set();     // firmas ya evaluadas
    }

    // Cuántos trials entran al grupo "bueno". Crece con la raíz del total (criterio de Optuna):
    // al principio conviene un grupo chico y exigente; con cientos de trials, quedarse con los
    // 3 mejores haría un modelo hipersensible al ruido de una ventana concreta.
    _corteBuenos(n) {
        return Math.min(Math.ceil(0.15 * n), Math.max(2, Math.floor(Math.sqrt(n))), 40);
    }

    _aleatoria() {
        const cfg = {};
        for (const d of this.espacio) {
            if (d.si && !d.si(cfg)) continue;
            if (d.tipo === 'bool' || d.tipo === 'cat') {
                const ops = opcionesDe(d);
                cfg[d.nombre] = ops[Math.floor(this.rng() * ops.length)];
            } else {
                const { lo, hi } = limites(d);
                cfg[d.nombre] = deTransformado(d, lo + this.rng() * (hi - lo));
            }
        }
        return reparar(cfg);
    }

    _bayesiana() {
        const orden = [...this.historial].sort((a, b) => b.score - a.score);
        const nBuenos = this._corteBuenos(orden.length);
        const buenos = orden.slice(0, nBuenos);
        const malos  = orden.slice(nBuenos);

        const cfg = {};
        for (const d of this.espacio) {
            // Los parámetros de un filtro apagado no se muestrean NI entran al modelo: así la
            // dimensionalidad efectiva del problema baja sola a medida que se descartan filtros.
            if (d.si && !d.si(cfg)) continue;

            const valoresDe = (grupo) => grupo
                .map(t => t.cfg[d.nombre])
                .filter(v => v !== undefined);
            const vBuenos = valoresDe(buenos), vMalos = valoresDe(malos);

            // Sin observaciones activas de este parámetro no hay nada que modelar todavía.
            if (vBuenos.length === 0) {
                const aleatoria = this._aleatoria();
                cfg[d.nombre] = aleatoria[d.nombre];
                continue;
            }

            const esCategorico = d.tipo === 'bool' || d.tipo === 'cat';
            const l = esCategorico ? parzenCategorico(opcionesDe(d), vBuenos) : parzenNumerico(d, vBuenos);
            const g = esCategorico ? parzenCategorico(opcionesDe(d), vMalos)  : parzenNumerico(d, vMalos);

            // Se sortean varios candidatos de la distribución de los buenos y se queda el que
            // maximiza l/g: el más típico entre los buenos que además sea raro entre los malos.
            let mejor = null, mejorRatio = -Infinity;
            for (let i = 0; i < this.nCandidatos; i++) {
                const c = l.muestrear(this.rng);
                const ratio = l.logDensidad(c) - g.logDensidad(c);
                if (ratio > mejorRatio) { mejorRatio = ratio; mejor = c; }
            }
            cfg[d.nombre] = esCategorico ? mejor : deTransformado(d, mejor);
        }
        return reparar(cfg);
    }

    // Devuelve la próxima combinación a evaluar. Reintenta si ya se evaluó una equivalente:
    // repetir un trial idéntico gasta presupuesto sin aportar información nueva.
    sugerir() {
        const bayesiana = this.historial.length >= this.nArranque;
        for (let intento = 0; intento < 12; intento++) {
            const cfg = bayesiana && intento < 8 ? this._bayesiana() : this._aleatoria();
            const f = firma(cfg, this.espacio);
            if (!this.vistas.has(f)) { this.vistas.add(f); return cfg; }
        }
        // Espacio muy explorado en esta zona: se devuelve igual y se deja que el registro lo
        // trate como repetido (mejor eso que entrar en un bucle infinito de reintentos).
        return this._aleatoria();
    }

    registrar(cfg, score) {
        if (Number.isFinite(score)) this.historial.push({ cfg, score });
    }

    mejor() {
        return this.historial.reduce((a, b) => (b.score > a.score ? b : a), this.historial[0] ?? null);
    }

    // Top-N combinaciones distintas por score. El walk-forward las necesita a todas, no solo
    // la mejor: la #1 in-sample suele ser la más sobreajustada, y con frecuencia la que
    // sobrevive out-of-sample está unos puestos más abajo.
    top(n) {
        return [...this.historial].sort((a, b) => b.score - a.score).slice(0, n);
    }
}

module.exports = { MuestreadorTPE, crearRng };
