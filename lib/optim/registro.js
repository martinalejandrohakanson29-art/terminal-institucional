// ============================================================
// REGISTRO DE TRIALS — CSV auditable
// ============================================================
// Guardar SOLO el ranking final es perder la mitad de la información: los trials descartados
// son los que dicen qué rangos no sirven, qué filtro no aporta nunca y si el optimizador se
// quedó atascado en una zona. Una fila por evaluación, en CSV, para filtrar y ordenar después
// en cualquier planilla.

const fs = require('fs');
const path = require('path');

const escapar = (v) => {
    if (v === null || v === undefined) return '';
    const s = String(v);
    return /[",\n;]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
};
const num = (v, dec = 4) => (Number.isFinite(v) ? v.toFixed(dec) : '');
const fecha = (ms) => new Date(ms).toISOString().slice(0, 16).replace('T', ' ');

class RegistroCSV {
    constructor(rutaArchivo, espacio) {
        this.ruta = rutaArchivo;
        // Una columna por parámetro buscado, además del JSON completo: las columnas sirven
        // para ordenar y hacer tablas dinámicas, el JSON para reproducir la combinación exacta.
        this.columnasParams = espacio.map(d => d.nombre);
        fs.mkdirSync(path.dirname(rutaArchivo), { recursive: true });

        const cabecera = [
            'fold', 'fase', 'trial', 'podado',
            'periodo_desde', 'periodo_hasta', 'dias',
            'score', 'sharpe', 'retorno_perc', 'max_dd_perc', 'calmar', 'profit_factor',
            'trades', 'win_rate', 'longs', 'shorts', 'max_racha_perdedora',
            ...this.columnasParams, 'params_json',
        ];
        this.stream = fs.createWriteStream(rutaArchivo, { encoding: 'utf8' });
        this.stream.write(cabecera.join(',') + '\n');
        this.filas = 0;
    }

    escribir({ fold, fase, nroTrial, podado, periodo, cfg, metricas, score }) {
        const m = metricas ?? {};
        const fila = [
            fold ?? '', fase ?? '', nroTrial ?? '', podado ? 'si' : 'no',
            periodo ? fecha(periodo.desde) : '', periodo ? fecha(periodo.hasta) : '',
            periodo ? ((periodo.hasta - periodo.desde) / 86400000).toFixed(1) : '',
            num(score), num(m.sharpe), num(m.retornoPerc, 3), num(m.maxDDPerc, 3),
            num(m.calmar, 3), num(m.profitFactor, 3),
            m.trades ?? '', num(m.winRate, 2), m.longs ?? '', m.shorts ?? '', m.maxLossStreak ?? '',
            ...this.columnasParams.map(c => escapar(cfg?.[c])),
            escapar(JSON.stringify(cfg ?? {})),
        ];
        this.stream.write(fila.join(',') + '\n');
        this.filas++;
    }

    cerrar() {
        return new Promise(resolve => this.stream.end(resolve));
    }
}

// Reporte final en JSON: el ranking con todo el detalle, para reprocesar sin volver a correr
// el estudio (que son minutos de CPU).
function guardarReporte(ruta, reporte) {
    fs.mkdirSync(path.dirname(ruta), { recursive: true });
    fs.writeFileSync(ruta, JSON.stringify(reporte, null, 2), 'utf8');
}

module.exports = { RegistroCSV, guardarReporte };
