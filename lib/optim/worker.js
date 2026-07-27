// Proceso trabajador: mantiene UNA ventana cargada (con su cache de indicadores) y evalúa las
// combinaciones que le manda el hilo principal. La ventana viaja una sola vez por fold; los
// trials son mensajes chicos. Al revés —mandar las velas con cada trial— el costo de
// serializar 40 000 velas dominaría por completo el de correr el backtest.

const { parentPort, workerData } = require('worker_threads');
const { prepararVentana, evaluar } = require('./evaluador');

let ventana = null;
const { fijos, cfgObjetivo } = workerData;

parentPort.on('message', (msg) => {
    try {
        if (msg.tipo === 'ventana') {
            // Cambiar de ventana descarta la cache anterior: los indicadores son de esas velas.
            ventana = prepararVentana(msg.ventana);
            parentPort.postMessage({ tipo: 'ventana-lista', id: msg.id });
            return;
        }
        if (msg.tipo === 'evaluar') {
            const r = evaluar(ventana, msg.cfg, fijos, cfgObjetivo, msg.fraccion ?? 1);
            parentPort.postMessage({ tipo: 'resultado', id: msg.id, ...r });
        }
    } catch (e) {
        parentPort.postMessage({ tipo: 'error', id: msg.id, error: e.message, stack: e.stack });
    }
});
