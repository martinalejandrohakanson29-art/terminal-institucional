// Pool de workers. Los backtests son CPU puro y Node corre en un solo hilo, así que sin esto
// se desperdician los otros núcleos de la máquina. El hilo principal se queda con el
// muestreador (que es barato) y reparte las evaluaciones (que no lo son).

const path = require('path');
const os = require('os');
const { Worker } = require('worker_threads');

class PoolEvaluadores {
    constructor({ fijos, cfgObjetivo, nWorkers }) {
        // Se deja un núcleo libre: si se ocupan todos, el hilo principal compite por CPU con
        // sus propios workers y el reparto de trabajo se vuelve errático.
        this.n = Math.max(1, nWorkers ?? Math.max(1, os.cpus().length - 1));
        this.pendientes = new Map(); // id -> {resolve, reject}
        this.siguienteId = 1;
        this.libres = [];
        this.cola = [];
        this.workers = [];

        for (let i = 0; i < this.n; i++) {
            const w = new Worker(path.join(__dirname, 'worker.js'), { workerData: { fijos, cfgObjetivo } });
            w.on('message', (msg) => this._recibir(w, msg));
            w.on('error', (err) => {
                // Un worker caído deja su trial colgado para siempre si no se rechaza acá.
                for (const [id, pend] of this.pendientes) { pend.reject(err); this.pendientes.delete(id); }
            });
            this.workers.push(w);
            this.libres.push(w);
        }
    }

    _recibir(w, msg) {
        const pend = this.pendientes.get(msg.id);
        this.pendientes.delete(msg.id);
        this.libres.push(w);
        this._despachar();
        if (!pend) return;
        if (msg.tipo === 'error') pend.reject(new Error(msg.error + '\n' + msg.stack));
        else pend.resolve(msg);
    }

    _despachar() {
        while (this.cola.length && this.libres.length) {
            const w = this.libres.pop();
            const tarea = this.cola.shift();
            this.pendientes.set(tarea.msg.id, tarea);
            w.postMessage(tarea.msg);
        }
    }

    _enviar(msg) {
        return new Promise((resolve, reject) => {
            this.cola.push({ msg, resolve, reject });
            this._despachar();
        });
    }

    // Carga la misma ventana en TODOS los workers. Hay que esperar a que terminen todos antes
    // de mandar trials: un worker que todavía tiene la ventana anterior devolvería métricas
    // del período equivocado sin que nada lo delate.
    async cargarVentana(ventana) {
        if (this.pendientes.size || this.cola.length) throw new Error('cargarVentana() con trials en vuelo');
        // Se le manda a cada worker directamente, sin pasar por la cola: la cola reparte "a
        // quien esté libre", y acá hace falta que la reciban todos y cada uno.
        this.libres = [];
        await Promise.all(this.workers.map(w => new Promise((resolve, reject) => {
            const id = this.siguienteId++;
            this.pendientes.set(id, { resolve, reject });
            w.postMessage({ tipo: 'ventana', id, ventana });
        })));
    }

    evaluar(cfg, fraccion = 1) {
        return this._enviar({ tipo: 'evaluar', id: this.siguienteId++, cfg, fraccion });
    }

    async cerrar() {
        await Promise.all(this.workers.map(w => w.terminate()));
    }
}

module.exports = { PoolEvaluadores };
