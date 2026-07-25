const binance = require('./binance');
const bingx = require('./bingx');

const EXCHANGES = { binance, bingx };

function getExchange(nombre) {
    const ex = EXCHANGES[String(nombre || '').toLowerCase()];
    if (!ex) throw new Error(`Exchange desconocido: "${nombre}". Válidos: ${Object.keys(EXCHANGES).join(', ')}`);
    return ex;
}

module.exports = { getExchange, EXCHANGES };
