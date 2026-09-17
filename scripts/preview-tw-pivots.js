// Isolated UI fixture: synthetic candles, no database, exchange or trading calls.
// Run: node scripts/preview-tw-pivots.js, then http://127.0.0.1:4319
const express = require('express');
const fs = require('node:fs');
const path = require('node:path');
const { TF } = require('../public/tw-pivots');
const app = express();
app.use(express.json());
app.get('/', (req, res) => {
    let html = fs.readFileSync(path.join(__dirname, '../public/index.html'), 'utf8');
    const start = html.indexOf('(async function arrancar() {');
    const end = html.indexOf('})();', start) + 5;
    html = html.slice(0, start) + `(async function arrancar() {
        renderChips(); await cargarDatos('1m');
    })();` + html.slice(end);
    html = html.replace('<head>', `<head><script>
        window.WebSocket = class { constructor() { this.readyState = 1; } close() {} send() {} };
        window.addEventListener('error', e => { const p = document.createElement('p'); p.textContent = 'ERROR: ' + e.message; document.body.prepend(p); });
    </script>`);
    res.type('html').send(html);
});
app.get('/api/klines', (req, res) => {
    const ms = TF[req.query.interval] || TF['1m'];
    const end = Math.floor(Date.now() / ms) * ms;
    const rows = [];
    for (let i = 0; i < 800; i++) {
        const ts = end - (799 - i) * ms;
        const mid = 60000 + 1200 * Math.sin(ts / 3600000 * .8);
        const open = mid + 120 * Math.sin(i), close = mid + 120 * Math.cos(i);
        rows.push([ts, open, Math.max(open, close) + 120, Math.min(open, close) - 160, close, 10]);
    }
    res.json(rows.slice(-Math.min(+req.query.limit || 1000, 10000)));
});
app.all('/api/*', (req, res) => res.json([]));
app.use(express.static(path.join(__dirname, '../public')));
app.listen(4319, '127.0.0.1', () => console.log('TW fixture: http://127.0.0.1:4319'));
