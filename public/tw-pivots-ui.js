/* Integration with the terminal's chart, exchange selector and saved layout. */
const twState = { visible: false, config: TWPivots.normalize(), result: null, generation: 0, timer: null, controller: null, rows: {}, lastFetch: 0 };
klinecharts.registerOverlay({
    name: 'twZone', totalStep: 1, lock: true,
    needDefaultPointFigure: false, needDefaultXAxisFigure: false, needDefaultYAxisFigure: false,
    createPointFigures: ({ coordinates, overlay }) => {
        if (coordinates.length < 2) return [];
        const [a, b] = coordinates, color = overlay.extendData.side === 'support' ? '38,166,154' : '239,83,80';
        return [{ type: 'polygon', ignoreEvent: true, attrs: { coordinates: [{ x: a.x, y: a.y }, { x: b.x, y: a.y }, { x: b.x, y: b.y }, { x: a.x, y: b.y }] }, styles: { style: 'stroke_fill', color: `rgba(${color},.10)`, borderColor: `rgba(${color},.5)`, borderSize: 1 } }];
    }
});
klinecharts.registerOverlay({
    name: 'twLabel', totalStep: 1, lock: true,
    needDefaultPointFigure: false, needDefaultXAxisFigure: false, needDefaultYAxisFigure: false,
    createPointFigures: ({ coordinates, overlay }) => {
        if (!coordinates.length) return [];
        const a = coordinates[0], d = overlay.extendData, bull = d.side === 'bull';
        return [{ type: 'text', ignoreEvent: true, attrs: { x: a.x, y: a.y + (bull ? 16 : -16), text: (bull ? '▲ ' : '▼ ') + d.text, align: 'center', baseline: bull ? 'top' : 'bottom' }, styles: { color: d.early ? '#a16207' : bull ? '#00897b' : '#d32f2f', size: 11, backgroundColor: 'rgba(255,255,255,.85)', paddingLeft: 3, paddingRight: 3 } }];
    }
});
klinecharts.registerOverlay({
    name: 'twNeckline', totalStep: 1, lock: true,
    needDefaultPointFigure: false, needDefaultXAxisFigure: false, needDefaultYAxisFigure: false,
    createPointFigures: ({ coordinates }) => coordinates.length < 2 ? [] : [{ type: 'line', ignoreEvent: true, attrs: { coordinates }, styles: { color: '#a16207', size: 1, style: 'dashed', dashedValue: [5, 4] } }]
});
function twStatus(text, error = false) {
    let el = document.getElementById('tw-status');
    if (!el) { el = document.createElement('span'); el.id = 'tw-status'; document.getElementById('chips-indicadores').after(el); }
    el.style.cssText = `font-size:11px;max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:${error ? '#b91c1c' : '#64748b'};display:${twState.visible ? 'inline' : 'none'}`;
    el.textContent = text;
    el.title = text + ' · Se muestran hasta 200 señales del historial cargado. Actualización cada 15 segundos.';
}
function twDraw() {
    chart.removeOverlay({ groupId: 'tw-mtf' });
    if (!twState.visible || !twState.result || !velasActuales.length) return;
    const data = velasActuales, first = data[0].timestamp, last = data[data.length - 1].timestamp;
    const c = twState.config, r = twState.result;
    const add = (name, points, extendData = {}) => chart.createOverlay({ name, groupId: 'tw-mtf', lock: true, points, extendData });
    // Anchor signals to the first chart bar available AT/AFTER confirmation, never in the past.
    const anchor = t => {
        let lo = 0, hi = data.length;
        while (lo < hi) { const mid = (lo + hi) >>> 1; if (data[mid].timestamp < t) lo = mid + 1; else hi = mid; }
        return lo < data.length ? data[lo].timestamp : null;
    };
    for (const z of r.zones) {
        if (z.start > last || !(z.side === 'support' ? c.showSupportZones : c.showResistanceZones)) continue;
        add('twZone', [{ timestamp: Math.max(first, z.start), value: z.upper }, { timestamp: last, value: z.lower }], z);
    }
    for (const s of r.signals.filter(s => s.time >= first && s.time <= last).slice(-200)) {
        const ts = anchor(s.time);
        if (ts !== null) add('twLabel', [{ timestamp: ts, value: s.price }], s);
    }
    if (c.showDoubleNeckline) for (const n of r.necklines) {
        if (n.start <= last) add('twNeckline', [{ timestamp: Math.max(first, n.start), value: n.neckline }, { timestamp: last, value: n.neckline }]);
    }
}
function twStop() {
    twState.generation++;
    clearTimeout(twState.timer);
    if (twState.controller) twState.controller.abort();
    twState.result = null;
    chart.removeOverlay({ groupId: 'tw-mtf' });
}
async function twRefresh(generation) {
    if (!twState.visible || generation !== twState.generation) return;
    const controller = new AbortController();
    twState.controller = controller;
    const timeout = setTimeout(() => controller.abort(), 30000);
    try {
        const c = twState.config, now = Date.now(), exchange = selectExchange.value;
        const frames = [...new Set([c.pivotTimeframe, c.patternTimeframe])];
        const results = await Promise.all(frames.map(async tf => {
            const full = !twState.rows[tf] || now - twState.lastFetch > TWPivots.TF[tf] * 5;
            const response = await fetch(`/api/klines?interval=${tf}&limit=${full ? 10000 : 10}&exchange=${encodeURIComponent(exchange)}`, { signal: controller.signal });
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const rows = await response.json();
            if (!Array.isArray(rows) || !rows.length) throw new Error('Sin velas disponibles');
            const merged = new Map((full ? [] : twState.rows[tf]).map(b => [+b[0], b]));
            for (const row of rows) if (Array.isArray(row)) merged.set(+row[0], row);
            return [tf, [...merged.values()].sort((a, b) => a[0] - b[0]).slice(-10000)];
        }));
        if (generation !== twState.generation) return;
        for (const [tf, rows] of results) twState.rows[tf] = rows;
        twState.lastFetch = now;
        twState.result = TWPivots.calculate(twState.rows[c.pivotTimeframe], twState.rows[c.patternTimeframe], c, now);
        twDraw();
        const r = twState.result;
        twStatus(r.pivotBars < c.pivotLeft + c.pivotRight + 1 || r.patternBars < 2 ? 'TW: historial insuficiente; esperando velas' : `TW ${c.pivotTimeframe}/${c.patternTimeframe} · ${r.zones.length} zonas · ${r.signals.length} señales`);
    } catch (error) {
        if (generation === twState.generation) {
            // Do not leave stale signals on screen after a failed refresh.
            twState.result = null;
            chart.removeOverlay({ groupId: 'tw-mtf' });
            twStatus('TW: no se pudo actualizar; reintentando…', true);
            console.error('TW MTF:', error);
        }
    } finally {
        clearTimeout(timeout);
        if (twState.visible && generation === twState.generation) twState.timer = setTimeout(() => twRefresh(generation), 15000);
    }
}
function twStart() {
    twStop();
    twState.rows = {}; twState.lastFetch = 0;
    if (twState.visible) { twStatus('TW: cargando velas cerradas…'); twRefresh(twState.generation); }
}
function twToggle() {
    twState.visible = !twState.visible;
    if (twState.visible) twStart(); else { twStop(); twStatus(''); }
    renderChips();
}
function twOpenConfig() {
    let dialog = document.getElementById('tw-config');
    if (!dialog) {
        dialog = document.createElement('dialog'); dialog.id = 'tw-config';
        dialog.style.cssText = 'margin:auto;padding:22px;border:1px solid #d1d4dc;border-radius:10px;width:620px;max-width:90vw;max-height:85vh;overflow:auto';
        document.body.append(dialog);
    }
    dialog.innerHTML = `<form id="tw-form"><h3>TW MTF · Pivots y Price Action</h3><p style="font-size:12px;margin:10px 0">Velas cerradas. Los pivots aparecen después de las velas de confirmación. DT/DP se confirman con cierre de la temporalidad pivot. Amarillo = candidato.<br>Desactivar zonas exige desactivar también «Exigir cercanía a zona» para recibir patrones.</p><div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">${Object.entries(TWPivots.fields).map(([key, f]) => {
        const v = twState.config[key];
        const input = typeof f[1] === 'boolean' ? `<input name="${key}" type="checkbox" ${v ? 'checked' : ''}>` : typeof f[1] === 'string' ? `<select name="${key}">${Object.keys(TWPivots.TF).map(tf => `<option ${tf === v ? 'selected' : ''}>${tf}</option>`).join('')}</select>` : `<input name="${key}" type="number" required min="${f[2]}" max="${f[3]}" step="${f[4]}" value="${v}" style="width:80px">`;
        return `<label style="font-size:12px;display:flex;justify-content:space-between;align-items:center;gap:8px">${f[0]} ${input}</label>`;
    }).join('')}</div><p id="tw-save-status" style="font-size:12px;color:#b91c1c"></p><div style="display:flex;justify-content:flex-end;gap:10px;margin-top:18px"><button type="button" id="tw-cancel">Cerrar</button><button type="submit">Aplicar y guardar</button></div></form>`;
    dialog.querySelector('#tw-cancel').onclick = () => dialog.close();
    dialog.querySelector('form').onsubmit = async event => {
        event.preventDefault();
        const values = {};
        for (const [key, f] of Object.entries(TWPivots.fields)) {
            const el = event.target.elements.namedItem(key);
            values[key] = typeof f[1] === 'boolean' ? el.checked : el.value;
        }
        twState.config = TWPivots.normalize(values);
        twStart();
        try { await guardarConfigServidor('configTW', twState.config); dialog.close(); }
        catch (_) { dialog.querySelector('#tw-save-status').textContent = 'Aplicado, pero no se pudo guardar en el servidor. Intentá nuevamente.'; }
    };
    dialog.showModal();
}
