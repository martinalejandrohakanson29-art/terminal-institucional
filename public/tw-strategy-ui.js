'use strict';
const twStrategyFields = Object.keys(TWPivots.fields).filter(k => !['showResistanceZones', 'showSupportZones', 'showEarlyDouble', 'showDoubleNeckline', 'confirmDoubleWithNeckline'].includes(k));
function initTWStrategy() {
    document.getElementById('tw-strategy-fields').innerHTML = twStrategyFields.map(key => {
        const f = TWPivots.fields[key], id = `tw-param-${key}`;
        const input = typeof f[1] === 'boolean' ? `<input id="${id}" type="checkbox" class="form-check" ${f[1] ? 'checked' : ''}>`
            : typeof f[1] === 'string' ? `<select id="${id}" class="form-select">${Object.keys(TWPivots.TF).map(tf => `<option ${tf === f[1] ? 'selected' : ''}>${tf}</option>`).join('')}</select>`
            : `<input id="${id}" type="number" class="form-input" min="${f[2]}" max="${f[3]}" step="${f[4]}" value="${f[1]}">`;
        return `<div class="form-row"><label for="${id}">${f[0]}</label>${input}</div>`;
    }).join('');
    updateTWStrategyMode();
}
function updateTWStrategyMode() {
    const active = document.getElementById('p-strategy-type').value === 'tw_mtf';
    document.getElementById('tw-strategy-panel').style.display = active ? 'block' : 'none';
    const keep = ['Estrategias Guardadas', 'Motor de estrategia', 'TW MTF', 'General', 'Costos de Operación', 'Apalancamiento', 'Operaciones', 'Tamaño de Posición'];
    for (const section of document.querySelectorAll('#form-panel > .form-section')) {
        const title = section.querySelector('h3')?.textContent.trim();
        if (keep.includes(title)) continue;
        if (active) {
            if (!section.hasAttribute('data-tw-display')) section.setAttribute('data-tw-display', section.style.display);
            section.style.setProperty('display', 'none', 'important');
        } else if (section.hasAttribute('data-tw-display')) {
            section.style.display = section.getAttribute('data-tw-display'); section.removeAttribute('data-tw-display');
        }
    }
    document.getElementById('classic-entry-options').style.display = active ? 'none' : '';
}
function readTWStrategy() {
    const config = {};
    for (const key of twStrategyFields) {
        const el = document.getElementById(`tw-param-${key}`);
        config[key] = typeof TWPivots.defaults[key] === 'boolean' ? el.checked : el.value;
    }
    const number = (id, fallback) => { const v = document.getElementById(id).value; return v !== '' && Number.isFinite(+v) ? +v : fallback; };
    return { strategyType: document.getElementById('p-strategy-type').value, twMode: document.getElementById('p-tw-mode').value,
        twConfig: TWPivots.normalize(config), twRiskMode: document.getElementById('p-tw-risk-mode').value,
        twRewardRisk: number('p-tw-rr', 2), twTakeProfitPerc: number('p-tw-tp-perc', 1), twStopLossPerc: number('p-tw-sl-perc', 1), twStopBufferPerc: number('p-tw-buffer', .1),
        twMinStopPerc: number('p-tw-min-stop', .05), twMaxStopPerc: number('p-tw-max-stop', 5),
        twMaxHoldMinutes: number('p-tw-max-hold', 0), twValidationPct: number('p-tw-validation', 30),
        useStagedProtection: document.getElementById('p-tw-staged').checked,
        stagedLevels: [1,2,3].map(k => ({ on: document.getElementById(`p-tw-staged-${k}-on`).checked,
            trig: number(`p-tw-staged-${k}-trig`, k === 1 ? 1 : k === 2 ? 1.5 : 2),
            stop: number(`p-tw-staged-${k}-stop`, k === 1 ? .5 : k === 2 ? 1 : 1.5) })) };
}
function restoreTWStrategy(p) {
    document.getElementById('p-strategy-type').value = p.strategyType === 'tw_mtf' ? 'tw_mtf' : 'classic';
    document.getElementById('p-tw-mode').value = ['both','double','rejection'].includes(p.twMode) ? p.twMode : 'rejection';
    const config = TWPivots.normalize(p.twConfig);
    for (const key of twStrategyFields) {
        const el = document.getElementById(`tw-param-${key}`);
        if (typeof TWPivots.defaults[key] === 'boolean') el.checked = config[key]; else el.value = config[key];
    }
    for (const [id, key, def] of [['rr','twRewardRisk',2], ['buffer','twStopBufferPerc',.1], ['min-stop','twMinStopPerc',.05], ['max-stop','twMaxStopPerc',5], ['max-hold','twMaxHoldMinutes',0], ['validation','twValidationPct',30]]) document.getElementById(`p-tw-${id}`).value = p[key] ?? def;
    document.getElementById('p-tw-risk-mode').value = p.twRiskMode === 'manual' ? 'manual' : 'structural';
    document.getElementById('p-tw-tp-perc').value = p.twTakeProfitPerc ?? 1;
    document.getElementById('p-tw-sl-perc').value = p.twStopLossPerc ?? 1;
    document.getElementById('p-tw-staged').checked = p.useStagedProtection === true;
    const levels = Array.isArray(p.stagedLevels) ? p.stagedLevels : [];
    [1,2,3].forEach(k => {
        const lv = levels[k - 1] || {}, defs = [[1,.5],[1.5,1],[2,1.5]][k - 1];
        document.getElementById(`p-tw-staged-${k}-on`).checked = lv.on !== false;
        document.getElementById(`p-tw-staged-${k}-trig`).value = lv.trig ?? defs[0];
        document.getElementById(`p-tw-staged-${k}-stop`).value = lv.stop ?? defs[1];
    });
    updateTWRiskMode(); toggleTWStaged();
    updateTWStrategyMode();
}
function renderTWSummary(tw) {
    if (!tw) return '';
    const names = { rejection_Long: 'Rechazo · Long', rejection_Short: 'Rechazo · Short', double_Long: 'Doble piso · Long', double_Short: 'Doble techo · Short' };
    const row = (name, s) => `<tr><td>${name}</td><td>${s.totalTrades}</td><td>${s.winRate.toFixed(1)}%</td><td>${fUSD(s.netProfit)}</td><td>${s.profitFactor == null ? '∞' : s.profitFactor.toFixed(2)}</td></tr>`;
    return `<div class="table-box" style="padding:12px;margin-bottom:14px"><h3>TW MTF · desglose de todas las operaciones</h3>
        <table class="trades-table"><thead><tr><th>Modalidad / dirección</th><th>Trades</th><th>Aciertos</th><th>Neto</th><th>PF</th></tr></thead><tbody>${Object.entries(tw.breakdown).map(([key,s]) => row(names[key],s)).join('')}</tbody></table>
        ${tw.validationPct > 0 ? `<h4 style="margin-top:12px">Comparación temporal · corte ${fTs(tw.splitTs)} ARG</h4><table class="trades-table"><tbody>${row(`Primer ${100-tw.validationPct}%`,tw.samples.development)}${row(`Último ${tw.validationPct}% · validación`,tw.samples.validation)}</tbody></table><p style="font-size:11px;color:#64748b">Mismos parámetros, capital continuo y operaciones asignadas por fecha de entrada. No son dos simulaciones independientes. Reservá el tramo final sin ajustar parámetros sobre sus resultados.</p>` : ''}
        <p style="font-size:11px;color:#64748b">${escHtml(tw.assumptions)}</p>
        <p style="font-size:11px;color:#64748b">${tw.pivotBars} velas pivot completas · ${tw.patternBars} velas patrón · ${tw.evaluatedMinutes} minutos evaluados · ${tw.signalCount} señales elegibles.<br>Descartes: riesgo ${tw.rejected.risk}, conflicto ${tw.rejected.conflict}, capital ${tw.rejected.capital}, posición ocupada ${tw.rejected.occupied}, señal vencida ${tw.rejected.stale}.</p></div>`;
}
