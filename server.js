require('dotenv').config();
const express = require('express');
const path = require('path');
const { Pool } = require('pg');
const WebSocket = require('ws');
const crypto = require('crypto');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const cookieParser = require('cookie-parser');

const app = express();
app.use(express.json());
app.use(cookieParser());

const JWT_SECRET = process.env.JWT_SECRET || 'cambiar_este_secreto_en_produccion';

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

let limiteGuardadoBD = 1.0;

async function inicializarBaseDeDatos() {
    const queryTablaBallenas = `
        CREATE TABLE IF NOT EXISTS ballenas (
            id SERIAL PRIMARY KEY,
            fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            precio NUMERIC NOT NULL,
            cantidad NUMERIC NOT NULL,
            es_venta BOOLEAN NOT NULL
        );
    `;
    const queryTablaConfig = `
        CREATE TABLE IF NOT EXISTS configuracion (
            clave VARCHAR(50) PRIMARY KEY,
            valor NUMERIC NOT NULL
        );
    `;
    const queryTablaOI = `
        CREATE TABLE IF NOT EXISTS open_interest (
            tiempo BIGINT PRIMARY KEY,
            valor NUMERIC NOT NULL
        );
    `;
    const queryTablaUsuarios = `
        CREATE TABLE IF NOT EXISTS usuarios (
            id SERIAL PRIMARY KEY,
            username VARCHAR(50) UNIQUE NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            rol VARCHAR(20) DEFAULT 'user' CHECK (rol IN ('user', 'admin')),
            activo BOOLEAN DEFAULT true,
            creado_en TIMESTAMP DEFAULT NOW()
        );
    `;
    const queryTablaConfigUsuario = `
        CREATE TABLE IF NOT EXISTS configs_usuario (
            id SERIAL PRIMARY KEY,
            usuario_id INTEGER REFERENCES usuarios(id) ON DELETE CASCADE,
            clave VARCHAR(100) NOT NULL,
            valor JSONB,
            actualizado_en TIMESTAMP DEFAULT NOW(),
            UNIQUE(usuario_id, clave)
        );
    `;
    const queryTablaEstrategias = `
        CREATE TABLE IF NOT EXISTS estrategias_guardadas (
            id SERIAL PRIMARY KEY,
            usuario_id INTEGER REFERENCES usuarios(id) ON DELETE CASCADE,
            nombre VARCHAR(100) NOT NULL,
            params JSONB NOT NULL,
            actualizado_en TIMESTAMP DEFAULT NOW(),
            UNIQUE(usuario_id, nombre)
        );
    `;
    const queryTablaAutoTrading = `
        CREATE TABLE IF NOT EXISTS auto_trading_config (
            id INTEGER PRIMARY KEY DEFAULT 1,
            habilitado BOOLEAN DEFAULT false,
            estrategia_nombre VARCHAR(100),
            position_usdt NUMERIC DEFAULT 100,
            ultima_senal VARCHAR(10),
            ultima_senal_ts BIGINT,
            posicion_lado VARCHAR(10),
            posicion_qty NUMERIC,
            posicion_entry NUMERIC,
            posicion_tp NUMERIC,
            posicion_sl NUMERIC,
            CONSTRAINT solo_una_fila CHECK (id = 1)
        );
        INSERT INTO auto_trading_config (id) VALUES (1) ON CONFLICT DO NOTHING;
    `;

    try {
        await pool.query(queryTablaBallenas);
        await pool.query(queryTablaConfig);
        await pool.query(queryTablaOI);
        await pool.query(queryTablaUsuarios);
        await pool.query(queryTablaConfigUsuario);
        await pool.query(queryTablaEstrategias);
        await pool.query(queryTablaAutoTrading);

        await pool.query(`
            DO $$ BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'open_interest'
                      AND column_name = 'tiempo'
                      AND data_type = 'integer'
                ) THEN
                    ALTER TABLE open_interest ALTER COLUMN tiempo TYPE BIGINT;
                END IF;
            END $$;
        `);

        await pool.query(`CREATE INDEX IF NOT EXISTS idx_ballenas_fecha ON ballenas(fecha DESC)`);

        // Migración: columnas de posición activa en auto_trading_config
        await pool.query(`
            ALTER TABLE auto_trading_config
                ADD COLUMN IF NOT EXISTS posicion_lado   VARCHAR(10),
                ADD COLUMN IF NOT EXISTS posicion_qty    NUMERIC,
                ADD COLUMN IF NOT EXISTS posicion_entry  NUMERIC,
                ADD COLUMN IF NOT EXISTS posicion_tp     NUMERIC,
                ADD COLUMN IF NOT EXISTS posicion_sl     NUMERIC
        `);
        await pool.query(`INSERT INTO configuracion (clave, valor) VALUES ('limite_bd', 1.0) ON CONFLICT (clave) DO NOTHING`);

        const configRes = await pool.query(`SELECT valor FROM configuracion WHERE clave = 'limite_bd'`);
        if (configRes.rows.length > 0) {
            limiteGuardadoBD = parseFloat(configRes.rows[0].valor);
            console.log(`🔧 Límite de guardado cargado: > ${limiteGuardadoBD} BTC`);
        }

        // Crear admin por defecto si no hay usuarios
        const countRes = await pool.query(`SELECT COUNT(*) FROM usuarios`);
        if (parseInt(countRes.rows[0].count) === 0) {
            const hash = await bcrypt.hash('admin123', 12);
            await pool.query(
                `INSERT INTO usuarios (username, email, password_hash, rol) VALUES ($1, $2, $3, $4)`,
                ['admin', 'admin@terminal.local', hash, 'admin']
            );
            console.log('👤 Usuario admin creado: admin / admin123  ← ¡Cambiar la contraseña!');
        }

        console.log('✅ Base de datos lista y conectada.');
    } catch (error) {
        console.error('❌ Error al inicializar la BD:', error);
    }
}
inicializarBaseDeDatos();

// --- RECOLECTOR DE OPEN INTEREST ---
async function guardarOpenInterest() {
    try {
        const respuesta = await fetch('https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT');
        const datos = await respuesta.json();
        if (datos && datos.openInterest) {
            const valor = parseFloat(datos.openInterest);
            const tiempoVelaActual = Math.floor(Date.now() / 60000) * 60;
            await pool.query(
                `INSERT INTO open_interest (tiempo, valor) VALUES ($1, $2)
                 ON CONFLICT (tiempo) DO UPDATE SET valor = EXCLUDED.valor`,
                [tiempoVelaActual, valor]
            );
        }
    } catch (error) {
        console.error('Error al guardar Open Interest:', error);
    }
}
setInterval(guardarOpenInterest, 60000);
guardarOpenInterest();

// --- CAZADOR DE BALLENAS ---
let wsConectando = false;

function iniciarRastreadorBallenas() {
    if (wsConectando) return;
    wsConectando = true;

    const ws = new WebSocket('wss://stream.binance.com:9443/ws/btcusdt@aggTrade');

    ws.on('open', () => {
        wsConectando = false;
        console.log('✅ Conectado a la Cinta de Binance.');
    });

    ws.on('message', async (data) => {
        try {
            const evento = JSON.parse(data);
            const cantidad = parseFloat(evento.q);
            const precio = parseFloat(evento.p);
            const es_venta = evento.m;
            if (cantidad >= limiteGuardadoBD) {
                await pool.query(
                    `INSERT INTO ballenas (precio, cantidad, es_venta) VALUES ($1, $2, $3)`,
                    [precio, cantidad, es_venta]
                );
            }
        } catch (error) {
            console.error('Error al guardar trade:', error);
        }
    });

    ws.on('error', (err) => {
        console.error('Error en WebSocket ballenas:', err.message);
        ws.terminate();
    });

    ws.on('close', () => {
        wsConectando = false;
        console.log('⚠️ Reconectando en 5 segundos...');
        setTimeout(iniciarRastreadorBallenas, 5000);
    });
}
iniciarRastreadorBallenas();


// ============================================================
// MIDDLEWARE DE AUTENTICACIÓN
// ============================================================

function autenticar(req, res, next) {
    const token = req.cookies.token;
    if (!token) return res.status(401).json({ error: 'No autenticado' });
    try {
        req.usuario = jwt.verify(token, JWT_SECRET);
        next();
    } catch {
        res.clearCookie('token');
        res.status(401).json({ error: 'Token inválido o expirado' });
    }
}

function soloAdmin(req, res, next) {
    if (req.usuario.rol !== 'admin') return res.status(403).json({ error: 'Sin permisos' });
    next();
}


// ============================================================
// RUTAS DE AUTENTICACIÓN
// ============================================================

app.post('/api/auth/login', async (req, res) => {
    const { username, password, recordar } = req.body;
    if (!username || !password) return res.status(400).json({ error: 'Datos incompletos' });

    try {
        const result = await pool.query(
            `SELECT id, username, email, password_hash, rol, activo FROM usuarios WHERE username = $1`,
            [username.trim().toLowerCase()]
        );
        const user = result.rows[0];
        if (!user || !user.activo) return res.status(401).json({ error: 'Credenciales incorrectas' });

        const ok = await bcrypt.compare(password, user.password_hash);
        if (!ok) return res.status(401).json({ error: 'Credenciales incorrectas' });

        const payload = { id: user.id, username: user.username, email: user.email, rol: user.rol };
        const expiresIn = recordar ? '30d' : '24h';
        const token = jwt.sign(payload, JWT_SECRET, { expiresIn });

        const cookieOpts = {
            httpOnly: true,
            sameSite: 'strict',
            path: '/',
        };
        if (recordar) {
            cookieOpts.maxAge = 30 * 24 * 60 * 60 * 1000; // 30 días
        }

        res.cookie('token', token, cookieOpts);
        res.json({ ok: true, usuario: payload });
    } catch (error) {
        console.error('Error en login:', error);
        res.status(500).json({ error: 'Error interno' });
    }
});

app.post('/api/auth/logout', (req, res) => {
    res.clearCookie('token', { path: '/' });
    res.json({ ok: true });
});

app.get('/api/auth/me', autenticar, (req, res) => {
    res.json({ usuario: req.usuario });
});

app.post('/api/auth/cambiar-password', autenticar, async (req, res) => {
    const { passwordActual, passwordNueva } = req.body;
    if (!passwordActual || !passwordNueva || passwordNueva.length < 6)
        return res.status(400).json({ error: 'Contraseña nueva debe tener al menos 6 caracteres' });

    try {
        const result = await pool.query(`SELECT password_hash FROM usuarios WHERE id = $1`, [req.usuario.id]);
        const ok = await bcrypt.compare(passwordActual, result.rows[0].password_hash);
        if (!ok) return res.status(401).json({ error: 'Contraseña actual incorrecta' });

        const hash = await bcrypt.hash(passwordNueva, 12);
        await pool.query(`UPDATE usuarios SET password_hash = $1 WHERE id = $2`, [hash, req.usuario.id]);
        res.json({ ok: true });
    } catch (error) {
        res.status(500).json({ error: 'Error interno' });
    }
});


// ============================================================
// RUTAS DE CONFIGURACIÓN POR USUARIO
// ============================================================

app.get('/api/config', autenticar, async (req, res) => {
    try {
        const result = await pool.query(
            `SELECT clave, valor FROM configs_usuario WHERE usuario_id = $1`,
            [req.usuario.id]
        );
        const config = {};
        result.rows.forEach(r => { config[r.clave] = r.valor; });
        res.json(config);
    } catch (error) {
        res.status(500).json({ error: 'Error al obtener configuración' });
    }
});

app.put('/api/config', autenticar, async (req, res) => {
    const { clave, valor } = req.body;
    if (!clave) return res.status(400).json({ error: 'Falta clave' });

    try {
        await pool.query(
            `INSERT INTO configs_usuario (usuario_id, clave, valor, actualizado_en)
             VALUES ($1, $2, $3, NOW())
             ON CONFLICT (usuario_id, clave) DO UPDATE SET valor = EXCLUDED.valor, actualizado_en = NOW()`,
            [req.usuario.id, clave, JSON.stringify(valor)]
        );
        res.json({ ok: true });
    } catch (error) {
        res.status(500).json({ error: 'Error al guardar configuración' });
    }
});


// ============================================================
// RUTAS DE ADMINISTRACIÓN (solo admin)
// ============================================================

app.get('/api/admin/usuarios', autenticar, soloAdmin, async (req, res) => {
    try {
        const result = await pool.query(
            `SELECT id, username, email, rol, activo, creado_en FROM usuarios ORDER BY creado_en ASC`
        );
        res.json(result.rows);
    } catch (error) {
        res.status(500).json({ error: 'Error interno' });
    }
});

app.post('/api/admin/usuarios', autenticar, soloAdmin, async (req, res) => {
    const { username, email, password, rol } = req.body;
    if (!username || !email || !password || password.length < 6)
        return res.status(400).json({ error: 'Datos incompletos o contraseña muy corta (mín. 6 caracteres)' });

    try {
        const hash = await bcrypt.hash(password, 12);
        const rolFinal = rol === 'admin' ? 'admin' : 'user';
        const result = await pool.query(
            `INSERT INTO usuarios (username, email, password_hash, rol) VALUES ($1, $2, $3, $4) RETURNING id, username, email, rol, activo, creado_en`,
            [username.trim().toLowerCase(), email.trim().toLowerCase(), hash, rolFinal]
        );
        res.status(201).json(result.rows[0]);
    } catch (error) {
        if (error.code === '23505') return res.status(409).json({ error: 'El usuario o email ya existe' });
        res.status(500).json({ error: 'Error interno' });
    }
});

app.put('/api/admin/usuarios/:id', autenticar, soloAdmin, async (req, res) => {
    const { id } = req.params;
    const { email, rol, activo, password } = req.body;

    try {
        if (password) {
            if (password.length < 6) return res.status(400).json({ error: 'Contraseña muy corta (mín. 6 caracteres)' });
            const hash = await bcrypt.hash(password, 12);
            await pool.query(`UPDATE usuarios SET password_hash = $1 WHERE id = $2`, [hash, id]);
        }

        const campos = [], vals = [];
        if (email !== undefined) { campos.push(`email = $${campos.length + 1}`); vals.push(email.trim().toLowerCase()); }
        if (rol !== undefined) { campos.push(`rol = $${campos.length + 1}`); vals.push(rol === 'admin' ? 'admin' : 'user'); }
        if (activo !== undefined) { campos.push(`activo = $${campos.length + 1}`); vals.push(!!activo); }

        if (campos.length > 0) {
            vals.push(id);
            await pool.query(`UPDATE usuarios SET ${campos.join(', ')} WHERE id = $${vals.length}`, vals);
        }

        const result = await pool.query(
            `SELECT id, username, email, rol, activo, creado_en FROM usuarios WHERE id = $1`, [id]
        );
        res.json(result.rows[0]);
    } catch (error) {
        res.status(500).json({ error: 'Error interno' });
    }
});

app.delete('/api/admin/usuarios/:id', autenticar, soloAdmin, async (req, res) => {
    const { id } = req.params;
    if (parseInt(id) === req.usuario.id) return res.status(400).json({ error: 'No puedes eliminarte a ti mismo' });

    try {
        await pool.query(`DELETE FROM usuarios WHERE id = $1`, [id]);
        res.json({ ok: true });
    } catch (error) {
        res.status(500).json({ error: 'Error interno' });
    }
});


// ============================================================
// RUTAS DE LA API (datos de mercado — protegidas)
// ============================================================

app.get('/api/ballenas', autenticar, async (req, res) => {
    try {
        const query = `
            SELECT precio, cantidad, es_venta, EXTRACT(EPOCH FROM fecha) as tiempo_segundos
            FROM ballenas
            ORDER BY fecha DESC
            LIMIT 100000
        `;
        const resultado = await pool.query(query);
        res.json(resultado.rows);
    } catch (error) {
        res.status(500).json({ error: 'Error interno del servidor' });
    }
});

app.get('/api/open-interest', autenticar, async (req, res) => {
    try {
        const query = `
            SELECT tiempo, valor FROM (
                SELECT tiempo, valor FROM open_interest ORDER BY tiempo DESC LIMIT 10000
            ) t ORDER BY tiempo ASC
        `;
        const resultado = await pool.query(query);
        res.json(resultado.rows);
    } catch (error) {
        res.status(500).json({ error: 'Error interno obteniendo OI' });
    }
});

app.get('/api/oi-live', autenticar, async (req, res) => {
    try {
        const respuesta = await fetch('https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT');
        const datos = await respuesta.json();
        res.json(datos);
    } catch (error) {
        res.status(500).json({ error: 'Error obteniendo OI en vivo' });
    }
});

app.get('/api/filtro-bd', autenticar, (req, res) => {
    res.json({ umbral: limiteGuardadoBD });
});

app.post('/api/filtro-bd', autenticar, async (req, res) => {
    const nuevoUmbral = parseFloat(req.body.umbral);
    if (!isNaN(nuevoUmbral) && nuevoUmbral > 0) {
        limiteGuardadoBD = nuevoUmbral;
        try {
            await pool.query(`UPDATE configuracion SET valor = $1 WHERE clave = 'limite_bd'`, [limiteGuardadoBD]);
            console.log(`🔧 Filtro BD actualizado: > ${limiteGuardadoBD} BTC`);
            res.json({ status: 'ok', umbral: limiteGuardadoBD });
        } catch (error) {
            console.error('Error guardando configuración:', error);
            res.status(500).json({ error: 'No se pudo guardar la configuración en BD' });
        }
    } else {
        res.status(400).json({ error: 'Número inválido' });
    }
});


app.get('/api/whale-histogram', autenticar, async (req, res) => {
    const horas   = Math.min(Math.max(parseInt(req.query.horas)  || 8,  1), 168);
    const bucket  = [50, 100, 200].includes(parseInt(req.query.bucket)) ? parseInt(req.query.bucket) : 100;
    try {
        const result = await pool.query(`
            SELECT
                (FLOOR(precio::numeric / $2) * $2)::bigint AS nivel,
                ROUND(SUM(CASE WHEN es_venta = false THEN cantidad ELSE 0 END)::numeric, 2) AS compras,
                ROUND(SUM(CASE WHEN es_venta = true  THEN cantidad ELSE 0 END)::numeric, 2) AS ventas
            FROM ballenas
            WHERE fecha >= NOW() - make_interval(hours => $1)
            GROUP BY nivel
            HAVING SUM(cantidad) > 0
            ORDER BY nivel DESC
        `, [horas, bucket]);
        res.json(result.rows);
    } catch (error) {
        console.error('Error whale-histogram:', error);
        res.status(500).json({ error: 'Error interno' });
    }
});


// ============================================================
// MOTOR DE BACKTEST
// ============================================================

function calcEMA(values, period) {
    const k = 2 / (period + 1);
    const result = new Array(values.length).fill(null);
    if (values.length < period) return result;
    result[period - 1] = values.slice(0, period).reduce((s, v) => s + v, 0) / period;
    for (let i = period; i < values.length; i++) {
        result[i] = values[i] * k + result[i - 1] * (1 - k);
    }
    return result;
}

function calcRSI14(closes) {
    const p = 14;
    const result = new Array(closes.length).fill(null);
    if (closes.length <= p) return result;
    let gSum = 0, lSum = 0;
    for (let i = 1; i <= p; i++) {
        const d = closes[i] - closes[i - 1];
        if (d > 0) gSum += d; else lSum -= d;
    }
    let avgG = gSum / p, avgL = lSum / p;
    result[p] = avgL === 0 ? 100 : 100 - 100 / (1 + avgG / avgL);
    for (let i = p + 1; i < closes.length; i++) {
        const d = closes[i] - closes[i - 1];
        avgG = (avgG * 13 + (d > 0 ? d : 0)) / 14;
        avgL = (avgL * 13 + (d < 0 ? -d : 0)) / 14;
        result[i] = avgL === 0 ? 100 : 100 - 100 / (1 + avgG / avgL);
    }
    return result;
}

function calcMACDArr(closes, fast = 12, slow = 26, signal = 9) {
    const emaF = calcEMA(closes, fast);
    const emaS = calcEMA(closes, slow);
    const macdLine = closes.map((_, i) =>
        emaF[i] !== null && emaS[i] !== null ? emaF[i] - emaS[i] : null
    );
    const signalLine = new Array(closes.length).fill(null);
    const firstIdx = macdLine.findIndex(v => v !== null);
    if (firstIdx < 0) return { macd: macdLine, signal: signalLine };
    const sigEMA = calcEMA(macdLine.slice(firstIdx), signal);
    sigEMA.forEach((v, i) => { signalLine[firstIdx + i] = v; });
    return { macd: macdLine, signal: signalLine };
}

async function fetchKlinesBatch(interval, totalBars) {
    const perReq = 1000;
    const CHUNK = 10; // requests en paralelo por tanda
    const dur = { '1m': 60000, '5m': 300000, '15m': 900000 }[interval] || 60000;
    const n = Math.ceil(totalBars / perReq);
    const now = Date.now();

    const urls = Array.from({ length: n }, (_, i) =>
        `https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=${interval}&limit=${perReq}${i > 0 ? `&endTime=${now - i * perReq * dur}` : ''}`
    );

    const results = [];
    for (let i = 0; i < urls.length; i += CHUNK) {
        const batch = urls.slice(i, i + CHUNK).map(url =>
            fetch(url).then(r => r.json()).catch(() => [])
        );
        const batchResults = await Promise.all(batch);
        results.push(...batchResults);
    }

    const seen = new Set();
    const all = [];
    results.flat().forEach(v => {
        if (Array.isArray(v) && !seen.has(v[0])) { seen.add(v[0]); all.push(v); }
    });
    return all.sort((a, b) => a[0] - b[0]);
}

function lookupHTF(sortedTs, byTs, target) {
    let lo = 0, hi = sortedTs.length - 1, found = -1;
    while (lo <= hi) {
        const mid = (lo + hi) >> 1;
        if (sortedTs[mid] <= target) { found = mid; lo = mid + 1; } else hi = mid - 1;
    }
    return found >= 0 ? byTs.get(sortedTs[found]) : null;
}

function runBacktest(bars1m, bars5m, bars15m, whalesArr, p) {
    const c1m = bars1m.map(b => parseFloat(b[4]));
    const e50 = calcEMA(c1m, 50), e100 = calcEMA(c1m, 100),
          e200 = calcEMA(c1m, 200), e500 = calcEMA(c1m, 500);

    const c15m = bars15m.map(b => parseFloat(b[4]));
    const rsiArr = calcRSI14(c15m);
    const rsi15mByTs = new Map(bars15m.map((b, i) => [parseInt(b[0]), rsiArr[i]]));
    const ts15m = bars15m.map(b => parseInt(b[0])).sort((a, b) => a - b);

    const c5m = bars5m.map(b => parseFloat(b[4]));
    const { macd: macdArr, signal: sigArr } = calcMACDArr(c5m);
    const macd5mByTs = new Map(bars5m.map((b, i) => [parseInt(b[0]), { macd: macdArr[i], signal: sigArr[i] }]));
    const ts5m = bars5m.map(b => parseInt(b[0])).sort((a, b) => a - b);

    // Delta de volumen — prefix sum para rolling sum O(1) por barra
    const deltaPfx = new Array(bars1m.length + 1).fill(0);
    for (let i = 0; i < bars1m.length; i++) {
        const bv = parseFloat(bars1m[i][9]); // takerBuyBaseAssetVolume
        const tv = parseFloat(bars1m[i][5]); // total volume
        deltaPfx[i + 1] = deltaPfx[i] + (bv - (tv - bv));
    }

    // Ballenas — ordenadas por tiempo para sliding window O(n)
    const whaleTrades = (whalesArr || [])
        .map(w => ({ ts: parseFloat(w.ts_sec) * 1000, btc: parseFloat(w.cantidad), isSell: w.es_venta }))
        .sort((a, b) => a.ts - b.ts);
    const whaleWindowMs = p.whaleWindow * 60000;
    let wLeft = 0, wRight = -1, wBuys = 0, wSells = 0;

    let capital = p.initialCapital, position = 0, entryPrice = 0;
    let entryBarIdx = null, lastClosedBarIdx = null;
    const trades = [];
    const equity = [{ ts: parseInt(bars1m[0][0]), v: capital }];
    const WARMUP = 500;

    for (let i = WARMUP; i < bars1m.length; i++) {
        const bar = bars1m[i];
        const ts = parseInt(bar[0]);
        const high = parseFloat(bar[2]), low = parseFloat(bar[3]), close = parseFloat(bar[4]);
        const E50 = e50[i], E100 = e100[i], E200 = e200[i], E500 = e500[i];
        if (!E500) continue;

        const rsiRaw  = lookupHTF(ts15m, rsi15mByTs, ts);
        const macdRaw = lookupHTF(ts5m, macd5mByTs, ts);
        if (rsiRaw === null || rsiRaw === undefined || !macdRaw || macdRaw.macd === null || macdRaw.signal === null) continue;

        const rsi15 = rsiRaw;
        const { macd: macd5, signal: sig5 } = macdRaw;

        // SALIDAS
        if (position !== 0) {
            const barsIn  = i - entryBarIdx;
            const longTP  = entryPrice * (1 + p.tpPerc / 100);
            const shortTP = entryPrice * (1 - p.tpPerc / 100);
            const longSL  = entryPrice * (1 - p.slPerc / 100);
            const shortSL = entryPrice * (1 + p.slPerc / 100);
            let exitPrice = null, exitReason = null;

            if (position === 1) {
                if (p.stopType === 'Porcentaje') {
                    if      (high >= longTP && low <= longSL) { exitPrice = longSL; exitReason = 'SL'; }
                    else if (high >= longTP)                  { exitPrice = longTP; exitReason = 'TP'; }
                    else if (low  <= longSL)                  { exitPrice = longSL; exitReason = 'SL'; }
                } else {
                    if (high >= longTP) { exitPrice = longTP; exitReason = 'TP'; }
                    else { const se = p.stopType === 'Ruptura EMA 200' ? E200 : E500; if (close < se) { exitPrice = close; exitReason = 'EMA'; } }
                }
                if (!exitPrice && p.useMaxTradeTime && barsIn >= p.maxTradeMinutes) { exitPrice = close; exitReason = 'Tiempo'; }
            } else {
                if (p.stopType === 'Porcentaje') {
                    if      (low <= shortTP && high >= shortSL) { exitPrice = shortSL; exitReason = 'SL'; }
                    else if (low  <= shortTP)                   { exitPrice = shortTP; exitReason = 'TP'; }
                    else if (high >= shortSL)                   { exitPrice = shortSL; exitReason = 'SL'; }
                } else {
                    if (low <= shortTP) { exitPrice = shortTP; exitReason = 'TP'; }
                    else { const se = p.stopType === 'Ruptura EMA 200' ? E200 : E500; if (close > se) { exitPrice = close; exitReason = 'EMA'; } }
                }
                if (!exitPrice && p.useMaxTradeTime && barsIn >= p.maxTradeMinutes) { exitPrice = close; exitReason = 'Tiempo'; }
            }

            if (exitPrice) {
                const raw = position === 1 ? (exitPrice - entryPrice) / entryPrice : (entryPrice - exitPrice) / entryPrice;
                const net = raw - (p.commission / 100) * 2;
                const pnlAbs = capital * net;
                capital += pnlAbs;
                trades.push({ type: position === 1 ? 'Long' : 'Short', entryTs: parseInt(bars1m[entryBarIdx][0]), exitTs: ts, entryPrice, exitPrice, pnlPerc: net * 100, pnlAbs, reason: exitReason, capital });
                equity.push({ ts, v: capital });
                position = 0; lastClosedBarIdx = i; entryBarIdx = null;
            }
        }

        // Actualizar sliding window de ballenas (se hace siempre, no solo en entrada)
        while (wRight + 1 < whaleTrades.length && whaleTrades[wRight + 1].ts <= ts) {
            wRight++;
            if (whaleTrades[wRight].isSell) wSells += whaleTrades[wRight].btc; else wBuys += whaleTrades[wRight].btc;
        }
        while (wLeft <= wRight && whaleTrades[wLeft].ts < ts - whaleWindowMs) {
            if (whaleTrades[wLeft].isSell) wSells -= whaleTrades[wLeft].btc; else wBuys -= whaleTrades[wLeft].btc;
            wLeft++;
        }
        const whaleDelta = wBuys - wSells;

        // ENTRADAS
        if (position === 0) {
            const barHour = new Date(ts).getUTCHours();
            if (barHour < p.startHour || barHour >= p.endHour) continue;
            const barsSinceClose = lastClosedBarIdx !== null ? i - lastClosedBarIdx : 999999;
            if (p.useCooldown && barsSinceClose < p.cooldownMinutes) continue;

            const above = close > E50 && close > E100 && close > E200 && close > E500;
            const below = close < E50 && close < E100 && close < E200 && close < E500;
            const bullAlign = E50 > E100 && E100 > E200 && E200 > E500;
            const bearAlign = E50 < E100 && E100 < E200 && E200 < E500;
            const d50 = Math.abs(close - E50) / close * 100, d100 = Math.abs(close - E100) / close * 100,
                  d200 = Math.abs(close - E200) / close * 100, d500 = Math.abs(close - E500) / close * 100;
            const nearEMA = d50 <= p.pullbackPerc || d100 <= p.pullbackPerc || d200 <= p.pullbackPerc || d500 <= p.pullbackPerc;
            const pullOK     = !p.usePullbackFilter || nearEMA;
            const alignLong  = !p.useEmaAlignment || bullAlign;
            const alignShort = !p.useEmaAlignment || bearAlign;

            // Delta de volumen rolling (últimas N velas)
            const dStart       = Math.max(0, i - p.deltaVelas + 1);
            const deltaRolling = deltaPfx[i + 1] - deltaPfx[dStart];
            const deltaOkLong  = !p.useDeltaFilter || deltaRolling > 0;
            const deltaOkShort = !p.useDeltaFilter || deltaRolling < 0;

            // Ballenas: delta en ventana reciente
            const whaleOkLong  = !p.useWhaleFilter || whaleDelta > 0;
            const whaleOkShort = !p.useWhaleFilter || whaleDelta < 0;

            if      (p.enableLongs  && above && alignLong  && rsi15 >= 60 && macd5 > sig5 && pullOK && deltaOkLong  && whaleOkLong)  { position = 1;  entryPrice = close; entryBarIdx = i; }
            else if (p.enableShorts && below && alignShort && rsi15 <= 40 && macd5 < sig5 && pullOK && deltaOkShort && whaleOkShort) { position = -1; entryPrice = close; entryBarIdx = i; }
        }
    }

    const wins   = trades.filter(t => t.pnlPerc > 0);
    const losses = trades.filter(t => t.pnlPerc <= 0);
    const grossW = wins.reduce((s, t) => s + Math.abs(t.pnlAbs), 0);
    const grossL = losses.reduce((s, t) => s + Math.abs(t.pnlAbs), 0);
    let peak = p.initialCapital, maxDDPerc = 0;
    equity.forEach(pt => {
        if (pt.v > peak) peak = pt.v;
        const dd = (peak - pt.v) / peak * 100;
        if (dd > maxDDPerc) maxDDPerc = dd;
    });

    return {
        stats: {
            totalTrades: trades.length,
            winners: wins.length, losers: losses.length,
            winRate: trades.length > 0 ? wins.length / trades.length * 100 : 0,
            netProfit: capital - p.initialCapital,
            netProfitPerc: (capital - p.initialCapital) / p.initialCapital * 100,
            finalCapital: capital,
            profitFactor: grossL > 0 ? grossW / grossL : grossW > 0 ? Infinity : 0,
            maxDrawdownPerc: maxDDPerc,
            avgWinPerc:  wins.length   > 0 ? wins.reduce((s, t) => s + t.pnlPerc, 0)   / wins.length   : 0,
            avgLossPerc: losses.length > 0 ? Math.abs(losses.reduce((s, t) => s + t.pnlPerc, 0) / losses.length) : 0,
            longsCount:  trades.filter(t => t.type === 'Long').length,
            shortsCount: trades.filter(t => t.type === 'Short').length,
        },
        trades: trades.slice(-300),
        equity
    };
}

// ── Auto-Trading Loop ─────────────────────────────────────────
const N8N_WEBHOOK_URL    = process.env.N8N_WEBHOOK_URL;
const AUTO_POSITION_USDT = parseFloat(process.env.AUTO_POSITION_USDT) || 100;
const BINANCE_API_KEY    = process.env.Clave_API_Binance;
const BINANCE_SECRET     = process.env.Clave_secreta_Binance;
const BINANCE_BASE       = 'https://testnet.binancefuture.com';
const BINANCE_WS_PRECIO  = BINANCE_BASE.includes('testnet')
    ? 'wss://stream.binancefuture.com/ws/btcusdt@aggTrade'
    : 'wss://fstream.binance.com/ws/btcusdt@aggTrade';

// Estado de posición activa (memoria + BD)
let posicionActiva = null; // { lado, qty, entry, tp, sl }

async function guardarPosicionBD(pos) {
    await pool.query(
        `UPDATE auto_trading_config
         SET posicion_lado=$1, posicion_qty=$2, posicion_entry=$3, posicion_tp=$4, posicion_sl=$5
         WHERE id=1`,
        [pos.lado, pos.qty, pos.entry, pos.tp, pos.sl]
    );
}

async function limpiarPosicionBD() {
    await pool.query(
        `UPDATE auto_trading_config
         SET posicion_lado=NULL, posicion_qty=NULL, posicion_entry=NULL, posicion_tp=NULL, posicion_sl=NULL
         WHERE id=1`
    );
}

function buildCloseUrl(lado, qty) {
    const side = lado === 'long' ? 'SELL' : 'BUY';
    const ts   = Date.now();
    const p    = `symbol=BTCUSDT&side=${side}&type=MARKET&quantity=${qty}&timestamp=${ts}`;
    return `${BINANCE_BASE}/fapi/v1/order?${p}&signature=${binanceSign(p)}`;
}

async function chequearSalida(precio) {
    if (!posicionActiva) return;
    const { lado, qty, tp, sl } = posicionActiva;

    const golpeTP = lado === 'long' ? precio >= tp : precio <= tp;
    const golpeSL = lado === 'long' ? precio <= sl : precio >= sl;
    if (!golpeTP && !golpeSL) return;

    const razon = golpeTP ? 'TP' : 'SL';
    console.log(`[AutoTrading] ${razon} alcanzado @ $${precio.toFixed(1)} — cerrando posición`);

    // Limpiar antes de ejecutar para evitar doble cierre
    const pos = posicionActiva;
    posicionActiva = null;
    await limpiarPosicionBD();
    await pool.query(`UPDATE auto_trading_config SET ultima_senal = NULL WHERE id = 1`);

    await ejecutarOrdenBinance(buildCloseUrl(pos.lado, pos.qty), BINANCE_API_KEY, `CIERRE-${razon}`);
}

// WebSocket de precio futuros para monitoreo TP/SL en tiempo real
let wsPrecioConectando = false;

function iniciarMonitorPrecio() {
    if (wsPrecioConectando) return;
    wsPrecioConectando = true;

    const ws = new WebSocket(BINANCE_WS_PRECIO);

    ws.on('open', () => {
        wsPrecioConectando = false;
        console.log('✅ Monitor de precio futuros conectado.');
    });

    ws.on('message', async (data) => {
        try {
            const evento = JSON.parse(data);
            await chequearSalida(parseFloat(evento.p));
        } catch (e) {
            console.error('Error en monitor de precio:', e.message);
        }
    });

    ws.on('error', (err) => {
        console.error('Error WebSocket precio:', err.message);
        ws.terminate();
    });

    ws.on('close', () => {
        wsPrecioConectando = false;
        setTimeout(iniciarMonitorPrecio, 5000);
    });
}

// Log de estado al arrancar
setTimeout(() => {
    console.log(`[AutoTrading] N8N_WEBHOOK_URL: ${N8N_WEBHOOK_URL ? '✅ configurado' : '❌ falta'}`);
    console.log(`[AutoTrading] Clave_API_Binance: ${BINANCE_API_KEY ? '✅ configurada' : '❌ falta'}`);
    console.log(`[AutoTrading] Clave_secreta_Binance: ${BINANCE_SECRET ? '✅ configurada' : '❌ falta'}`);
}, 3000);

function binanceSign(params) {
    if (!BINANCE_SECRET) throw new Error('Clave_secreta_Binance no configurada en el entorno del servidor');
    return crypto.createHmac('sha256', BINANCE_SECRET).update(params).digest('hex');
}

async function ejecutarOrdenBinance(url, apiKey, etiqueta) {
    try {
        const resp = await fetch(url, {
            method: 'POST',
            headers: { 'X-MBX-APIKEY': apiKey, 'Content-Type': 'application/x-www-form-urlencoded' },
        });
        const body = await resp.json();
        if (resp.ok) {
            console.log(`[AutoTrading] ✅ Orden ${etiqueta} ejecutada — orderId: ${body.orderId}`);
        } else {
            console.error(`[AutoTrading] ❌ Orden ${etiqueta} rechazada — ${body.code}: ${body.msg}`);
        }
        return { ok: resp.ok, body };
    } catch (e) {
        console.error(`[AutoTrading] ❌ Error red orden ${etiqueta}: ${e.message}`);
        return { ok: false, error: e.message };
    }
}

function buildBinanceUrls(signal, entry, tp, sl, positionUsdt) {
    const side   = signal === 'long' ? 'BUY' : 'SELL';
    const tpSide = signal === 'long' ? 'SELL' : 'BUY';
    const qty    = Math.floor((positionUsdt / entry) * 1000) / 1000;
    const ts     = Date.now();

    const entryP = `symbol=BTCUSDT&side=${side}&type=MARKET&quantity=${qty}&timestamp=${ts}`;
    const tpP    = `symbol=BTCUSDT&side=${tpSide}&type=TAKE_PROFIT_MARKET&stopPrice=${tp.toFixed(1)}&closePosition=true&workingType=CONTRACT_PRICE&recvWindow=10000&timestamp=${ts + 1}`;
    const slP    = `symbol=BTCUSDT&side=${tpSide}&type=STOP_MARKET&stopPrice=${sl.toFixed(1)}&closePosition=true&workingType=CONTRACT_PRICE&recvWindow=10000&timestamp=${ts + 2}`;

    return {
        entryUrl: `${BINANCE_BASE}/fapi/v1/order?${entryP}&signature=${binanceSign(entryP)}`,
        tpUrl:    `${BINANCE_BASE}/fapi/v1/order?${tpP}&signature=${binanceSign(tpP)}`,
        slUrl:    `${BINANCE_BASE}/fapi/v1/order?${slP}&signature=${binanceSign(slP)}`,
        apiKey:   BINANCE_API_KEY,
        qty, side, entry, tp, sl,
    };
}

async function ejecutarAutoTrading() {
    if (!N8N_WEBHOOK_URL) return;
    if (posicionActiva) return; // ya hay posición abierta, el WS gestiona la salida
    try {
        const cfgRes = await pool.query('SELECT * FROM auto_trading_config WHERE id = 1');
        const cfg = cfgRes.rows[0];
        if (!cfg || !cfg.habilitado || !cfg.estrategia_nombre) return;

        const stratRes = await pool.query(
            `SELECT params FROM estrategias_guardadas WHERE nombre = $1 LIMIT 1`,
            [cfg.estrategia_nombre]
        );
        if (!stratRes.rows.length) return;
        const p = stratRes.rows[0].params;

        const [bars1m, bars5m, bars15m, whaleRes] = await Promise.all([
            fetchKlinesBatch('1m',  520),
            fetchKlinesBatch('5m',  50),
            fetchKlinesBatch('15m', 30),
            pool.query(
                `SELECT EXTRACT(EPOCH FROM fecha) as ts_sec, cantidad, es_venta
                 FROM ballenas WHERE fecha >= NOW() - INTERVAL '2 hours' AND cantidad >= $1 ORDER BY fecha ASC`,
                [p.whaleMinBTC || 5]
            ),
        ]);

        const resultado = evaluarSenal(bars1m, bars5m, bars15m, whaleRes.rows, p);
        const nuevaSenal = resultado.signal;

        // Solo disparar si la señal cambia (evita órdenes duplicadas)
        if (nuevaSenal === cfg.ultima_senal) return;

        await pool.query(
            `UPDATE auto_trading_config SET ultima_senal = $1, ultima_senal_ts = $2 WHERE id = 1`,
            [nuevaSenal, Date.now()]
        );

        if (!nuevaSenal) {
            console.log(`[AutoTrading] Señal cerrada — sin posición nueva`);
            return;
        }

        const positionUsdt = parseFloat(cfg.position_usdt) || AUTO_POSITION_USDT;
        const urls = buildBinanceUrls(nuevaSenal, resultado.entry, resultado.tp, resultado.sl, positionUsdt);

        console.log(`[AutoTrading] Nueva señal: ${nuevaSenal.toUpperCase()} @ $${resultado.entry} | TP $${resultado.tp?.toFixed(0)} | SL $${resultado.sl?.toFixed(0)} | qty ${urls.qty} BTC`);

        const ordenEntrada = await ejecutarOrdenBinance(urls.entryUrl, urls.apiKey, 'ENTRADA');
        if (ordenEntrada.ok) {
            posicionActiva = { lado: nuevaSenal, qty: urls.qty, entry: resultado.entry, tp: resultado.tp, sl: resultado.sl };
            await guardarPosicionBD(posicionActiva);
            console.log(`[AutoTrading] Posición guardada — TP $${resultado.tp?.toFixed(0)} | SL $${resultado.sl?.toFixed(0)}`);
        }

        // Notificar a n8n (opcional, solo logging)
        if (N8N_WEBHOOK_URL) {
            fetch(N8N_WEBHOOK_URL, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    signal: nuevaSenal, entry: resultado.entry,
                    tp: resultado.tp, sl: resultado.sl,
                    qty: urls.qty, estrategia: cfg.estrategia_nombre,
                    ordenOk: ordenEntrada.ok,
                }),
            }).catch(() => {});
        }

    } catch (e) {
        console.error('[AutoTrading] Error en loop:', e.message);
    }
}

// Arrancar loop después de que la BD esté lista
setTimeout(async () => {
    // Recuperar posición activa si el servidor se reinició con una posición abierta
    try {
        const r = await pool.query(
            'SELECT posicion_lado, posicion_qty, posicion_entry, posicion_tp, posicion_sl FROM auto_trading_config WHERE id = 1'
        );
        const pos = r.rows[0];
        if (pos && pos.posicion_lado) {
            posicionActiva = {
                lado:  pos.posicion_lado,
                qty:   parseFloat(pos.posicion_qty),
                entry: parseFloat(pos.posicion_entry),
                tp:    parseFloat(pos.posicion_tp),
                sl:    parseFloat(pos.posicion_sl),
            };
            console.log(`[AutoTrading] Posición activa recuperada: ${posicionActiva.lado.toUpperCase()} @ $${posicionActiva.entry} | TP $${posicionActiva.tp?.toFixed(0)} | SL $${posicionActiva.sl?.toFixed(0)}`);
        }
    } catch (e) {
        console.error('[AutoTrading] Error cargando posición desde BD:', e.message);
    }

    iniciarMonitorPrecio();
    ejecutarAutoTrading();
    setInterval(ejecutarAutoTrading, 60 * 1000);
}, 5000);

// ── Endpoints de Auto-Trading ─────────────────────────────────
app.get('/api/autotrading', autenticar, soloAdmin, async (req, res) => {
    try {
        const r = await pool.query('SELECT * FROM auto_trading_config WHERE id = 1');
        res.json(r.rows[0] || {});
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/autotrading', autenticar, soloAdmin, async (req, res) => {
    const { habilitado, estrategia_nombre, position_usdt } = req.body;
    try {
        await pool.query(
            `UPDATE auto_trading_config
             SET habilitado = COALESCE($1, habilitado),
                 estrategia_nombre = COALESCE($2, estrategia_nombre),
                 position_usdt = COALESCE($3, position_usdt)
             WHERE id = 1`,
            [
                habilitado !== undefined ? habilitado : null,
                estrategia_nombre !== undefined ? estrategia_nombre : null,
                position_usdt     !== undefined ? parseFloat(position_usdt) : null,
            ]
        );
        // Reset señal cuando se cambia config para re-evaluar
        if (estrategia_nombre !== undefined || habilitado === true) {
            await pool.query(`UPDATE auto_trading_config SET ultima_senal = NULL WHERE id = 1`);
        }
        res.json({ ok: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/autotrading/status', autenticar, async (req, res) => {
    try {
        const r = await pool.query(
            'SELECT habilitado, estrategia_nombre, ultima_senal, ultima_senal_ts, position_usdt FROM auto_trading_config WHERE id = 1'
        );
        res.json(r.rows[0] || { habilitado: false });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// Endpoint de test: ejecuta órdenes reales en Binance Testnet (solo admin)
app.post('/api/autotrading/test', autenticar, soloAdmin, async (req, res) => {
    const signal       = req.body.signal                    || 'long';
    const entry        = parseFloat(req.body.entry)         || 95000;
    const tp           = parseFloat(req.body.tp)            || 95475;
    const sl           = parseFloat(req.body.sl)            || 94050;
    const positionUsdt = parseFloat(req.body.positionUsdt)  || 100;
    try {
        const urls = buildBinanceUrls(signal, entry, tp, sl, positionUsdt);

        const resEntrada = await ejecutarOrdenBinance(urls.entryUrl, urls.apiKey, 'ENTRADA');
        if (resEntrada.ok) {
            posicionActiva = { lado: signal, qty: urls.qty, entry, tp, sl };
            await guardarPosicionBD(posicionActiva);
        }
        const resTp = { ok: false, body: { msg: 'gestionado por monitor WebSocket' } };
        const resSl = { ok: false, body: { msg: 'gestionado por monitor WebSocket' } };

        // Notificar n8n si está configurado
        if (N8N_WEBHOOK_URL) {
            fetch(N8N_WEBHOOK_URL, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ signal, entry, tp, sl, qty: urls.qty, estrategia: 'TEST', ordenOk: resEntrada.ok }),
            }).catch(() => {});
        }

        res.json({
            entrada: { ok: resEntrada.ok, orderId: resEntrada.body?.orderId, msg: resEntrada.body?.msg },
            tp:      { ok: resTp.ok,      orderId: resTp.body?.orderId,      msg: resTp.body?.msg },
            sl:      { ok: resSl.ok,      orderId: resSl.body?.orderId,      msg: resSl.body?.msg },
            qty: urls.qty, signal, entry, tp, sl,
        });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// ── Evaluación de señal en tiempo real ────────────────────────
function evaluarSenal(bars1m, bars5m, bars15m, whalesArr, p) {
    if (bars1m.length < 510) return { signal: null, reason: 'datos_insuficientes' };

    const c1m  = bars1m.map(b => parseFloat(b[4]));
    const e50  = calcEMA(c1m, 50);
    const e100 = calcEMA(c1m, 100);
    const e200 = calcEMA(c1m, 200);
    const e500 = calcEMA(c1m, 500);

    const c15m   = bars15m.map(b => parseFloat(b[4]));
    const rsiArr = calcRSI14(c15m);
    const rsi15mByTs = new Map(bars15m.map((b, i) => [parseInt(b[0]), rsiArr[i]]));
    const rsi15mTs   = [...rsi15mByTs.keys()].sort((a, b) => a - b);

    const { macd: macdArr, signal: sigArr } = calcMACDArr(bars5m.map(b => parseFloat(b[4])));
    const macd5mByTs = new Map(bars5m.map((b, i) => [parseInt(b[0]), { macd: macdArr[i], sig: sigArr[i] }]));
    const macd5mTs   = [...macd5mByTs.keys()].sort((a, b) => a - b);

    const i     = bars1m.length - 1;
    const bar   = bars1m[i];
    const ts    = parseInt(bar[0]);
    const close = parseFloat(bar[4]);

    const E50 = e50[i], E100 = e100[i], E200 = e200[i], E500 = e500[i];

    const rsiLookup  = lookupHTF(rsi15mTs, rsi15mByTs, ts);
    const macdLookup = lookupHTF(macd5mTs, macd5mByTs, ts);
    const rsi15 = rsiLookup  ?? 50;
    const macd5 = macdLookup?.macd ?? 0;
    const sig5  = macdLookup?.sig  ?? 0;

    const barHour  = new Date(ts).getUTCHours();
    const horarioOk = barHour >= (p.startHour ?? 9) && barHour < (p.endHour ?? 20);

    const above     = close > E50 && close > E100 && close > E200 && close > E500;
    const below     = close < E50 && close < E100 && close < E200 && close < E500;
    const bullAlign = E50 > E100 && E100 > E200 && E200 > E500;
    const bearAlign = E50 < E100 && E100 < E200 && E200 < E500;

    const nearEMA = !p.usePullbackFilter || [E50, E100, E200, E500].some(e =>
        Math.abs(close - e) / close * 100 <= (p.pullbackPerc ?? 0.2)
    );

    const deltaSlice   = bars1m.slice(-(p.deltaVelas ?? 3));
    const deltaRolling = deltaSlice.reduce((s, b) => {
        const totalVol = parseFloat(b[5]);
        const buyVol   = parseFloat(b[9]);
        return s + (2 * buyVol - totalVol);
    }, 0);
    const deltaOkLong  = !p.useDeltaFilter || deltaRolling > 0;
    const deltaOkShort = !p.useDeltaFilter || deltaRolling < 0;

    const nowMs    = Date.now();
    const windowMs = (p.whaleWindow ?? 30) * 60000;
    const whaleDelta = whalesArr
        .filter(w => parseFloat(w.ts_sec) * 1000 >= nowMs - windowMs)
        .reduce((s, w) => s + (w.es_venta ? -parseFloat(w.cantidad) : parseFloat(w.cantidad)), 0);
    const whaleOkLong  = !p.useWhaleFilter || whaleDelta > 0;
    const whaleOkShort = !p.useWhaleFilter || whaleDelta < 0;

    const alignLong  = !p.useEmaAlignment || bullAlign;
    const alignShort = !p.useEmaAlignment || bearAlign;

    let signal = null;
    if (horarioOk && above && alignLong && rsi15 >= 60 && macd5 > sig5 && nearEMA && deltaOkLong && whaleOkLong)
        signal = 'long';
    else if (horarioOk && below && alignShort && rsi15 <= 40 && macd5 < sig5 && nearEMA && deltaOkShort && whaleOkShort)
        signal = 'short';

    const tpPerc = p.tpPerc ?? 0.5;
    const slPerc = p.slPerc ?? 1.0;
    const stopType = p.stopType ?? 'Porcentaje';

    const tp = signal === 'long'  ? close * (1 + tpPerc / 100) :
               signal === 'short' ? close * (1 - tpPerc / 100) : null;

    let sl = null;
    if (signal) {
        if (stopType === 'Porcentaje')
            sl = signal === 'long' ? close * (1 - slPerc / 100) : close * (1 + slPerc / 100);
        else
            sl = stopType === 'Ruptura EMA 200' ? E200 : E500;
    }

    return {
        signal, timestamp: ts, entry: close, tp, sl,
        indicadores: { rsi15, macd5, horarioOk, above, below, nearEMA, deltaRolling, whaleDelta, E50, E100, E200, E500 }
    };
}

app.get('/api/estrategia/signal', autenticar, async (req, res) => {
    const { nombre } = req.query;
    if (!nombre) return res.status(400).json({ error: 'nombre requerido' });
    try {
        const stratRes = await pool.query(
            'SELECT params FROM estrategias_guardadas WHERE usuario_id = $1 AND nombre = $2',
            [req.usuario.id, nombre]
        );
        if (stratRes.rows.length === 0) return res.status(404).json({ error: 'Estrategia no encontrada' });
        const p = stratRes.rows[0].params;

        const [bars1m, bars5m, bars15m, whaleRes] = await Promise.all([
            fetchKlinesBatch('1m',  520),
            fetchKlinesBatch('5m',  50),
            fetchKlinesBatch('15m', 30),
            pool.query(
                `SELECT EXTRACT(EPOCH FROM fecha) as ts_sec, cantidad, es_venta
                 FROM ballenas WHERE fecha >= NOW() - INTERVAL '2 hours' AND cantidad >= $1 ORDER BY fecha ASC`,
                [p.whaleMinBTC || 5]
            ),
        ]);

        res.json(evaluarSenal(bars1m, bars5m, bars15m, whaleRes.rows, p));
    } catch (e) {
        console.error('Error signal:', e);
        res.status(500).json({ error: e.message });
    }
});

// ── Estrategias guardadas ──────────────────────────────────────
app.get('/api/estrategias', autenticar, async (req, res) => {
    try {
        const result = await pool.query(
            `SELECT nombre, params, actualizado_en FROM estrategias_guardadas
             WHERE usuario_id = $1 ORDER BY nombre ASC`,
            [req.usuario.id]
        );
        res.json(result.rows);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

app.post('/api/estrategias', autenticar, async (req, res) => {
    const { nombre, params } = req.body;
    if (!nombre || !nombre.trim()) return res.status(400).json({ error: 'Nombre requerido' });
    if (!params || typeof params !== 'object') return res.status(400).json({ error: 'Parámetros inválidos' });
    const nombreLimpio = nombre.trim().slice(0, 100);
    try {
        await pool.query(
            `INSERT INTO estrategias_guardadas (usuario_id, nombre, params, actualizado_en)
             VALUES ($1, $2, $3, NOW())
             ON CONFLICT (usuario_id, nombre) DO UPDATE SET params = EXCLUDED.params, actualizado_en = NOW()`,
            [req.usuario.id, nombreLimpio, params]
        );
        res.json({ ok: true });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

app.delete('/api/estrategias/:nombre', autenticar, async (req, res) => {
    try {
        const result = await pool.query(
            `DELETE FROM estrategias_guardadas WHERE usuario_id = $1 AND nombre = $2`,
            [req.usuario.id, req.params.nombre]
        );
        if (result.rowCount === 0) return res.status(404).json({ error: 'No encontrada' });
        res.json({ ok: true });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

app.post('/api/backtest', autenticar, async (req, res) => {
    try {
        const p = {
            enableLongs:       req.body.enableLongs !== false,
            enableShorts:      req.body.enableShorts !== false,
            tpPerc:            parseFloat(req.body.tpPerc)  || 0.5,
            stopType:          req.body.stopType || 'Porcentaje',
            slPerc:            parseFloat(req.body.slPerc)  || 1.0,
            startHour:         parseInt(req.body.startHour) ?? 9,
            endHour:           parseInt(req.body.endHour)   ?? 20,
            usePullbackFilter: req.body.usePullbackFilter !== false,
            pullbackPerc:      parseFloat(req.body.pullbackPerc) || 0.20,
            useEmaAlignment:   req.body.useEmaAlignment !== false,
            useMaxTradeTime:   req.body.useMaxTradeTime !== false,
            maxTradeMinutes:   parseInt(req.body.maxTradeMinutes) || 15,
            useCooldown:       req.body.useCooldown !== false,
            cooldownMinutes:   parseInt(req.body.cooldownMinutes) || 45,
            useDeltaFilter:    req.body.useDeltaFilter === true,
            deltaVelas:        parseInt(req.body.deltaVelas) || 3,
            useWhaleFilter:    req.body.useWhaleFilter === true,
            whaleWindow:       parseInt(req.body.whaleWindow) || 30,
            whaleMinBTC:       parseFloat(req.body.whaleMinBTC) || 5,
            commission:        0.04,
            initialCapital:    parseFloat(req.body.initialCapital) || 1000,
        };
        const days = Math.min(Math.max(parseInt(req.body.lookbackDays) || 7, 1), 60);
        const periodStart = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
        const [bars1m, bars5m, bars15m, whaleRes] = await Promise.all([
            fetchKlinesBatch('1m',  days * 1440),
            fetchKlinesBatch('5m',  days * 288),
            fetchKlinesBatch('15m', days * 96),
            pool.query(
                `SELECT EXTRACT(EPOCH FROM fecha) as ts_sec, cantidad, es_venta
                 FROM ballenas WHERE fecha >= $1 AND cantidad >= $2 ORDER BY fecha ASC`,
                [periodStart.toISOString(), p.whaleMinBTC]
            ),
        ]);
        res.json(runBacktest(bars1m, bars5m, bars15m, whaleRes.rows, p));
    } catch (err) {
        console.error('Error backtest:', err);
        res.status(500).json({ error: 'Error al ejecutar backtest' });
    }
});


// ============================================================
// RUTAS DE PÁGINAS (deben ir ANTES de express.static para que
// tomen prioridad sobre el auto-index de index.html en "/")
// ============================================================

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'login.html')));
app.get('/terminal', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));
app.get('/admin', (req, res) => res.sendFile(path.join(__dirname, 'public', 'admin.html')));
app.get('/estrategias', (req, res) => res.sendFile(path.join(__dirname, 'public', 'estrategias.html')));

// index: false evita que express.static sirva index.html automáticamente para "/"
app.use(express.static(path.join(__dirname, 'public'), { index: false }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`¡Terminal Institucional encendida en puerto ${PORT}!`));
