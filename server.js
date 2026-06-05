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

// ── Cifrado de secretos (claves API Binance de cada usuario) ──────────
// Las claves secretas de Binance NO se guardan en texto plano: se cifran con
// AES-256-GCM usando ENCRYPTION_KEY del entorno y se descifran solo en memoria
// al momento de firmar una request. Sin ENCRYPTION_KEY, no se permite guardarlas.
const ENCRYPTION_KEY = process.env.ENCRYPTION_KEY
    ? crypto.createHash('sha256').update(process.env.ENCRYPTION_KEY).digest() // 32 bytes
    : null;

function cifrarSecreto(textoPlano) {
    if (!ENCRYPTION_KEY) throw new Error('ENCRYPTION_KEY no configurada en el entorno — no se pueden guardar claves');
    const iv     = crypto.randomBytes(12);
    const cipher = crypto.createCipheriv('aes-256-gcm', ENCRYPTION_KEY, iv);
    const ct     = Buffer.concat([cipher.update(textoPlano, 'utf8'), cipher.final()]);
    const tag    = cipher.getAuthTag();
    // formato: iv:tag:ciphertext (todo base64)
    return `${iv.toString('base64')}:${tag.toString('base64')}:${ct.toString('base64')}`;
}

function descifrarSecreto(blob) {
    if (!ENCRYPTION_KEY) throw new Error('ENCRYPTION_KEY no configurada en el entorno');
    const [ivB64, tagB64, ctB64] = String(blob).split(':');
    const iv       = Buffer.from(ivB64,  'base64');
    const tag      = Buffer.from(tagB64, 'base64');
    const ct       = Buffer.from(ctB64,  'base64');
    const decipher = crypto.createDecipheriv('aes-256-gcm', ENCRYPTION_KEY, iv);
    decipher.setAuthTag(tag);
    return Buffer.concat([decipher.update(ct), decipher.final()]).toString('utf8');
}

// Enmascara una api_key para mostrarla sin revelarla entera (ej. "XSLE…R5Hkm").
function enmascararClave(clave) {
    if (!clave) return '';
    if (clave.length <= 10) return '••••';
    return `${clave.slice(0, 4)}…${clave.slice(-4)}`;
}

// ── Helpers Binance por-cuenta (multi-cuenta) ─────────────────────────
// Firman y operan con el contexto de cada cuenta { apiKey, secret, base }, en vez de
// usar claves globales del entorno (cada usuario opera su propia cuenta de Binance).
function firmarParams(params, secret) {
    return crypto.createHmac('sha256', secret).update(params).digest('hex');
}

async function balanceDeCuenta(ctx) {
    const ts  = Date.now();
    const q   = `timestamp=${ts}&recvWindow=10000`;
    const url = `${ctx.base}/fapi/v2/balance?${q}&signature=${firmarParams(q, ctx.secret)}`;
    const r    = await fetch(url, { headers: { 'X-MBX-APIKEY': ctx.apiKey } });
    const body = await r.json();
    if (!Array.isArray(body)) throw new Error(body && body.msg ? body.msg : 'no se pudo leer balance');
    const usdt = body.find(b => b.asset === 'USDT');
    return {
        wallet:     usdt ? parseFloat(usdt.balance) : 0,
        disponible: usdt ? parseFloat(usdt.availableBalance) : 0,
    };
}

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.DATABASE_URL?.includes('sslmode=disable') ? false : { rejectUnauthorized: false }
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
    // Cache de velas 1m de BTCUSDT para los backtests de /estrategias. Guardamos solo el
    // timeframe de 1m; 5m y 15m se derivan agregando en SQL (ver fetchKlinesDesdeBD).
    const queryTablaKlines = `
        CREATE TABLE IF NOT EXISTS klines_1m (
            open_time      BIGINT PRIMARY KEY,
            open           NUMERIC NOT NULL,
            high           NUMERIC NOT NULL,
            low            NUMERIC NOT NULL,
            close          NUMERIC NOT NULL,
            volume         NUMERIC NOT NULL,
            close_time     BIGINT  NOT NULL,
            taker_buy_base NUMERIC NOT NULL
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
        await pool.query(queryTablaKlines);
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
                ADD COLUMN IF NOT EXISTS posicion_sl     NUMERIC,
                ADD COLUMN IF NOT EXISTS ultima_cierre_ts BIGINT,
                ADD COLUMN IF NOT EXISTS usuario_id      INTEGER REFERENCES usuarios(id)
        `);
        // La tabla de entradas debe existir ANTES de los ALTER que la modifican (en una BD
        // nueva, alterar una tabla inexistente abortaría toda la inicialización).
        await pool.query(`
            CREATE TABLE IF NOT EXISTS auto_trading_entradas (
                id           SERIAL PRIMARY KEY,
                ts           BIGINT  NOT NULL,
                lado         VARCHAR(10) NOT NULL,
                precio_entrada NUMERIC NOT NULL,
                precio_tp    NUMERIC,
                precio_sl    NUMERIC,
                estado       VARCHAR(10) DEFAULT 'abierta',
                precio_cierre NUMERIC,
                razon_cierre VARCHAR(10),
                ts_cierre    BIGINT
            )
        `);
        // Migración: usuario_id en auto_trading_entradas
        await pool.query(`ALTER TABLE auto_trading_entradas ADD COLUMN IF NOT EXISTS usuario_id INTEGER REFERENCES usuarios(id)`);
        // Migración: qty y stop_type por entrada (para gestionar sub-posiciones / pyramiding en vivo)
        await pool.query(`
            ALTER TABLE auto_trading_entradas
                ADD COLUMN IF NOT EXISTS qty       NUMERIC,
                ADD COLUMN IF NOT EXISTS stop_type VARCHAR(20)
        `);
        // Migración: IDs de las órdenes de protección (TP/SL) colocadas en el exchange,
        // para poder cancelarlas al cerrar la sub-posición y evitar órdenes huérfanas.
        await pool.query(`
            ALTER TABLE auto_trading_entradas
                ADD COLUMN IF NOT EXISTS tp_order_id VARCHAR(32),
                ADD COLUMN IF NOT EXISTS sl_order_id VARCHAR(32)
        `);
        // Migración: referencia de capital inicial para sizing "% capital inicial" en vivo
        await pool.query(`ALTER TABLE auto_trading_config ADD COLUMN IF NOT EXISTS capital_inicial_ref NUMERIC`);

        // Auto-trading multi-cuenta: una cuenta Binance por usuario (1:1). Reemplaza la
        // config global única (auto_trading_config id=1). El secret va cifrado (AES-256-GCM).
        await pool.query(`
            CREATE TABLE IF NOT EXISTS cuentas_trading (
                usuario_id          INTEGER PRIMARY KEY REFERENCES usuarios(id) ON DELETE CASCADE,
                api_key             TEXT,
                api_secret_cifrado  TEXT,
                base_url            TEXT DEFAULT 'https://testnet.binancefuture.com',
                margin_type         VARCHAR(10) DEFAULT 'CROSSED',
                estrategia_nombre   VARCHAR(100),
                position_usdt       NUMERIC DEFAULT 100,
                habilitado          BOOLEAN DEFAULT false,
                posicion_lado       VARCHAR(10),
                posicion_qty        NUMERIC,
                posicion_entry      NUMERIC,
                posicion_tp         NUMERIC,
                posicion_sl         NUMERIC,
                ultima_senal        VARCHAR(10),
                ultima_senal_ts     BIGINT,
                ultima_cierre_ts    BIGINT,
                capital_inicial_ref NUMERIC,
                creado_en           TIMESTAMP DEFAULT NOW()
            )
        `);
        // account_id en auto_trading_entradas para separar entradas por cuenta (= usuario_id).
        await pool.query(`ALTER TABLE auto_trading_entradas ADD COLUMN IF NOT EXISTS account_id INTEGER`);

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

function calcRSI(closes, period = 14) {
    const p = period;
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
        avgG = (avgG * (p - 1) + (d > 0 ? d : 0)) / p;
        avgL = (avgL * (p - 1) + (d < 0 ? -d : 0)) / p;
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

function calcADX(highs, lows, closes, period = 14) {
    const n = closes.length;
    const result = new Array(n).fill(null);
    if (n < 2 * period + 1) return result;

    const tr = [], plusDM = [], minusDM = [];
    for (let i = 1; i < n; i++) {
        const up   = highs[i] - highs[i - 1];
        const down = lows[i - 1] - lows[i];
        plusDM.push(up > down && up > 0 ? up : 0);
        minusDM.push(down > up && down > 0 ? down : 0);
        tr.push(Math.max(highs[i] - lows[i], Math.abs(highs[i] - closes[i - 1]), Math.abs(lows[i] - closes[i - 1])));
    }

    let smTR = 0, smPDM = 0, smMDM = 0;
    for (let i = 0; i < period; i++) { smTR += tr[i]; smPDM += plusDM[i]; smMDM += minusDM[i]; }

    const dx = [];
    const pushDX = () => {
        const pdi = smTR > 0 ? (smPDM / smTR) * 100 : 0;
        const mdi = smTR > 0 ? (smMDM / smTR) * 100 : 0;
        const s = pdi + mdi;
        dx.push(s > 0 ? Math.abs(pdi - mdi) / s * 100 : 0);
    };
    pushDX();
    for (let i = period; i < tr.length; i++) {
        smTR  = smTR  - smTR  / period + tr[i];
        smPDM = smPDM - smPDM / period + plusDM[i];
        smMDM = smMDM - smMDM / period + minusDM[i];
        pushDX();
    }

    if (dx.length < period) return result;
    let adx = dx.slice(0, period).reduce((s, v) => s + v, 0) / period;
    result[2 * period - 1] = adx;
    for (let j = period; j < dx.length; j++) {
        adx = (adx * (period - 1) + dx[j]) / period;
        result[period + j] = adx;
    }
    return result;
}

function calcVWAP(bars, session = 'daily') {
    const n = bars.length;
    const result = new Array(n).fill(null);
    let cumPV = 0, cumV = 0, lastKey = null;
    for (let i = 0; i < n; i++) {
        const ts     = parseInt(bars[i][0]);
        const high   = parseFloat(bars[i][2]);
        const low    = parseFloat(bars[i][3]);
        const close  = parseFloat(bars[i][4]);
        const volume = parseFloat(bars[i][5]);
        const date   = new Date(ts);
        let key;
        if (session === 'weekly') {
            const dow = (date.getUTCDay() + 6) % 7; // Mon=0
            key = ts - (dow * 86400000) - (ts % 86400000);
        } else if (session === 'monthly') {
            key = `${date.getUTCFullYear()}-${date.getUTCMonth()}`;
        } else {
            key = `${date.getUTCFullYear()}-${date.getUTCMonth()}-${date.getUTCDate()}`;
        }
        if (key !== lastKey) { cumPV = 0; cumV = 0; lastKey = key; }
        const tp = (high + low + close) / 3;
        cumPV += tp * volume;
        cumV  += volume;
        result[i] = cumV > 0 ? cumPV / cumV : null;
    }
    return result;
}

async function fetchKlinesBatch(interval, totalBars) {
    const perReq = 1000;
    const CHUNK = 20; // requests en paralelo por tanda
    const dur = { '1m': 60000, '5m': 300000, '15m': 900000 }[interval] || 60000;
    const n = Math.ceil(totalBars / perReq);
    const now = Date.now();

    // Cuando se piden pocas velas (p.ej. el sync incremental: ~3 velas), no tiene
    // sentido pedir 1000 a Binance y descartar 997 por dedup. Acotamos el limit.
    const lim = Math.min(perReq, totalBars);
    const urls = Array.from({ length: n }, (_, i) =>
        `https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=${interval}&limit=${lim}${i > 0 ? `&endTime=${now - i * perReq * dur}` : ''}`
    );

    // No silenciar fallos: un request fallido (p.ej. rate-limit) dejaría huecos de
    // velas que corrompen indicadores y omiten trades sin aviso. Reintentamos y, si
    // persiste, abortamos el backtest con un error claro en vez de correr con datos rotos.
    async function fetchUrl(url, intento = 0) {
        try {
            const r = await fetch(url);
            const j = await r.json();
            if (!Array.isArray(j)) {
                throw new Error(j && j.msg ? j.msg : `respuesta inesperada (${JSON.stringify(j).slice(0, 100)})`);
            }
            return j;
        } catch (e) {
            if (intento < 2) {
                await new Promise(res => setTimeout(res, 400 * (intento + 1)));
                return fetchUrl(url, intento + 1);
            }
            throw new Error(`No se pudieron descargar las velas ${interval} de Binance (${e.message}). Probá con menos días o reintentá.`);
        }
    }

    const results = [];
    for (let i = 0; i < urls.length; i += CHUNK) {
        const batch = urls.slice(i, i + CHUNK).map(url => fetchUrl(url));
        const batchResults = await Promise.all(batch);
        results.push(...batchResults);
    }

    const seen = new Set();
    const all = [];
    results.flat().forEach(v => {
        // v[6] = closeTime. Descartamos la vela en curso (aún no cerrada) para no operar
        // sobre datos incompletos ni introducir look-ahead en el último bar.
        if (Array.isArray(v) && v[6] <= now && !seen.has(v[0])) { seen.add(v[0]); all.push(v); }
    });
    return all.sort((a, b) => a[0] - b[0]);
}

// ── Cache de velas en BD ───────────────────────────────────────────────────
// Guardamos solo 1m. fetchKlinesDesdeBD devuelve velas en EL MISMO formato array
// que Binance (índices: 0 openTime, 1 open, 2 high, 3 low, 4 close, 5 volume,
// 6 closeTime, 9 takerBuyBase) para que runBacktest sea agnóstico de la fuente.
const DIAS_CACHE_KLINES = 365;
let sincronizandoKlines = false;

async function upsertKlines1m(velas) {
    const CHUNK = 500; // 500 filas × 8 params = 4000, bajo el límite de 65535 de PG
    let insertadas = 0;
    for (let i = 0; i < velas.length; i += CHUNK) {
        const slice = velas.slice(i, i + CHUNK);
        const placeholders = [];
        const params = [];
        slice.forEach((k, idx) => {
            const o = idx * 8;
            placeholders.push(`($${o+1},$${o+2},$${o+3},$${o+4},$${o+5},$${o+6},$${o+7},$${o+8})`);
            params.push(
                Number(k[0]), parseFloat(k[1]), parseFloat(k[2]), parseFloat(k[3]),
                parseFloat(k[4]), parseFloat(k[5]), Number(k[6]), parseFloat(k[9])
            );
        });
        const r = await pool.query(
            `INSERT INTO klines_1m (open_time, open, high, low, close, volume, close_time, taker_buy_base)
             VALUES ${placeholders.join(',')}
             ON CONFLICT (open_time) DO NOTHING`,
            params
        );
        insertadas += r.rowCount;
    }
    return insertadas;
}

// Mantiene la cache fresca. Si la tabla está vacía hace el backfill inicial de
// DIAS_CACHE_KLINES días; si no, trae solo las velas nuevas desde la última guardada.
async function sincronizarKlines() {
    if (sincronizandoKlines) return;
    sincronizandoKlines = true;
    try {
        const r = await pool.query('SELECT MAX(open_time) AS ult FROM klines_1m');
        const ult = r.rows[0].ult ? Number(r.rows[0].ult) : null;
        const now = Date.now();
        let faltan;
        if (!ult) {
            faltan = DIAS_CACHE_KLINES * 1440;
            console.log(`[Klines] BD vacía: backfill inicial de ${DIAS_CACHE_KLINES} días (~${faltan} velas 1m). Puede tardar varios minutos…`);
        } else {
            faltan = Math.ceil((now - ult) / 60000) + 2; // +2 de solape para no perder velas
            if (faltan <= 2) { sincronizandoKlines = false; return; } // nada nuevo cerrado
            faltan = Math.min(faltan, DIAS_CACHE_KLINES * 1440);
        }
        const velas = await fetchKlinesBatch('1m', faltan);
        if (velas.length) {
            const nuevas = await upsertKlines1m(velas);
            if (nuevas > 0) {
                const ultIso = new Date(Number(velas[velas.length - 1][0])).toISOString().slice(0, 16).replace('T', ' ');
                console.log(`[Klines] +${nuevas} velas 1m nuevas (última: ${ultIso} UTC).`);
            }
        }
        // Descartamos velas más viejas que la ventana de cache para no crecer sin límite.
        await pool.query('DELETE FROM klines_1m WHERE open_time < $1', [now - DIAS_CACHE_KLINES * 86400000]);
    } catch (e) {
        console.error('[Klines] Error al sincronizar:', e.message);
    } finally {
        sincronizandoKlines = false;
    }
}
setInterval(sincronizarKlines, 60000);
// Arranque diferido: damos tiempo a que inicializarBaseDeDatos cree la tabla.
setTimeout(sincronizarKlines, 8000);

async function fetchKlinesDesdeBD(interval, days) {
    const desde = Date.now() - days * 86400000;
    if (interval === '1m') {
        const r = await pool.query(
            `SELECT open_time, open, high, low, close, volume, close_time, taker_buy_base
             FROM klines_1m WHERE open_time >= $1::bigint ORDER BY open_time ASC`,
            [desde]
        );
        return r.rows.map(f => [
            Number(f.open_time), Number(f.open), Number(f.high), Number(f.low),
            Number(f.close), Number(f.volume), Number(f.close_time), 0, 0, Number(f.taker_buy_base)
        ]);
    }
    // Derivamos 5m / 15m agregando las velas de 1m por bucket temporal.
    const bucket = interval === '15m' ? 900000 : 300000;
    // Descartamos el bucket EN CURSO: aquel cuyo período aún no terminó ((b+1)*bucket > now).
    // Sus velas 1m están incompletas, así que su OHLC/indicadores son parciales y cambian cada
    // minuto. Incluirlo hacía que un backtest abriera trades sobre datos no finales que
    // desaparecían (o cerraban en un SL fantasma) al re-correrlo un minuto después.
    const r = await pool.query(
        `SELECT b * $2::bigint AS open_time,
                (array_agg(open  ORDER BY open_time ASC ))[1] AS open,
                MAX(high) AS high,
                MIN(low)  AS low,
                (array_agg(close ORDER BY open_time DESC))[1] AS close,
                SUM(volume) AS volume,
                MAX(close_time) AS close_time,
                SUM(taker_buy_base) AS taker_buy_base
         FROM (SELECT open_time, open_time / $2::bigint AS b, open, high, low, close, volume, close_time, taker_buy_base
               FROM klines_1m WHERE open_time >= $1::bigint) t
         GROUP BY b
         HAVING (b + 1) * $2::bigint <= $3::bigint
         ORDER BY b ASC`,
        [desde, bucket, Date.now()]
    );
    return r.rows.map(f => [
        Number(f.open_time), Number(f.open), Number(f.high), Number(f.low),
        Number(f.close), Number(f.volume), Number(f.close_time), 0, 0, Number(f.taker_buy_base)
    ]);
}

function lookupHTF(sortedTs, byTs, target) {
    let lo = 0, hi = sortedTs.length - 1, found = -1;
    while (lo <= hi) {
        const mid = (lo + hi) >> 1;
        if (sortedTs[mid] <= target) { found = mid; lo = mid + 1; } else hi = mid - 1;
    }
    return found >= 0 ? byTs.get(sortedTs[found]) : null;
}

function calcCapitalEntrada(p, capital, posicionesAbiertas) {
    let monto;
    if (p.posicionTipo === 'monto_fijo') {
        monto = p.posicionValor ?? capital;
    } else if (p.posicionTipo === 'porc_capital_inicial') {
        monto = p.initialCapital * ((p.posicionValor ?? 100) / 100);
    } else {
        // porc_capital_actual
        monto = capital * ((p.posicionValor ?? 100) / 100);
    }
    // Descontar lo ya asignado a posiciones abiertas para no sobre-exponer el capital.
    // Solo entrar si el disponible cubre el monto completo — sin posiciones parciales.
    const asignado = posicionesAbiertas.reduce((s, pos) => s + pos.capitalAtEntry, 0);
    const disponible = capital - asignado;
    return disponible >= monto ? monto : 0;
}

// Costo total de una operación como fracción del capital de la posición (margen):
// comisión y slippage se cobran en entrada + salida; el funding se prorratea por el
// tiempo en operación. Todo escala con la palanca porque se aplica sobre el nocional.
function costoOperacion(p, palanca, minutesHeld) {
    const fees = (p.commission   / 100) * 2 * palanca;                          // entrada + salida
    const slip = (p.slippagePerc / 100) * 2 * palanca;                          // entrada + salida
    const fund = (p.fundingPerc  / 100) * (Math.max(0, minutesHeld) / 480) * palanca; // por cada 8h
    return fees + slip + fund;
}

function runBacktest(bars1m, bars5m, bars15m, whalesArr, p) {
    const c1m = bars1m.map(b => parseFloat(b[4]));
    const e50 = calcEMA(c1m, 50), e100 = calcEMA(c1m, 100),
          e200 = calcEMA(c1m, 200), e500 = calcEMA(c1m, 500);

    // Indexamos los indicadores HTF por tiempo de CIERRE (b[6]), no de apertura (b[0]).
    // Así lookupHTF(ts) solo devuelve velas HTF ya cerradas respecto a la vela 1m actual,
    // evitando look-ahead bias (usar el valor final de una vela 15m/5m aún en formación).
    const c15m = bars15m.map(b => parseFloat(b[4]));
    const c5m_pb = bars5m.map(b => parseFloat(b[4]));

    // Pullback EMA arrays — configurables por el usuario (período + temporalidad)
    const pbEMAConfig = (Array.isArray(p.pullbackEMAs) && p.pullbackEMAs.length > 0)
        ? p.pullbackEMAs
        : [{ period:50,tf:'1m' },{ period:100,tf:'1m' },{ period:200,tf:'1m' },{ period:500,tf:'1m' }];
    const pbArr1m = {}, pbArr5m = {}, pbArr15m = {};
    for (const { period, tf } of pbEMAConfig) {
        if (tf === '1m' && !pbArr1m[period]) {
            pbArr1m[period] = calcEMA(c1m, period);
        } else if (tf === '5m' && !pbArr5m[period]) {
            const vals = calcEMA(c5m_pb, period);
            pbArr5m[period] = {
                ts:  bars5m.map(b => parseInt(b[6])).sort((a, b) => a - b),
                map: new Map(bars5m.map((b, i) => [parseInt(b[6]), vals[i]]))
            };
        } else if (tf === '15m' && !pbArr15m[period]) {
            const vals = calcEMA(c15m, period);
            pbArr15m[period] = {
                ts:  bars15m.map(b => parseInt(b[6])).sort((a, b) => a - b),
                map: new Map(bars15m.map((b, i) => [parseInt(b[6]), vals[i]]))
            };
        }
    }
    // RSI — configurable período, temporalidad y umbrales de entrada
    const rsiPeriod   = p.rsiPeriod   || 14;
    const rsiTf       = p.rsiTf       || '15m';
    const rsiLongMin  = p.rsiLongMin  ?? 60;
    const rsiShortMax = p.rsiShortMax ?? 40;
    const useRsiFilter = p.useRsiFilter !== false;   // default ON (compat. estrategias previas)
    let rsiDirect = null, rsiByTs = null, tsRsi = null;
    if (rsiTf === '1m') {
        rsiDirect = calcRSI(c1m, rsiPeriod);
    } else if (rsiTf === '5m') {
        const arr = calcRSI(c5m_pb, rsiPeriod);
        rsiByTs = new Map(bars5m.map((b, i) => [parseInt(b[6]), arr[i]]));
        tsRsi   = bars5m.map(b => parseInt(b[6])).sort((a, b) => a - b);
    } else {
        const arr = calcRSI(c15m, rsiPeriod);
        rsiByTs = new Map(bars15m.map((b, i) => [parseInt(b[6]), arr[i]]));
        tsRsi   = bars15m.map(b => parseInt(b[6])).sort((a, b) => a - b);
    }

    const c5m = c5m_pb;
    // MACD — configurable período (fast/slow/signal) y temporalidad
    const macdFast   = p.macdFast   || 12;
    const macdSlow   = p.macdSlow   || 26;
    const macdSignal = p.macdSignal || 9;
    const macdTf     = p.macdTf     || '5m';
    const useMacdFilter = p.useMacdFilter !== false;  // default ON (compat. estrategias previas)
    let macdDirect = null, macdByTs = null, tsMACD = null;
    if (macdTf === '1m') {
        const { macd: mArr, signal: sArr } = calcMACDArr(c1m, macdFast, macdSlow, macdSignal);
        macdDirect = mArr.map((m, idx) => ({ macd: m, sig: sArr[idx] }));
    } else if (macdTf === '5m') {
        const { macd: mArr, signal: sArr } = calcMACDArr(c5m, macdFast, macdSlow, macdSignal);
        macdByTs = new Map(bars5m.map((b, i) => [parseInt(b[6]), { macd: mArr[i], sig: sArr[i] }]));
        tsMACD   = bars5m.map(b => parseInt(b[6])).sort((a, b) => a - b);
    } else {
        const { macd: mArr, signal: sArr } = calcMACDArr(c15m, macdFast, macdSlow, macdSignal);
        macdByTs = new Map(bars15m.map((b, i) => [parseInt(b[6]), { macd: mArr[i], sig: sArr[i] }]));
        tsMACD   = bars15m.map(b => parseInt(b[6])).sort((a, b) => a - b);
    }

    const h15m = bars15m.map(b => parseFloat(b[2]));
    const l15m = bars15m.map(b => parseFloat(b[3]));
    const adxArr15m = calcADX(h15m, l15m, c15m);
    const adx15mByTs = new Map(bars15m.map((b, i) => [parseInt(b[6]), adxArr15m[i]]));
    const tsAdx15m = bars15m.map(b => parseInt(b[6])).sort((a, b) => a - b);

    // VWAP — configurable timeframe y sesión
    const vwapTf_bt      = p.vwapTf      || '5m';
    const vwapSession_bt = p.vwapSession || 'daily';
    let vwapDirect_bt = null, vwapByTs_bt = null, tsVwap_bt = null;
    if (p.useVwapFilter) {
        if (vwapTf_bt === '1m') {
            vwapDirect_bt = calcVWAP(bars1m, vwapSession_bt);
        } else if (vwapTf_bt === '5m') {
            const vals = calcVWAP(bars5m, vwapSession_bt);
            vwapByTs_bt = new Map(bars5m.map((b, idx) => [parseInt(b[6]), vals[idx]]));
            tsVwap_bt   = bars5m.map(b => parseInt(b[6])).sort((a, b) => a - b);
        } else {
            const vals = calcVWAP(bars15m, vwapSession_bt);
            vwapByTs_bt = new Map(bars15m.map((b, idx) => [parseInt(b[6]), vals[idx]]));
            tsVwap_bt   = bars15m.map(b => parseInt(b[6])).sort((a, b) => a - b);
        }
    }

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

    let capital = p.initialCapital;
    let posiciones = []; // { side, entry, entryBarIdx, tp, sl, capitalAtEntry }
    let lastClosedBarIdx = null;
    const trades = [];
    const equity = [{ ts: parseInt(bars1m[0][0]), v: capital }];
    // Drawdown marcado a mercado: se actualiza en cada vela (no solo al cerrar trade),
    // capturando la peor excursión adversa de todas las posiciones abiertas.
    let ddPeak = capital, maxDDPerc = 0;
    const WARMUP = 500;
    // Curva de equity marcada a mercado (incluye PnL no realizado), submuestreada a ~600
    // puntos para que coincida con el max drawdown reportado y no pese de más en payload.
    const eqStep = Math.max(1, Math.floor((bars1m.length - WARMUP) / 600));

    for (let i = WARMUP; i < bars1m.length; i++) {
        const bar = bars1m[i];
        const ts = parseInt(bar[0]);
        const tsClose = parseInt(bar[6]); // instante real de la decisión (cierre de la vela 1m)
        const high = parseFloat(bar[2]), low = parseFloat(bar[3]), close = parseFloat(bar[4]);
        const E50 = e50[i], E100 = e100[i], E200 = e200[i], E500 = e500[i];
        if (!E500) continue;

        const rsiRaw  = rsiTf === '1m'  ? rsiDirect[i]  : lookupHTF(tsRsi,  rsiByTs,  tsClose);
        const macdRaw = macdTf === '1m' ? macdDirect[i] : lookupHTF(tsMACD, macdByTs, tsClose);
        if (rsiRaw == null || !macdRaw || macdRaw.macd === null || macdRaw.sig === null) continue;

        const rsiVal = rsiRaw;
        const { macd: macd5, sig: sig5 } = macdRaw;

        // SALIDAS — iterar en reversa para poder hacer splice sin afectar índices
        for (let j = posiciones.length - 1; j >= 0; j--) {
            const pos = posiciones[j];
            const barsIn = i - pos.entryBarIdx;
            let exitPrice = null, exitReason = null;

            // Liquidación por apalancamiento — tiene prioridad sobre TP/SL.
            // El margen aislado se agota un poco ANTES de 1/palanca por el margen de
            // mantenimiento y la comisión de cierre, así que la liquidación ocurre antes.
            if (p.palancaActivo && p.palancaValor > 1) {
                const MMR = 0.005; // margen de mantenimiento aprox. (BTC perp)
                const liqDelta = Math.max(0.0001, 1 / p.palancaValor - MMR - (p.commission / 100));
                if (pos.side === 1 && low <= pos.entry * (1 - liqDelta)) {
                    exitPrice = pos.entry * (1 - liqDelta); exitReason = 'LIQ';
                } else if (pos.side === -1 && high >= pos.entry * (1 + liqDelta)) {
                    exitPrice = pos.entry * (1 + liqDelta); exitReason = 'LIQ';
                }
            }

            if (!exitPrice) {
                if (pos.side === 1) {
                    if (p.stopType === 'Porcentaje') {
                        if      (high >= pos.tp && low <= pos.sl) { exitPrice = pos.sl; exitReason = 'SL'; }
                        else if (high >= pos.tp)                   { exitPrice = pos.tp; exitReason = 'TP'; }
                        else if (low  <= pos.sl)                   { exitPrice = pos.sl; exitReason = 'SL'; }
                    } else {
                        // Conservador: si en la misma vela rompe la EMA (al cierre) y toca el TP, prioriza la ruptura.
                        const se = p.stopType === 'Ruptura EMA 200' ? E200 : E500;
                        if      (close < se)     { exitPrice = close;  exitReason = 'EMA'; }
                        else if (high >= pos.tp) { exitPrice = pos.tp; exitReason = 'TP'; }
                    }
                    if (!exitPrice && p.useMaxTradeTime && barsIn >= p.maxTradeMinutes) { exitPrice = close; exitReason = 'Tiempo'; }
                } else {
                    if (p.stopType === 'Porcentaje') {
                        if      (low <= pos.tp && high >= pos.sl) { exitPrice = pos.sl; exitReason = 'SL'; }
                        else if (low  <= pos.tp)                   { exitPrice = pos.tp; exitReason = 'TP'; }
                        else if (high >= pos.sl)                   { exitPrice = pos.sl; exitReason = 'SL'; }
                    } else {
                        // Conservador: si en la misma vela rompe la EMA (al cierre) y toca el TP, prioriza la ruptura.
                        const se = p.stopType === 'Ruptura EMA 200' ? E200 : E500;
                        if      (close > se)     { exitPrice = close;   exitReason = 'EMA'; }
                        else if (low <= pos.tp)  { exitPrice = pos.tp;  exitReason = 'TP'; }
                    }
                    if (!exitPrice && p.useMaxTradeTime && barsIn >= p.maxTradeMinutes) { exitPrice = close; exitReason = 'Tiempo'; }
                }
            }

            if (exitPrice) {
                const raw = pos.side === 1
                    ? (exitPrice - pos.entry) / pos.entry
                    : (pos.entry - exitPrice) / pos.entry;
                const palanca = p.palancaActivo ? (p.palancaValor || 1) : 1;
                const net = raw * palanca - costoOperacion(p, palanca, barsIn);
                // En margen aislado la liquidación consume todo el margen (el de
                // mantenimiento restante se lo lleva el fee de liquidación): pérdida = -margen.
                // Sin este caso especial, a palanca alta se sub-contabiliza la pérdida.
                const pnlAbs = exitReason === 'LIQ'
                    ? -pos.capitalAtEntry
                    : Math.max(pos.capitalAtEntry * net, -pos.capitalAtEntry);
                capital += pnlAbs;
                trades.push({ type: pos.side === 1 ? 'Long' : 'Short', entryTs: parseInt(bars1m[pos.entryBarIdx][0]), exitTs: ts, entryPrice: pos.entry, exitPrice, tp: pos.tp, sl: pos.sl, pnlPerc: (pnlAbs / pos.capitalAtEntry) * 100, pnlAbs, reason: exitReason, capital });
                lastClosedBarIdx = i;
                posiciones.splice(j, 1);
            }
        }

        // Actualizar sliding window de ballenas (se hace siempre, no solo en entrada).
        // Anclada al CIERRE de la vela 1m (tsClose): la decisión ocurre al cierre, así que
        // se incluyen las ballenas de la propia vela (igual que el filtro de delta) sin
        // look-ahead, y queda alineado con el modo en vivo (que usa el momento actual).
        while (wRight + 1 < whaleTrades.length && whaleTrades[wRight + 1].ts <= tsClose) {
            wRight++;
            if (whaleTrades[wRight].isSell) wSells += whaleTrades[wRight].btc; else wBuys += whaleTrades[wRight].btc;
        }
        while (wLeft <= wRight && whaleTrades[wLeft].ts < tsClose - whaleWindowMs) {
            if (whaleTrades[wLeft].isSell) wSells -= whaleTrades[wLeft].btc; else wBuys -= whaleTrades[wLeft].btc;
            wLeft++;
        }
        const whaleDelta = wBuys - wSells;

        // ENTRADAS — permitir si no hay posición abierta, o si múltiples entradas está habilitado.
        // Filtro opcional: si ya hay sub-posiciones abiertas con PnL no realizado negativo,
        // bloquear nuevas entradas múltiples (filtra rachas perdedoras al precio actual).
        const hayPosicionEnPerdida = p.blockMultipleIfLosing && posiciones.some(pos =>
            (pos.side === 1 ? close - pos.entry : pos.entry - close) < 0
        );
        if ((posiciones.length === 0 || p.allowMultipleEntries) && !hayPosicionEnPerdida) {
            const barHour = new Date(ts).getUTCHours();
            const argDay = new Date(ts - 3 * 3600000).getUTCDay(); // 0=Dom, 6=Sáb en horario Argentina
            const barsSinceClose = lastClosedBarIdx !== null ? i - lastClosedBarIdx : 999999;

            if (
                barHour >= p.startHour && barHour < p.endHour &&
                (p.operaFinDeSemana || (argDay !== 0 && argDay !== 6)) &&
                !(p.useCooldown && barsSinceClose < p.cooldownMinutes)
            ) {
                const above = close > E50 && close > E100 && close > E200 && close > E500;
                const below = close < E50 && close < E100 && close < E200 && close < E500;
                const bullAlign = E50 > E100 && E100 > E200 && E200 > E500;
                const bearAlign = E50 < E100 && E100 < E200 && E200 < E500;
                const pbVals = pbEMAConfig.map(({ period, tf }) => {
                    if (tf === '1m')  return pbArr1m[period]?.[i];
                    if (tf === '5m')  return pbArr5m[period]  ? lookupHTF(pbArr5m[period].ts,  pbArr5m[period].map,  tsClose) : null;
                    if (tf === '15m') return pbArr15m[period] ? lookupHTF(pbArr15m[period].ts, pbArr15m[period].map, tsClose) : null;
                    return null;
                }).filter(v => v != null && v > 0);
                const nearEMA = pbVals.some(e => Math.abs(close - e) / close * 100 <= (p.pullbackPerc ?? 0.2));
                const pullOK     = !p.usePullbackFilter || (pbVals.length > 0 && nearEMA);
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

                // ADX 15m: mide fuerza de tendencia (>=25 = tendencia fuerte)
                const adxRaw = lookupHTF(tsAdx15m, adx15mByTs, tsClose);
                const adxOk  = !p.useADXFilter || (adxRaw !== null && adxRaw >= (p.adxThreshold ?? 25));

                // VWAP — dirección y/o pullback
                const vwapVal_bt = !p.useVwapFilter ? null
                    : vwapTf_bt === '1m' ? vwapDirect_bt[i] : lookupHTF(tsVwap_bt, vwapByTs_bt, tsClose);
                const vwapOkLong  = !p.useVwapFilter || (vwapVal_bt !== null &&
                    (!p.vwapUseDirection || close > vwapVal_bt) &&
                    (!p.vwapUsePullback  || Math.abs(close - vwapVal_bt) / close * 100 <= (p.vwapPullbackPerc ?? 0.3))
                );
                const vwapOkShort = !p.useVwapFilter || (vwapVal_bt !== null &&
                    (!p.vwapUseDirection || close < vwapVal_bt) &&
                    (!p.vwapUsePullback  || Math.abs(close - vwapVal_bt) / close * 100 <= (p.vwapPullbackPerc ?? 0.3))
                );

                if (p.enableLongs && above && alignLong && (!useRsiFilter || rsiVal >= rsiLongMin) && (!useMacdFilter || macd5 > sig5) && pullOK && deltaOkLong && whaleOkLong && adxOk && vwapOkLong) {
                    const capEntrada = calcCapitalEntrada(p, capital, posiciones);
                    if (capEntrada <= 0) { /* sin capital disponible, no entrar */ }
                    else {
                        const tp = close * (1 + p.tpPerc / 100);
                        const sl = p.stopType === 'Porcentaje' ? close * (1 - p.slPerc / 100) : (p.stopType === 'Ruptura EMA 200' ? E200 : E500);
                        posiciones.push({ side: 1, entry: close, entryBarIdx: i, tp, sl, capitalAtEntry: capEntrada });
                    }
                } else if (p.enableShorts && below && alignShort && (!useRsiFilter || rsiVal <= rsiShortMax) && (!useMacdFilter || macd5 < sig5) && pullOK && deltaOkShort && whaleOkShort && adxOk && vwapOkShort) {
                    const capEntrada = calcCapitalEntrada(p, capital, posiciones);
                    if (capEntrada <= 0) { /* sin capital disponible, no entrar */ }
                    else {
                        const tp = close * (1 - p.tpPerc / 100);
                        const sl = p.stopType === 'Porcentaje' ? close * (1 + p.slPerc / 100) : (p.stopType === 'Ruptura EMA 200' ? E200 : E500);
                        posiciones.push({ side: -1, entry: close, entryBarIdx: i, tp, sl, capitalAtEntry: capEntrada });
                    }
                }
            }
        }

        // Marcado a mercado de la vela: equity realizada + PnL no realizado de todas las posiciones abiertas
        let markedEquity = capital;
        for (const pos of posiciones) {
            const raw = pos.side === 1 ? (close - pos.entry) / pos.entry : (pos.entry - close) / pos.entry;
            const palanca = p.palancaActivo ? (p.palancaValor || 1) : 1;
            markedEquity += Math.max(pos.capitalAtEntry * (raw * palanca - costoOperacion(p, palanca, i - pos.entryBarIdx)), -pos.capitalAtEntry);
        }
        if (markedEquity > ddPeak) ddPeak = markedEquity;
        const ddNow = (ddPeak - markedEquity) / ddPeak * 100;
        if (ddNow > maxDDPerc) maxDDPerc = ddNow;

        // Muestreo de la curva de equity a mercado (último bar siempre incluido)
        if ((i - WARMUP) % eqStep === 0 || i === bars1m.length - 1) {
            equity.push({ ts, v: markedEquity });
        }
    }

    // Cierre forzado de posiciones que quedan abiertas al final del período: se liquidan
    // al cierre de la última vela ('Fin') para que stats (netProfit/winRate/...) y la curva
    // de equity reflejen lo mismo y no quede PnL no realizado fuera de las métricas.
    if (posiciones.length > 0) {
        const lastIdx   = bars1m.length - 1;
        const lastClose = parseFloat(bars1m[lastIdx][4]);
        const lastTs    = parseInt(bars1m[lastIdx][0]);
        for (let j = posiciones.length - 1; j >= 0; j--) {
            const pos = posiciones[j];
            const barsIn = lastIdx - pos.entryBarIdx;
            const raw = pos.side === 1
                ? (lastClose - pos.entry) / pos.entry
                : (pos.entry - lastClose) / pos.entry;
            const palanca = p.palancaActivo ? (p.palancaValor || 1) : 1;
            const net = raw * palanca - costoOperacion(p, palanca, barsIn);
            const pnlAbs = Math.max(pos.capitalAtEntry * net, -pos.capitalAtEntry);
            capital += pnlAbs;
            trades.push({ type: pos.side === 1 ? 'Long' : 'Short', entryTs: parseInt(bars1m[pos.entryBarIdx][0]), exitTs: lastTs, entryPrice: pos.entry, exitPrice: lastClose, tp: pos.tp, sl: pos.sl, pnlPerc: (pnlAbs / pos.capitalAtEntry) * 100, pnlAbs, reason: 'Fin', capital });
        }
        posiciones = [];
    }

    const wins   = trades.filter(t => t.pnlPerc > 0);
    const losses = trades.filter(t => t.pnlPerc <= 0);
    const grossW = wins.reduce((s, t) => s + Math.abs(t.pnlAbs), 0);
    const grossL = losses.reduce((s, t) => s + Math.abs(t.pnlAbs), 0);

    // Desglose de razones de salida sobre TODOS los trades (no solo los devueltos)
    const exitReasons = {};
    trades.forEach(t => { exitReasons[t.reason] = (exitReasons[t.reason] || 0) + 1; });

    // Máxima racha de ganadoras / perdedoras consecutivas
    let maxWinStreak = 0, maxLossStreak = 0, curWin = 0, curLoss = 0;
    trades.forEach(t => {
        if (t.pnlPerc > 0) {
            curWin++; curLoss = 0;
            if (curWin > maxWinStreak) maxWinStreak = curWin;
        } else {
            curLoss++; curWin = 0;
            if (curLoss > maxLossStreak) maxLossStreak = curLoss;
        }
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
            maxWinStreak, maxLossStreak,
            exitReasons,
        },
        trades: trades.slice(-300),
        equity
    };
}

// ── Auto-Trading Loop ─────────────────────────────────────────
const N8N_WEBHOOK_URL    = process.env.N8N_WEBHOOK_URL;
const BINANCE_API_KEY    = process.env.Clave_API_Binance;
const BINANCE_SECRET     = process.env.Clave_secreta_Binance;
// Por defecto testnet (plata virtual). Para operar en real, definir en el entorno
// BINANCE_BASE=https://fapi.binance.com (y usar claves API de la cuenta real).
const BINANCE_BASE       = process.env.BINANCE_BASE || 'https://testnet.binancefuture.com';
// Testnet y real son mercados distintos con precios distintos. Como un usuario puede operar
// en testnet y otro en real al mismo tiempo, mantenemos un feed de precio por entorno y nunca
// evaluamos los stops de una cuenta con el precio del otro mercado.
const WS_PRECIO_POR_ENTORNO = {
    testnet: 'wss://stream.binancefuture.com/ws/btcusdt@aggTrade',
    real:    'wss://fstream.binance.com/ws/btcusdt@aggTrade',
};

// Estado de sub-posiciones activas POR CUENTA (uid → array). El exchange netea las
// posiciones de un símbolo dentro de cada cuenta, así que cada sub-posición vive en memoria
// y se cierra con una orden reduceOnly PARCIAL, replicando el pyramiding del backtest.
const posicionesPorCuenta = new Map(); // uid -> [{ id, lado, qty, entry, tp, sl, entryTs, stopType, tpOrderId, slOrderId }]
const ctxActivos          = new Map(); // uid -> { uid, apiKey, secret, base, marginType } (claves descifradas en memoria)
const ultimoPrecioPorEntorno = { testnet: null, real: null }; // último precio del WS de futuros por entorno

// El entorno de una cuenta se deduce de su base_url (testnet vs producción).
function entornoDeBase(base) { return String(base).includes('testnet') ? 'testnet' : 'real'; }
// Último precio de futuros del mercado en el que opera la cuenta.
function precioDeCtx(ctx) { return ultimoPrecioPorEntorno[entornoDeBase(ctx.base)]; }

function posDe(uid) {
    let arr = posicionesPorCuenta.get(uid);
    if (!arr) { arr = []; posicionesPorCuenta.set(uid, arr); }
    return arr;
}

// Contexto de una cuenta (claves descifradas) a partir de su fila de cuentas_trading.
function ctxDeCuenta(row) {
    return {
        uid:        row.usuario_id,
        apiKey:     row.api_key,
        secret:     descifrarSecreto(row.api_secret_cifrado),
        base:       row.base_url || 'https://testnet.binancefuture.com',
        marginType: (row.margin_type || 'CROSSED').toUpperCase(),
    };
}

// Mantiene las columnas posicion_* (posición NETA de la cuenta) para mostrar estado en la UI.
async function sincronizarPosicionBD(uid) {
    const arr = posDe(uid);
    if (arr.length === 0) {
        await pool.query(
            `UPDATE cuentas_trading
             SET posicion_lado=NULL, posicion_qty=NULL, posicion_entry=NULL, posicion_tp=NULL, posicion_sl=NULL
             WHERE usuario_id=$1`, [uid]
        );
        return;
    }
    const qty   = arr.reduce((s, p) => s + p.qty, 0);
    const entry = arr.reduce((s, p) => s + p.entry * p.qty, 0) / (qty || 1);
    await pool.query(
        `UPDATE cuentas_trading
         SET posicion_lado=$1, posicion_qty=$2, posicion_entry=$3, posicion_tp=$4, posicion_sl=$5
         WHERE usuario_id=$6`,
        [arr[0].lado, qty, entry, arr[0].tp, arr[0].sl, uid]
    );
}

function buildCloseUrl(ctx, lado, qty) {
    const side = lado === 'long' ? 'SELL' : 'BUY';
    const ts   = Date.now();
    const p    = `symbol=BTCUSDT&side=${side}&type=MARKET&quantity=${qty}&reduceOnly=true&timestamp=${ts}`;
    return `${ctx.base}/fapi/v1/order?${p}&signature=${firmarParams(p, ctx.secret)}`;
}

function buildEntryUrl(ctx, lado, qty) {
    const side = lado === 'long' ? 'BUY' : 'SELL';
    const ts   = Date.now();
    const p    = `symbol=BTCUSDT&side=${side}&type=MARKET&quantity=${qty}&timestamp=${ts}`;
    return `${ctx.base}/fapi/v1/order?${p}&signature=${firmarParams(p, ctx.secret)}`;
}

// BTCUSDT perp tiene tick size 0.1 → los stopPrice deben redondearse a 1 decimal.
function redondearPrecio(precio) {
    return Math.round(precio * 10) / 10;
}

// Orden de protección reduceOnly en el exchange (TAKE_PROFIT_MARKET o STOP_MARKET).
// Disparada por MARK_PRICE para evitar wicks de precio last. Cierra solo su qty.
// Desde 2025-12-09 Binance migró las órdenes condicionales al servicio Algo:
// /fapi/v1/order las rechaza con -4120, así que van por /fapi/v1/algoOrder
// (algoType=CONDITIONAL, stopPrice → triggerPrice, la respuesta trae algoId).
function buildStopUrl(ctx, lado, qty, stopPrice, tipo) {
    const side = lado === 'long' ? 'SELL' : 'BUY';
    const sp   = redondearPrecio(stopPrice);
    const ts   = Date.now();
    const p    = `algoType=CONDITIONAL&symbol=BTCUSDT&side=${side}&type=${tipo}&quantity=${qty}&triggerPrice=${sp}&reduceOnly=true&workingType=MARK_PRICE&timestamp=${ts}`;
    return `${ctx.base}/fapi/v1/algoOrder?${p}&signature=${firmarParams(p, ctx.secret)}`;
}

// Cancela una orden de protección por su algoId (las TP/SL ahora son órdenes Algo).
async function cancelarOrden(ctx, algoId) {
    if (!algoId) return;
    try {
        const ts  = Date.now();
        const q   = `symbol=BTCUSDT&algoId=${algoId}&timestamp=${ts}`;
        const url = `${ctx.base}/fapi/v1/algoOrder?${q}&signature=${firmarParams(q, ctx.secret)}`;
        await fetch(url, { method: 'DELETE', headers: { 'X-MBX-APIKEY': ctx.apiKey } });
    } catch (e) { console.error(`[AutoTrading] Error cancelando orden ${algoId}: ${e.message}`); }
}

// Barrido: cancela todas las órdenes abiertas del símbolo (al quedar plano / reconciliar).
// Limpia tanto las condicionales Algo (TP/SL actuales) como cualquier orden clásica residual.
async function cancelarTodasLasOrdenes(ctx) {
    const ts = Date.now();
    const eliminar = async (endpoint) => {
        try {
            const q   = `symbol=BTCUSDT&timestamp=${ts}`;
            const url = `${ctx.base}/${endpoint}?${q}&signature=${firmarParams(q, ctx.secret)}`;
            await fetch(url, { method: 'DELETE', headers: { 'X-MBX-APIKEY': ctx.apiKey } });
        } catch (e) { console.error(`[AutoTrading] Error cancelando ${endpoint}: ${e.message}`); }
    };
    await eliminar('fapi/v1/algoOpenOrders');
    await eliminar('fapi/v1/allOpenOrders');
}

// Coloca en el exchange las órdenes de protección de una sub-posición recién abierta.
// El TP siempre es un nivel fijo; el SL solo se coloca si el stop es por Porcentaje
// (los stops por Ruptura EMA / Tiempo no son niveles de precio y los gestiona el server).
// Sirven de red de seguridad: si el server se cae, el exchange igual cierra la posición.
async function colocarProteccionExchange(ctx, sub) {
    const estado = { tp: null, sl: null }; // null = no intentada; {ok,msg} si se intentó
    try {
        if (sub.tp) {
            const r = await ejecutarOrdenBinance(ctx,
                buildStopUrl(ctx, sub.lado, sub.qty, sub.tp, 'TAKE_PROFIT_MARKET'), `TP-EXCH #${sub.id}`);
            const id = r.body?.algoId ?? r.body?.orderId;
            if (r.ok && id) sub.tpOrderId = String(id);
            estado.tp = { ok: r.ok, msg: r.body?.msg, code: r.body?.code };
        }
        if ((sub.stopType ?? 'Porcentaje') === 'Porcentaje' && sub.sl) {
            const r = await ejecutarOrdenBinance(ctx,
                buildStopUrl(ctx, sub.lado, sub.qty, sub.sl, 'STOP_MARKET'), `SL-EXCH #${sub.id}`);
            const id = r.body?.algoId ?? r.body?.orderId;
            if (r.ok && id) sub.slOrderId = String(id);
            estado.sl = { ok: r.ok, msg: r.body?.msg, code: r.body?.code };
        }
        await pool.query(
            `UPDATE auto_trading_entradas SET tp_order_id=$1, sl_order_id=$2 WHERE id=$3`,
            [sub.tpOrderId || null, sub.slOrderId || null, sub.id]
        );
    } catch (e) {
        console.error(`[AutoTrading] Error colocando protección exchange #${sub.id}: ${e.message}`);
    }
    return estado;
}

// Cierra UNA sub-posición de la cuenta `ctx`. El llamador ya debe haberla removido del
// array de la cuenta (síncrono, antes de awaits) para evitar dobles cierres por ticks.
async function cerrarSubPosicion(ctx, pos, razon, precio) {
    const arr = posDe(ctx.uid);
    console.log(`[AutoTrading u${ctx.uid}] ${razon} sub-pos #${pos.id} ${pos.lado.toUpperCase()} qty ${pos.qty} @ $${precio.toFixed(1)}`);
    const ahoraMs = Date.now();
    await pool.query(
        `UPDATE auto_trading_entradas SET estado='cerrada', precio_cierre=$1, razon_cierre=$2, ts_cierre=$3 WHERE id=$4`,
        [precio, razon, ahoraMs, pos.id]
    );
    await pool.query(`UPDATE cuentas_trading SET ultima_cierre_ts=$1 WHERE usuario_id=$2`, [ahoraMs, ctx.uid]);
    // Al quedar plana, resetear la última señal para poder re-entrar en la próxima señal.
    if (arr.length === 0) {
        await pool.query(`UPDATE cuentas_trading SET ultima_senal=NULL WHERE usuario_id=$1`, [ctx.uid]);
    }
    await sincronizarPosicionBD(ctx.uid);
    // Cancelar las órdenes de protección de esta sub-posición ANTES del cierre a mercado,
    // para no dejar stops huérfanos. Si una ya se ejecutó, el cancel falla silenciosamente.
    // El cierre reduceOnly que sigue es benigno si el exchange ya cerró (Binance lo rechaza -2022).
    await cancelarOrden(ctx, pos.tpOrderId);
    await cancelarOrden(ctx, pos.slOrderId);
    await ejecutarOrdenBinance(ctx, buildCloseUrl(ctx, pos.lado, pos.qty), `CIERRE-${razon}`);
    // Barrido final al quedar plana: limpia cualquier stop residual del exchange.
    if (arr.length === 0) await cancelarTodasLasOrdenes(ctx);
}

// TP/SL por tick para TODAS las cuentas con posiciones. El SL fijo por tick solo aplica a
// stop por Porcentaje; el stop por Ruptura EMA es dinámico (lo gestiona el ciclo de 1 min).
async function chequearSalida(precio, entorno) {
    ultimoPrecioPorEntorno[entorno] = precio;
    for (const [uid, arr] of posicionesPorCuenta) {
        if (!arr.length) continue;
        const ctx = ctxActivos.get(uid);
        if (!ctx) continue; // sin claves en memoria no podemos cerrar; el exchange igual tiene el TP/SL
        if (entornoDeBase(ctx.base) !== entorno) continue; // este feed es del otro mercado: no aplica

        const aCerrar = [];
        for (let i = arr.length - 1; i >= 0; i--) {
            const pos = arr[i];
            const golpeTP   = pos.lado === 'long' ? precio >= pos.tp : precio <= pos.tp;
            const slPorTick = (pos.stopType ?? 'Porcentaje') === 'Porcentaje';
            const golpeSL   = slPorTick && (pos.lado === 'long' ? precio <= pos.sl : precio >= pos.sl);
            if (golpeTP || golpeSL) {
                arr.splice(i, 1); // remover síncronamente antes de awaits
                aCerrar.push({ pos, razon: golpeTP ? 'TP' : 'SL' });
            }
        }
        for (const { pos, razon } of aCerrar) await cerrarSubPosicion(ctx, pos, razon, precio);
    }
}

// Cada ciclo (1 min): salida por tiempo máximo y stop EMA dinámico, por sub-posición de la
// cuenta `ctx`. Usa los klines 1m compartidos del ciclo. Replica el backtest al cierre de vela.
async function gestionarPosicionAbierta(ctx, p, bars1m) {
    const arr = posDe(ctx.uid);
    if (arr.length === 0) return;
    const ahora = Date.now();

    const stopEMA = (p.stopType === 'Ruptura EMA 200' || p.stopType === 'Ruptura EMA 500');
    let precioActual = precioDeCtx(ctx), emaVal = null;
    if (stopEMA && bars1m && bars1m.length >= 510) {
        const c1m = bars1m.map(b => parseFloat(b[4]));
        const emaArr = calcEMA(c1m, p.stopType === 'Ruptura EMA 200' ? 200 : 500);
        const last = bars1m.length - 1;
        emaVal = emaArr[last];
        precioActual = c1m[last]; // decisión al cierre de vela, igual que el backtest
    }
    if (!precioActual && bars1m && bars1m.length) precioActual = parseFloat(bars1m[bars1m.length - 1][4]);
    if (!precioActual) return;

    const aCerrar = [];
    for (let i = arr.length - 1; i >= 0; i--) {
        const pos = arr[i];
        let razon = null;
        if (p.useMaxTradeTime && pos.entryTs && (ahora - pos.entryTs) / 60000 >= (p.maxTradeMinutes ?? 15)) {
            razon = 'Tiempo';
        } else if (stopEMA && emaVal != null) {
            const rompe = pos.lado === 'long' ? precioActual < emaVal : precioActual > emaVal;
            if (rompe) razon = 'EMA';
        }
        if (razon) {
            arr.splice(i, 1);
            aCerrar.push({ pos, razon });
        }
    }
    for (const { pos, razon } of aCerrar) await cerrarSubPosicion(ctx, pos, razon, precioActual);
}

// Devuelve el NOCIONAL (USDT) de la orden de entrada según el sizing de la estrategia, sobre
// el balance de la cuenta `ctx`. En el backtest posicionValor es el MARGEN; exposición = margen × palanca.
async function calcularNocionalEntrada(ctx, p, row) {
    const tipo    = p.posicionTipo  || 'porc_capital_actual';
    const valor   = p.posicionValor ?? 100;
    const palanca = (p.palancaActivo && p.palancaValor > 1) ? p.palancaValor : 1;

    let bal;
    try { bal = await balanceDeCuenta(ctx); }
    catch (e) { return { ok: false, motivo: 'no se pudo leer balance: ' + e.message }; }

    let margin;
    if (tipo === 'monto_fijo')                margin = valor;
    else if (tipo === 'porc_capital_inicial') margin = (parseFloat(row.capital_inicial_ref) || bal.wallet) * (valor / 100);
    else                                      margin = bal.wallet * (valor / 100); // porc_capital_actual

    if (!(margin > 0)) return { ok: false, motivo: 'monto calculado <= 0' };
    // availableBalance ya descuenta el margen de las sub-posiciones abiertas → cubre el
    // chequeo de no sobre-exponer el capital (equivalente a calcCapitalEntrada del backtest).
    if (margin > bal.disponible) return { ok: false, motivo: `margen ${margin.toFixed(2)} > disponible ${bal.disponible.toFixed(2)}` };

    return { ok: true, nocional: margin * palanca };
}

// WebSocket de precio futuros para monitoreo TP/SL en tiempo real. Un monitor por entorno
// (testnet y real), porque cada mercado tiene su propio precio y sus propias cuentas.
const monitorConectando = new Set(); // entornos con una conexión en curso (evita doble-connect)

function iniciarMonitorPrecio(wsUrl, entorno) {
    if (monitorConectando.has(entorno)) return;
    monitorConectando.add(entorno);

    const ws = new WebSocket(wsUrl);

    ws.on('open', () => {
        monitorConectando.delete(entorno);
        console.log(`✅ Monitor de precio futuros (${entorno}) conectado.`);
    });

    ws.on('message', async (data) => {
        try {
            const evento = JSON.parse(data);
            await chequearSalida(parseFloat(evento.p), entorno);
        } catch (e) {
            console.error(`Error en monitor de precio (${entorno}):`, e.message);
        }
    });

    ws.on('error', (err) => {
        console.error(`Error WebSocket precio (${entorno}):`, err.message);
        ws.terminate();
    });

    ws.on('close', () => {
        monitorConectando.delete(entorno);
        setTimeout(() => iniciarMonitorPrecio(wsUrl, entorno), 5000);
    });
}

// Log de estado al arrancar
setTimeout(() => {
    const modo = BINANCE_BASE.includes('testnet') ? '🧪 TESTNET (plata virtual)' : '🔴 REAL (plata de verdad)';
    console.log(`[AutoTrading] Entorno Binance (base por defecto): ${modo} — ${BINANCE_BASE}`);
    console.log(`[AutoTrading] ENCRYPTION_KEY: ${ENCRYPTION_KEY ? '✅ configurada' : '❌ FALTA — no se pueden guardar/usar claves de cuentas'}`);
    console.log(`[AutoTrading] N8N_WEBHOOK_URL: ${N8N_WEBHOOK_URL ? '✅ configurado (opcional)' : '➖ no configurado (opcional)'}`);
}, 3000);

async function ejecutarOrdenBinance(ctx, url, etiqueta) {
    try {
        const resp = await fetch(url, {
            method: 'POST',
            headers: { 'X-MBX-APIKEY': ctx.apiKey, 'Content-Type': 'application/x-www-form-urlencoded' },
        });
        const body = await resp.json();
        if (resp.ok) {
            console.log(`[AutoTrading u${ctx.uid}] ✅ Orden ${etiqueta} — orderId: ${body.orderId ?? body.algoId ?? '—'}`);
        } else {
            console.error(`[AutoTrading u${ctx.uid}] ❌ Orden ${etiqueta} rechazada — ${body.code}: ${body.msg}`);
        }
        return { ok: resp.ok, body };
    } catch (e) {
        console.error(`[AutoTrading u${ctx.uid}] ❌ Error red orden ${etiqueta}: ${e.message}`);
        return { ok: false, error: e.message };
    }
}

// Setea el apalancamiento real del símbolo para la cuenta `ctx` (perfil de riesgo del backtest).
async function setBinanceLeverage(ctx, leverage) {
    const ts = Date.now();
    const q  = `symbol=BTCUSDT&leverage=${Math.round(leverage)}&timestamp=${ts}`;
    const url = `${ctx.base}/fapi/v1/leverage?${q}&signature=${firmarParams(q, ctx.secret)}`;
    const r = await ejecutarOrdenBinance(ctx, url, `LEVERAGE-${Math.round(leverage)}x`);
    return r.ok;
}

// Asegura que la cuenta `ctx` esté en One-way (no Hedge) y con su margin type. Los códigos
// -4059 / -4046 significan "no hace falta cambiar" y se tratan como éxito.
async function asegurarConfiguracionCuenta(ctx) {
    // Modo One-way
    try {
        const ts  = Date.now();
        const q   = `dualSidePosition=false&timestamp=${ts}`;
        const url = `${ctx.base}/fapi/v1/positionSide/dual?${q}&signature=${firmarParams(q, ctx.secret)}`;
        const r   = await fetch(url, { method: 'POST', headers: { 'X-MBX-APIKEY': ctx.apiKey } });
        const b   = await r.json();
        if (r.ok || b.code === -4059) console.log(`[AutoTrading u${ctx.uid}] Modo de posición: One-way ✅`);
        else console.warn(`[AutoTrading u${ctx.uid}] ⚠️ No se pudo fijar One-way: ${b.code} ${b.msg}`);
    } catch (e) { console.error(`[AutoTrading u${ctx.uid}] Error modo de posición:`, e.message); }

    // Margin type
    try {
        const mt  = ctx.marginType || 'CROSSED';
        const ts  = Date.now();
        const q   = `symbol=BTCUSDT&marginType=${mt}&timestamp=${ts}`;
        const url = `${ctx.base}/fapi/v1/marginType?${q}&signature=${firmarParams(q, ctx.secret)}`;
        const r   = await fetch(url, { method: 'POST', headers: { 'X-MBX-APIKEY': ctx.apiKey } });
        const b   = await r.json();
        if (r.ok || b.code === -4046) console.log(`[AutoTrading u${ctx.uid}] Margin type: ${mt} ✅`);
        else console.warn(`[AutoTrading u${ctx.uid}] ⚠️ No se pudo fijar margin type: ${b.code} ${b.msg}`);
    } catch (e) { console.error(`[AutoTrading u${ctx.uid}] Error margin type:`, e.message); }
}

// Posición NETA real del símbolo en el exchange de la cuenta `ctx` (positionAmt con signo).
async function obtenerPosicionExchange(ctx) {
    const ts  = Date.now();
    const q   = `symbol=BTCUSDT&timestamp=${ts}&recvWindow=10000`;
    const url = `${ctx.base}/fapi/v2/positionRisk?${q}&signature=${firmarParams(q, ctx.secret)}`;
    const r    = await fetch(url, { headers: { 'X-MBX-APIKEY': ctx.apiKey } });
    const body = await r.json();
    if (!Array.isArray(body)) throw new Error(body && body.msg ? body.msg : 'positionRisk no disponible');
    const pos = body.find(x => x.symbol === 'BTCUSDT');
    return pos ? parseFloat(pos.positionAmt) : 0;
}

// Migración una-vez: pasa la config global antigua (auto_trading_config id=1, que usaba las
// claves del ENV) a una fila de cuentas_trading para su usuario, cifrando el secret del ENV.
// Así el bot que ya venía corriendo sigue operando como cuenta de ese usuario.
async function migrarConfigGlobal() {
    if (!ENCRYPTION_KEY || !BINANCE_API_KEY || !BINANCE_SECRET) return;
    const cfg = (await pool.query('SELECT * FROM auto_trading_config WHERE id=1')).rows[0];
    if (!cfg || !cfg.usuario_id) return;
    const existe = await pool.query('SELECT 1 FROM cuentas_trading WHERE usuario_id=$1', [cfg.usuario_id]);
    if (existe.rows.length) return; // ya migrado
    await pool.query(
        `INSERT INTO cuentas_trading
            (usuario_id, api_key, api_secret_cifrado, base_url, margin_type, estrategia_nombre,
             position_usdt, habilitado, posicion_lado, posicion_qty, posicion_entry, posicion_tp, posicion_sl,
             ultima_senal, ultima_senal_ts, ultima_cierre_ts, capital_inicial_ref)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`,
        [cfg.usuario_id, BINANCE_API_KEY, cifrarSecreto(BINANCE_SECRET), BINANCE_BASE,
         (process.env.BINANCE_MARGIN_TYPE || 'CROSSED').toUpperCase(), cfg.estrategia_nombre,
         cfg.position_usdt, cfg.habilitado, cfg.posicion_lado, cfg.posicion_qty, cfg.posicion_entry,
         cfg.posicion_tp, cfg.posicion_sl, cfg.ultima_senal, cfg.ultima_senal_ts, cfg.ultima_cierre_ts, cfg.capital_inicial_ref]
    );
    console.log(`[AutoTrading] Config global migrada a la cuenta del usuario ${cfg.usuario_id}.`);
}

let cicloCorriendo = false;
async function ejecutarAutoTrading() {
    if (!ENCRYPTION_KEY) return;        // sin master key no podemos descifrar las claves de las cuentas
    if (cicloCorriendo) return;         // evita solapamiento de ciclos (entradas duplicadas)
    cicloCorriendo = true;
    try {
        // Cuentas a procesar: habilitadas con estrategia + claves, MÁS las que tengan posiciones
        // abiertas aunque estén apagadas (para gestionarles las salidas).
        const habil = await pool.query(
            `SELECT * FROM cuentas_trading WHERE habilitado = true AND estrategia_nombre IS NOT NULL AND api_key IS NOT NULL`
        );
        const rowsByUid = new Map(habil.rows.map(r => [r.usuario_id, r]));
        for (const uid of posicionesPorCuenta.keys()) {
            if (posDe(uid).length && !rowsByUid.has(uid)) {
                const r = await pool.query('SELECT * FROM cuentas_trading WHERE usuario_id=$1', [uid]);
                if (r.rows.length && r.rows[0].api_key) rowsByUid.set(uid, r.rows[0]);
            }
        }
        if (rowsByUid.size === 0) return;

        // Datos de mercado COMPARTIDOS: se descargan una sola vez por ciclo (BTCUSDT es igual
        // para todas las cuentas). Velas suficientes para que EMA/MACD/RSI/ADX converjan.
        const [bars1m, bars5m, bars15m] = await Promise.all([
            fetchKlinesBatch('1m',  3000),
            fetchKlinesBatch('5m',  800),
            fetchKlinesBatch('15m', 800),
        ]);

        for (const [uid, row] of rowsByUid) {
            try { await procesarCuenta(row, bars1m, bars5m, bars15m); }
            catch (e) { console.error(`[AutoTrading u${uid}] Error procesando cuenta:`, e.message); }
        }
    } catch (e) {
        console.error('[AutoTrading] Error en loop:', e.message);
    } finally {
        cicloCorriendo = false;
    }
}

// Procesa UNA cuenta: gestiona salidas y evalúa/abre entrada según su estrategia, usando los
// datos de mercado compartidos del ciclo. Aislada por try/catch en el llamador.
async function procesarCuenta(row, bars1m, bars5m, bars15m) {
    let ctx;
    try { ctx = ctxDeCuenta(row); }
    catch (e) { console.error(`[AutoTrading u${row.usuario_id}] No se pudo descifrar la clave:`, e.message); return; }
    ctxActivos.set(row.usuario_id, ctx);

    const stratRes = await pool.query(
        'SELECT params FROM estrategias_guardadas WHERE nombre = $1 AND usuario_id = $2 LIMIT 1',
        [row.estrategia_nombre, row.usuario_id]
    );
    if (!stratRes.rows.length) return;
    const p   = stratRes.rows[0].params;
    const arr = posDe(row.usuario_id);

    // Gestionar salidas de sub-posiciones abiertas (tiempo / EMA). El WS cubre TP/SL fijos.
    if (arr.length > 0) await gestionarPosicionAbierta(ctx, p, bars1m);

    // Cuenta apagada: solo gestionar salidas, no abrir nuevas entradas.
    if (!row.habilitado) return;

    // ¿Se permite abrir entrada? Sin posiciones, o con pyramiding habilitado.
    if (arr.length > 0 && !p.allowMultipleEntries) return;

    const whaleRes = await pool.query(
        `SELECT EXTRACT(EPOCH FROM fecha) as ts_sec, cantidad, es_venta
         FROM ballenas WHERE fecha >= NOW() - make_interval(mins => $1) AND cantidad >= $2 ORDER BY fecha ASC`,
        [(parseInt(p.whaleWindow) || 30) + 5, p.whaleMinBTC || 5]
    );

    // Cooldown: no entrar si pasó menos del tiempo configurado desde el último cierre.
    if (p.useCooldown && row.ultima_cierre_ts) {
        const min = (Date.now() - parseInt(row.ultima_cierre_ts)) / 60000;
        if (min < (p.cooldownMinutes ?? 45)) {
            console.log(`[AutoTrading u${row.usuario_id}] Cooldown — faltan ${((p.cooldownMinutes ?? 45) - min).toFixed(1)} min`);
            return;
        }
    }

    const resultado  = evaluarSenal(bars1m, bars5m, bars15m, whaleRes.rows, p);
    const nuevaSenal = resultado.signal;

    // Filtro opcional: no apilar entradas mientras alguna sub-posición esté en pérdida.
    if (p.allowMultipleEntries && p.blockMultipleIfLosing && arr.length > 0) {
        const precioRef = resultado.entry || precioDeCtx(ctx);
        const enPerdida = precioRef && arr.some(pos =>
            (pos.lado === 'long' ? precioRef - pos.entry : pos.entry - precioRef) < 0);
        if (enPerdida) { console.log(`[AutoTrading u${row.usuario_id}] Entrada múltiple omitida — en pérdida`); return; }
    }

    // Dedup en modo una-sola-posición.
    if (!p.allowMultipleEntries && nuevaSenal === row.ultima_senal) return;

    await pool.query('UPDATE cuentas_trading SET ultima_senal=$1, ultima_senal_ts=$2 WHERE usuario_id=$3',
        [nuevaSenal, Date.now(), row.usuario_id]);

    if (!nuevaSenal) {
        if (arr.length === 0) console.log(`[AutoTrading u${row.usuario_id}] Sin señal`);
        return;
    }

    // Binance netea: si ya hay sub-posiciones, solo se apila del MISMO lado.
    if (arr.length > 0 && arr[0].lado !== nuevaSenal) {
        console.log(`[AutoTrading u${row.usuario_id}] Señal ${nuevaSenal} opuesta a ${arr[0].lado} — omitida`);
        return;
    }

    const sizing = await calcularNocionalEntrada(ctx, p, row);
    if (!sizing.ok) { console.log(`[AutoTrading u${row.usuario_id}] Entrada omitida — ${sizing.motivo}`); return; }
    const qty = Math.floor((sizing.nocional / resultado.entry) * 1000) / 1000;
    if (qty < 0.001) { console.log(`[AutoTrading u${row.usuario_id}] qty ${qty} < 0.001 BTC — omitida`); return; }

    console.log(`[AutoTrading u${row.usuario_id}] Nueva señal: ${nuevaSenal.toUpperCase()} @ $${resultado.entry} | TP $${resultado.tp?.toFixed(0)} | SL $${resultado.sl?.toFixed(0)} | qty ${qty} BTC`);

    if (p.palancaActivo && p.palancaValor > 1) await setBinanceLeverage(ctx, p.palancaValor);

    const ordenEntrada = await ejecutarOrdenBinance(ctx, buildEntryUrl(ctx, nuevaSenal, qty), 'ENTRADA');
    if (ordenEntrada.ok) {
        const stopType = p.stopType ?? 'Porcentaje';
        const ins = await pool.query(
            `INSERT INTO auto_trading_entradas (ts, lado, precio_entrada, precio_tp, precio_sl, qty, stop_type, estado, usuario_id, account_id)
             VALUES ($1, $2, $3, $4, $5, $6, $7, 'abierta', $8, $8) RETURNING id`,
            [Date.now(), nuevaSenal, resultado.entry, resultado.tp, resultado.sl, qty, stopType, row.usuario_id]
        );
        const sub = {
            id: ins.rows[0].id, lado: nuevaSenal, qty, entry: resultado.entry,
            tp: resultado.tp, sl: resultado.sl, entryTs: Date.now(), stopType,
        };
        arr.push(sub);
        await sincronizarPosicionBD(row.usuario_id);
        // Red de seguridad: TP/SL reales en el exchange aunque el server se caiga.
        await colocarProteccionExchange(ctx, sub);
        console.log(`[AutoTrading u${row.usuario_id}] Sub-posición #${ins.rows[0].id} abierta — abiertas: ${arr.length}`);
    }

    if (N8N_WEBHOOK_URL) {
        fetch(N8N_WEBHOOK_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                usuario_id: row.usuario_id, signal: nuevaSenal, entry: resultado.entry,
                tp: resultado.tp, sl: resultado.sl, qty, estrategia: row.estrategia_nombre,
                ordenOk: ordenEntrada.ok,
            }),
        }).catch(() => {});
    }
}

// Arrancar loop después de que la BD esté lista
setTimeout(async () => {
    // 0. Migrar la config global antigua a cuentas_trading (una sola vez), para no perder el
    //    bot que ya venía corriendo con las claves del ENV.
    try { await migrarConfigGlobal(); } catch (e) { console.error('[AutoTrading] Error migrando config global:', e.message); }

    // 1. Recuperar sub-posiciones abiertas agrupadas por cuenta (usuario_id).
    try {
        const openRows = await pool.query(
            `SELECT id, ts, lado, precio_entrada, precio_tp, precio_sl, qty, stop_type, tp_order_id, sl_order_id, usuario_id
             FROM auto_trading_entradas WHERE estado = 'abierta' ORDER BY ts ASC`
        );
        for (const row of openRows.rows) {
            const uid = row.usuario_id;
            if (!uid) { console.warn(`[AutoTrading] Entrada #${row.id} sin usuario_id — no se gestiona.`); continue; }
            const qty = parseFloat(row.qty);
            if (!(qty > 0)) { console.warn(`[AutoTrading] Sub-pos #${row.id} sin qty válida — revisar manualmente.`); continue; }
            posDe(uid).push({
                id: row.id, lado: row.lado, qty,
                entry: parseFloat(row.precio_entrada), tp: parseFloat(row.precio_tp), sl: parseFloat(row.precio_sl),
                entryTs: parseInt(row.ts), stopType: row.stop_type || 'Porcentaje',
                tpOrderId: row.tp_order_id || null, slOrderId: row.sl_order_id || null,
            });
        }

        // 2. Por cada cuenta con posiciones: ctx, reconciliar con el exchange y fijar el modo.
        for (const uid of posicionesPorCuenta.keys()) {
            const arr = posDe(uid);
            if (!arr.length) continue;
            const cr = await pool.query('SELECT * FROM cuentas_trading WHERE usuario_id=$1', [uid]);
            if (!cr.rows.length || !cr.rows[0].api_key) {
                console.warn(`[AutoTrading u${uid}] ${arr.length} posición(es) abiertas pero la cuenta no tiene claves — no se gestionarán.`);
                continue;
            }
            let ctx;
            try { ctx = ctxDeCuenta(cr.rows[0]); }
            catch (e) { console.error(`[AutoTrading u${uid}] No se pudo descifrar clave:`, e.message); continue; }
            ctxActivos.set(uid, ctx);
            await sincronizarPosicionBD(uid);
            console.log(`[AutoTrading u${uid}] ${arr.length} sub-posición(es) recuperada(s).`);

            // Reconciliar: si el exchange está plano, los stops cerraron la posición con el server caído.
            try {
                const amt = await obtenerPosicionExchange(ctx);
                if (Math.abs(amt) < 1e-8) {
                    console.warn(`[AutoTrading u${uid}] ⚠️ Exchange PLANO pero el libro tenía ${arr.length} — reconciliando (cerradas por el exchange).`);
                    const ahoraMs = Date.now();
                    for (const pos of arr) {
                        await pool.query(`UPDATE auto_trading_entradas SET estado='cerrada', razon_cierre='Exchange', ts_cierre=$1 WHERE id=$2`, [ahoraMs, pos.id]);
                    }
                    posicionesPorCuenta.set(uid, []);
                    await cancelarTodasLasOrdenes(ctx);
                    await pool.query(`UPDATE cuentas_trading SET ultima_senal=NULL, ultima_cierre_ts=$1 WHERE usuario_id=$2`, [ahoraMs, uid]);
                    await sincronizarPosicionBD(uid);
                } else {
                    const sumLibro = arr.reduce((s, p) => s + p.qty, 0) * (arr[0].lado === 'long' ? 1 : -1);
                    if (Math.abs(amt - sumLibro) > 0.0005) {
                        console.warn(`[AutoTrading u${uid}] ⚠️ Discrepancia: exchange ${amt} BTC vs libro ${sumLibro} BTC. Revisar manualmente.`);
                    }
                    // Recolocar protección a sub-posiciones legacy sin órdenes en el exchange.
                    for (const sub of arr) {
                        if (!sub.tpOrderId && !sub.slOrderId) await colocarProteccionExchange(ctx, sub);
                    }
                }
            } catch (e) { console.error(`[AutoTrading u${uid}] Error reconciliando:`, e.message); }

            try { await asegurarConfiguracionCuenta(ctx); } catch (_) {}
        }
    } catch (e) {
        console.error('[AutoTrading] Error cargando posiciones desde BD:', e.message);
    }

    iniciarMonitorPrecio(WS_PRECIO_POR_ENTORNO.testnet, 'testnet');
    iniciarMonitorPrecio(WS_PRECIO_POR_ENTORNO.real,    'real');
    ejecutarAutoTrading();
    setInterval(ejecutarAutoTrading, 60 * 1000);
}, 5000);

// ── Endpoints de Auto-Trading (per-usuario; alias de /api/mi-cuenta para la UI actual) ──
app.get('/api/autotrading', autenticar, async (req, res) => {
    try {
        const r = await pool.query(
            `SELECT habilitado, estrategia_nombre, position_usdt, (api_key IS NOT NULL) AS configurada
             FROM cuentas_trading WHERE usuario_id = $1`, [req.usuario.id]
        );
        res.json(r.rows[0] || { configurada: false, habilitado: false });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/autotrading', autenticar, async (req, res) => {
    const { habilitado, estrategia_nombre, position_usdt } = req.body;
    try {
        const cuenta = await pool.query(
            'SELECT api_key, api_secret_cifrado, base_url FROM cuentas_trading WHERE usuario_id = $1',
            [req.usuario.id]
        );
        if (!cuenta.rows.length || !cuenta.rows[0].api_key) {
            return res.status(400).json({ error: 'Primero cargá tus claves de Binance' });
        }
        if (habilitado === true && !estrategia_nombre) {
            return res.status(400).json({ error: 'Seleccioná una estrategia para encender el bot' });
        }
        if (estrategia_nombre) {
            const s = await pool.query('SELECT 1 FROM estrategias_guardadas WHERE nombre=$1 AND usuario_id=$2', [estrategia_nombre, req.usuario.id]);
            if (!s.rows.length) return res.status(400).json({ error: 'Estrategia no encontrada' });
        }
        await pool.query(
            `UPDATE cuentas_trading SET
                habilitado        = COALESCE($1, habilitado),
                estrategia_nombre = COALESCE($2, estrategia_nombre),
                position_usdt     = COALESCE($3, position_usdt)
             WHERE usuario_id = $4`,
            [
                habilitado !== undefined ? habilitado : null,
                estrategia_nombre !== undefined ? estrategia_nombre : null,
                position_usdt     !== undefined ? parseFloat(position_usdt) : null,
                req.usuario.id,
            ]
        );
        if (estrategia_nombre !== undefined || habilitado === true) {
            await pool.query('UPDATE cuentas_trading SET ultima_senal = NULL WHERE usuario_id = $1', [req.usuario.id]);
        }
        if (habilitado === true) {
            try {
                const c = cuenta.rows[0];
                const bal = await balanceDeCuenta({ apiKey: c.api_key, secret: descifrarSecreto(c.api_secret_cifrado), base: c.base_url });
                await pool.query('UPDATE cuentas_trading SET capital_inicial_ref = $1 WHERE usuario_id = $2', [bal.wallet, req.usuario.id]);
                console.log(`[AutoTrading u${req.usuario.id}] Capital inicial de referencia: ${bal.wallet} USDT`);
            } catch (e) {
                console.error(`[AutoTrading u${req.usuario.id}] No se pudo snapshotear capital inicial:`, e.message);
            }
        }
        res.json({ ok: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/autotrading/status', autenticar, async (req, res) => {
    try {
        const r = await pool.query(
            'SELECT habilitado, estrategia_nombre, ultima_senal, ultima_senal_ts, position_usdt FROM cuentas_trading WHERE usuario_id = $1',
            [req.usuario.id]
        );
        res.json(r.rows[0] || { habilitado: false });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/autotrading/entradas', autenticar, async (req, res) => {
    try {
        const r = await pool.query(
            `SELECT id, ts, lado, precio_entrada, precio_tp, precio_sl,
                    estado, precio_cierre, razon_cierre, ts_cierre
             FROM auto_trading_entradas
             WHERE usuario_id = $1
             ORDER BY ts DESC LIMIT 200`,
            [req.usuario.id]
        );
        res.json(r.rows);
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// Overview de admin: estado de las cuentas de todos los usuarios.
app.get('/api/admin/autotrading', autenticar, soloAdmin, async (req, res) => {
    try {
        const r = await pool.query(
            `SELECT c.usuario_id, u.username, c.habilitado, c.estrategia_nombre, c.position_usdt,
                    c.ultima_senal, c.posicion_lado, c.posicion_qty, (c.api_key IS NOT NULL) AS tiene_claves
             FROM cuentas_trading c JOIN usuarios u ON u.id = c.usuario_id
             ORDER BY u.username ASC`
        );
        res.json(r.rows);
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// Endpoint de test: ejecuta una orden real en la cuenta (testnet) del propio usuario.
// Con `prueba:true` arma una entrada de verificación: usa el precio actual de mercado,
// TP/SL bien cortos (±0.15%) para que se gatille rápido, y una qty mínima que respeta
// el notional mínimo de Binance. Sirve para validar de punta a punta (incluidas las
// órdenes de protección Algo) sin tener que esperar una señal real.
app.post('/api/autotrading/test', autenticar, async (req, res) => {
    const signal       = req.body.signal                    || 'long';
    const positionUsdt = parseFloat(req.body.positionUsdt)  || 100;
    let entry          = parseFloat(req.body.entry)         || 95000;
    let tp             = parseFloat(req.body.tp)            || 95475;
    let sl             = parseFloat(req.body.sl)            || 94050;

    try {
        const cr = await pool.query('SELECT * FROM cuentas_trading WHERE usuario_id = $1', [req.usuario.id]);
        if (!cr.rows.length || !cr.rows[0].api_key) return res.status(400).json({ error: 'Cargá tus claves de Binance primero' });
        let ctx;
        try { ctx = ctxDeCuenta(cr.rows[0]); }
        catch (e) { return res.status(500).json({ error: 'No se pudo descifrar la clave: ' + e.message }); }
        ctxActivos.set(req.usuario.id, ctx);

        if (req.body.prueba) {
            // Precio de referencia del mercado de ESTA cuenta (testnet o real), no de un feed global.
            const ref = precioDeCtx(ctx);
            if (!ref) return res.status(503).json({ error: 'Aún no hay precio de mercado en vivo para tu entorno; probá de nuevo en unos segundos.' });
            const pct = 0.0015; // ±0.15% → toca TP/SL en pocos minutos sin gatillarse al instante
            entry = ref;
            tp = signal === 'long' ? ref * (1 + pct) : ref * (1 - pct);
            sl = signal === 'long' ? ref * (1 - pct) : ref * (1 + pct);
        }

        let qty = Math.floor((positionUsdt / entry) * 1000) / 1000;
        // BTCUSDT perp exige ~100 USDT de notional mínimo; redondeamos hacia arriba al
        // step de 0.001 BTC para no caer en -4164 (min notional) en la prueba.
        if (req.body.prueba) {
            const minQty = Math.ceil((120 / entry) * 1000) / 1000;
            if (qty < minQty) qty = minQty;
        }

        const resEntrada = await ejecutarOrdenBinance(ctx, buildEntryUrl(ctx, signal, qty), 'ENTRADA-TEST');
        let proteccion = null;
        if (resEntrada.ok) {
            const ins = await pool.query(
                `INSERT INTO auto_trading_entradas (ts, lado, precio_entrada, precio_tp, precio_sl, qty, stop_type, estado, usuario_id, account_id)
                 VALUES ($1, $2, $3, $4, $5, $6, 'Porcentaje', 'abierta', $7, $7) RETURNING id`,
                [Date.now(), signal, entry, tp, sl, qty, req.usuario.id]
            );
            const sub = { id: ins.rows[0].id, lado: signal, qty, entry, tp, sl, entryTs: Date.now(), stopType: 'Porcentaje' };
            posDe(req.usuario.id).push(sub);
            await sincronizarPosicionBD(req.usuario.id);
            proteccion = await colocarProteccionExchange(ctx, sub);
        }

        res.json({
            entrada: { ok: resEntrada.ok, orderId: resEntrada.body?.orderId, msg: resEntrada.body?.msg, code: resEntrada.body?.code },
            proteccion,
            qty, signal, entry, tp, sl,
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

    // Pullback EMAs configurables
    const pbEMAConfig = (Array.isArray(p.pullbackEMAs) && p.pullbackEMAs.length > 0)
        ? p.pullbackEMAs
        : [{ period:50,tf:'1m' },{ period:100,tf:'1m' },{ period:200,tf:'1m' },{ period:500,tf:'1m' }];
    const pbSn1m = {}, pbSn5m = {}, pbSn15m = {};
    const c5m_sn  = bars5m.map(b => parseFloat(b[4]));
    const c15m_sn = bars15m.map(b => parseFloat(b[4]));
    for (const { period, tf } of pbEMAConfig) {
        if (tf === '1m' && !pbSn1m[period]) {
            pbSn1m[period] = calcEMA(c1m, period);
        } else if (tf === '5m' && !pbSn5m[period]) {
            const vals = calcEMA(c5m_sn, period);
            pbSn5m[period] = {
                ts:  bars5m.map(b => parseInt(b[6])).sort((a, b) => a - b),
                map: new Map(bars5m.map((b, idx) => [parseInt(b[6]), vals[idx]]))
            };
        } else if (tf === '15m' && !pbSn15m[period]) {
            const vals = calcEMA(c15m_sn, period);
            pbSn15m[period] = {
                ts:  bars15m.map(b => parseInt(b[6])).sort((a, b) => a - b),
                map: new Map(bars15m.map((b, idx) => [parseInt(b[6]), vals[idx]]))
            };
        }
    }

    // RSI — configurable período, temporalidad y umbrales de entrada
    const rsiPeriod_sn   = p.rsiPeriod   || 14;
    const rsiTf_sn       = p.rsiTf       || '15m';
    const rsiLongMin_sn  = p.rsiLongMin  ?? 60;
    const rsiShortMax_sn = p.rsiShortMax ?? 40;
    const useRsiFilter_sn = p.useRsiFilter !== false;   // default ON (compat. estrategias previas)
    let rsiSnByTs = null, rsiSnTs = null, rsiSnDirect = null;
    if (rsiTf_sn === '1m') {
        rsiSnDirect = calcRSI(c1m, rsiPeriod_sn);
    } else if (rsiTf_sn === '5m') {
        const arr = calcRSI(c5m_sn, rsiPeriod_sn);
        rsiSnByTs = new Map(bars5m.map((b, i) => [parseInt(b[6]), arr[i]]));
        rsiSnTs   = [...rsiSnByTs.keys()].sort((a, b) => a - b);
    } else {
        const arr = calcRSI(c15m_sn, rsiPeriod_sn);
        rsiSnByTs = new Map(bars15m.map((b, i) => [parseInt(b[6]), arr[i]]));
        rsiSnTs   = [...rsiSnByTs.keys()].sort((a, b) => a - b);
    }

    const adxArr15m = calcADX(bars15m.map(b => parseFloat(b[2])), bars15m.map(b => parseFloat(b[3])), c15m_sn);
    const adxByTs15m = new Map(bars15m.map((b, i) => [parseInt(b[6]), adxArr15m[i]]));
    const adxTs15m   = [...adxByTs15m.keys()].sort((a, b) => a - b);
    const adxValue   = lookupHTF(adxTs15m, adxByTs15m, parseInt(bars1m[bars1m.length - 1][6]));

    // VWAP — configurable timeframe y sesión
    const vwapTf_sn      = p.vwapTf      || '5m';
    const vwapSession_sn = p.vwapSession || 'daily';
    let vwapDirect_sn = null, vwapByTs_sn = null, tsVwap_sn = null;
    if (p.useVwapFilter) {
        if (vwapTf_sn === '1m') {
            vwapDirect_sn = calcVWAP(bars1m, vwapSession_sn);
        } else if (vwapTf_sn === '5m') {
            const vals = calcVWAP(bars5m, vwapSession_sn);
            vwapByTs_sn = new Map(bars5m.map((b, idx) => [parseInt(b[6]), vals[idx]]));
            tsVwap_sn   = [...vwapByTs_sn.keys()].sort((a, b) => a - b);
        } else {
            const vals = calcVWAP(bars15m, vwapSession_sn);
            vwapByTs_sn = new Map(bars15m.map((b, idx) => [parseInt(b[6]), vals[idx]]));
            tsVwap_sn   = [...vwapByTs_sn.keys()].sort((a, b) => a - b);
        }
    }

    // MACD — configurable período (fast/slow/signal) y temporalidad
    const macdFast_sn   = p.macdFast   || 12;
    const macdSlow_sn   = p.macdSlow   || 26;
    const macdSignal_sn = p.macdSignal || 9;
    const macdTf_sn     = p.macdTf     || '5m';
    const useMacdFilter_sn = p.useMacdFilter !== false;  // default ON (compat. estrategias previas)
    let macdSnDirect = null, macdSnByTs = null, macdSnTs = null;
    if (macdTf_sn === '1m') {
        const { macd: mArr, signal: sArr } = calcMACDArr(c1m, macdFast_sn, macdSlow_sn, macdSignal_sn);
        macdSnDirect = mArr.map((m, idx) => ({ macd: m, sig: sArr[idx] }));
    } else if (macdTf_sn === '5m') {
        const { macd: mArr, signal: sArr } = calcMACDArr(c5m_sn, macdFast_sn, macdSlow_sn, macdSignal_sn);
        macdSnByTs = new Map(bars5m.map((b, i) => [parseInt(b[6]), { macd: mArr[i], sig: sArr[i] }]));
        macdSnTs   = [...macdSnByTs.keys()].sort((a, b) => a - b);
    } else {
        const { macd: mArr, signal: sArr } = calcMACDArr(c15m_sn, macdFast_sn, macdSlow_sn, macdSignal_sn);
        macdSnByTs = new Map(bars15m.map((b, i) => [parseInt(b[6]), { macd: mArr[i], sig: sArr[i] }]));
        macdSnTs   = [...macdSnByTs.keys()].sort((a, b) => a - b);
    }

    const i     = bars1m.length - 1;
    const bar   = bars1m[i];
    const ts    = parseInt(bar[0]);
    const tsClose = parseInt(bar[6]); // instante real de la decisión (cierre de la vela 1m)
    const close = parseFloat(bar[4]);

    const E50 = e50[i], E100 = e100[i], E200 = e200[i], E500 = e500[i];

    const rsiLookup  = rsiTf_sn  === '1m' ? rsiSnDirect[i]  : lookupHTF(rsiSnTs,  rsiSnByTs,  tsClose);
    const macdLookup = macdTf_sn === '1m' ? macdSnDirect[i] : lookupHTF(macdSnTs, macdSnByTs, tsClose);
    if (!macdLookup || macdLookup.macd == null || macdLookup.sig == null)
        return { signal: null, reason: 'macd_no_calentado', indicadores: {} };
    // Coherencia con el backtest: si el RSI no está calentado, no se evalúa señal
    // (el backtest saltea esa vela en vez de asumir un valor neutro de 50).
    if (rsiLookup == null)
        return { signal: null, reason: 'rsi_no_calentado', indicadores: {} };
    const rsiVal = rsiLookup;
    const macd5 = macdLookup.macd;
    const sig5  = macdLookup.sig;

    const barHour  = new Date(ts).getUTCHours();
    const argDay   = new Date(ts - 3 * 3600000).getUTCDay(); // 0=Dom, 6=Sáb en horario Argentina
    const horarioOk = barHour >= (p.startHour ?? 9) && barHour < (p.endHour ?? 20)
                   && (p.operaFinDeSemana || (argDay !== 0 && argDay !== 6));

    const above     = close > E50 && close > E100 && close > E200 && close > E500;
    const below     = close < E50 && close < E100 && close < E200 && close < E500;
    const bullAlign = E50 > E100 && E100 > E200 && E200 > E500;
    const bearAlign = E50 < E100 && E100 < E200 && E200 < E500;

    const pbSnVals = pbEMAConfig.map(({ period, tf }) => {
        if (tf === '1m')  return pbSn1m[period]?.[i];
        if (tf === '5m')  return pbSn5m[period]  ? lookupHTF(pbSn5m[period].ts,  pbSn5m[period].map,  tsClose) : null;
        if (tf === '15m') return pbSn15m[period] ? lookupHTF(pbSn15m[period].ts, pbSn15m[period].map, tsClose) : null;
        return null;
    }).filter(v => v != null && v > 0);
    const nearEMA = !p.usePullbackFilter || (pbSnVals.length > 0 && pbSnVals.some(e =>
        Math.abs(close - e) / close * 100 <= (p.pullbackPerc ?? 0.2)
    ));

    const deltaSlice   = bars1m.slice(-(p.deltaVelas ?? 3));
    const deltaRolling = deltaSlice.reduce((s, b) => {
        const totalVol = parseFloat(b[5]);
        const buyVol   = parseFloat(b[9]);
        return s + (2 * buyVol - totalVol);
    }, 0);
    const deltaOkLong  = !p.useDeltaFilter || deltaRolling > 0;
    const deltaOkShort = !p.useDeltaFilter || deltaRolling < 0;

    // Ventana de ballenas anclada al CIERRE de la última vela 1m (tsClose), igual que el sliding
    // window del backtest [tsClose - windowMs, tsClose]. Antes se anclaba a Date.now(), así que si
    // el poll caía lejos del cierre de vela la ventana no coincidía con la del backtest y la señal
    // divergía (el backtest abría con ballenas que el vivo ya no contaba, o viceversa).
    const windowMs = (p.whaleWindow ?? 30) * 60000;
    const whaleDelta = whalesArr
        .filter(w => { const t = parseFloat(w.ts_sec) * 1000; return t >= tsClose - windowMs && t <= tsClose; })
        .reduce((s, w) => s + (w.es_venta ? -parseFloat(w.cantidad) : parseFloat(w.cantidad)), 0);
    const whaleOkLong  = !p.useWhaleFilter || whaleDelta > 0;
    const whaleOkShort = !p.useWhaleFilter || whaleDelta < 0;

    const alignLong  = !p.useEmaAlignment || bullAlign;
    const alignShort = !p.useEmaAlignment || bearAlign;

    const adxOk = !p.useADXFilter || (adxValue !== null && adxValue >= (p.adxThreshold ?? 25));

    // VWAP — dirección y/o pullback
    const vwapVal_sn = !p.useVwapFilter ? null
        : vwapTf_sn === '1m' ? vwapDirect_sn[i] : lookupHTF(tsVwap_sn, vwapByTs_sn, tsClose);
    const vwapOkLong_sn  = !p.useVwapFilter || (vwapVal_sn !== null &&
        (!p.vwapUseDirection || close > vwapVal_sn) &&
        (!p.vwapUsePullback  || Math.abs(close - vwapVal_sn) / close * 100 <= (p.vwapPullbackPerc ?? 0.3))
    );
    const vwapOkShort_sn = !p.useVwapFilter || (vwapVal_sn !== null &&
        (!p.vwapUseDirection || close < vwapVal_sn) &&
        (!p.vwapUsePullback  || Math.abs(close - vwapVal_sn) / close * 100 <= (p.vwapPullbackPerc ?? 0.3))
    );

    let signal = null;
    if (p.enableLongs !== false && horarioOk && above && alignLong && (!useRsiFilter_sn || rsiVal >= rsiLongMin_sn) && (!useMacdFilter_sn || macd5 > sig5) && nearEMA && deltaOkLong && whaleOkLong && adxOk && vwapOkLong_sn)
        signal = 'long';
    else if (p.enableShorts !== false && horarioOk && below && alignShort && (!useRsiFilter_sn || rsiVal <= rsiShortMax_sn) && (!useMacdFilter_sn || macd5 < sig5) && nearEMA && deltaOkShort && whaleOkShort && adxOk && vwapOkShort_sn)
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
        indicadores: { rsi15: rsiVal, rsiTf: rsiTf_sn, macd5, macdTf: macdTf_sn, adx: adxValue, vwap: vwapVal_sn, vwapTf: vwapTf_sn, horarioOk, above, below, nearEMA, deltaRolling, whaleDelta, E50, E100, E200, E500 }
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
            // Suficientes velas para que EMA/MACD/RSI/ADX (incluso de período alto en HTF)
            // converjan igual que en el backtest y no diverja la señal en vivo.
            fetchKlinesBatch('1m',  3000),
            fetchKlinesBatch('5m',  800),
            fetchKlinesBatch('15m', 800),
            pool.query(
                `SELECT EXTRACT(EPOCH FROM fecha) as ts_sec, cantidad, es_venta
                 FROM ballenas WHERE fecha >= NOW() - make_interval(mins => $1) AND cantidad >= $2 ORDER BY fecha ASC`,
                [(parseInt(p.whaleWindow) || 30) + 5, p.whaleMinBTC || 5]
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

// ============================================================
// CUENTA DE TRADING (multi-cuenta: una cuenta Binance por usuario)
// ============================================================

// Config de la cuenta del usuario (nunca expone el secret; la api_key va enmascarada).
app.get('/api/mi-cuenta', autenticar, async (req, res) => {
    try {
        const r = await pool.query(
            `SELECT api_key, base_url, margin_type, estrategia_nombre, position_usdt, habilitado
             FROM cuentas_trading WHERE usuario_id = $1`,
            [req.usuario.id]
        );
        if (!r.rows.length) return res.json({ configurada: false });
        const c = r.rows[0];
        res.json({
            configurada:       !!c.api_key,
            api_key_mascara:   enmascararClave(c.api_key),
            base_url:          c.base_url,
            entorno:           String(c.base_url).includes('testnet') ? 'testnet' : 'real',
            margin_type:       c.margin_type,
            estrategia_nombre: c.estrategia_nombre,
            position_usdt:     c.position_usdt,
            habilitado:        c.habilitado,
        });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// Guarda/actualiza las claves Binance. El secret se cifra. Valida contra Binance antes de guardar.
app.put('/api/mi-cuenta', autenticar, async (req, res) => {
    const apiKey    = (req.body.api_key    || '').trim();
    const apiSecret = (req.body.api_secret || '').trim();
    if (!apiKey || !apiSecret) return res.status(400).json({ error: 'api_key y api_secret requeridos' });
    if (!ENCRYPTION_KEY) return res.status(503).json({ error: 'El servidor no tiene ENCRYPTION_KEY configurada; no se pueden guardar claves de forma segura' });

    // Entorno elegido por el usuario. 'real' opera con plata de verdad; cualquier valor no
    // reconocido cae en testnet por seguridad (nunca asumir real sin que lo pidan explícito).
    const entorno = String(req.body.entorno || 'testnet').toLowerCase() === 'real' ? 'real' : 'testnet';
    const base = entorno === 'real' ? 'https://fapi.binance.com' : 'https://testnet.binancefuture.com';
    try {
        // Verificar las claves contra Binance antes de persistir (evita guardar claves rotas).
        // Se valida contra la base del entorno elegido: una clave de testnet no sirve en real.
        let balance;
        try { balance = await balanceDeCuenta({ apiKey, secret: apiSecret, base }); }
        catch (e) { return res.status(400).json({ error: 'Las claves no pasaron la verificación con Binance: ' + e.message }); }

        const secretCifrado = cifrarSecreto(apiSecret);
        await pool.query(
            `INSERT INTO cuentas_trading (usuario_id, api_key, api_secret_cifrado, base_url)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (usuario_id) DO UPDATE
                SET api_key = EXCLUDED.api_key,
                    api_secret_cifrado = EXCLUDED.api_secret_cifrado,
                    base_url = EXCLUDED.base_url`,
            [req.usuario.id, apiKey, secretCifrado, base]
        );
        res.json({ ok: true, entorno, balance_usdt: balance.wallet });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// Desvincula la cuenta (borra las claves). Bloqueado si el bot está encendido.
app.delete('/api/mi-cuenta', autenticar, async (req, res) => {
    try {
        const r = await pool.query('SELECT habilitado FROM cuentas_trading WHERE usuario_id = $1', [req.usuario.id]);
        if (r.rows.length && r.rows[0].habilitado) {
            return res.status(409).json({ error: 'Apagá el auto-trading antes de desvincular la cuenta' });
        }
        await pool.query('DELETE FROM cuentas_trading WHERE usuario_id = $1', [req.usuario.id]);
        res.json({ ok: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// Configura estrategia + monto + encendido del bot para la cuenta del usuario.
app.put('/api/mi-cuenta/autotrading', autenticar, async (req, res) => {
    const { habilitado, estrategia_nombre, position_usdt } = req.body;
    try {
        const cuenta = await pool.query(
            'SELECT api_key, api_secret_cifrado, base_url FROM cuentas_trading WHERE usuario_id = $1',
            [req.usuario.id]
        );
        if (!cuenta.rows.length || !cuenta.rows[0].api_key) {
            return res.status(400).json({ error: 'Primero cargá tus claves de Binance' });
        }
        if (habilitado === true && !estrategia_nombre) {
            return res.status(400).json({ error: 'Seleccioná una estrategia para encender el bot' });
        }
        if (estrategia_nombre) {
            const s = await pool.query(
                'SELECT 1 FROM estrategias_guardadas WHERE nombre = $1 AND usuario_id = $2',
                [estrategia_nombre, req.usuario.id]
            );
            if (!s.rows.length) return res.status(400).json({ error: 'Estrategia no encontrada' });
        }
        await pool.query(
            `UPDATE cuentas_trading SET
                habilitado        = COALESCE($1, habilitado),
                estrategia_nombre = COALESCE($2, estrategia_nombre),
                position_usdt     = COALESCE($3, position_usdt)
             WHERE usuario_id = $4`,
            [
                habilitado !== undefined ? habilitado : null,
                estrategia_nombre !== undefined ? estrategia_nombre : null,
                position_usdt !== undefined ? parseFloat(position_usdt) : null,
                req.usuario.id,
            ]
        );
        // Reset de la última señal al cambiar config / encender, para re-evaluar limpio.
        if (estrategia_nombre !== undefined || habilitado === true) {
            await pool.query('UPDATE cuentas_trading SET ultima_senal = NULL WHERE usuario_id = $1', [req.usuario.id]);
        }
        // Snapshot del capital inicial al encender (base para sizing "% capital inicial").
        if (habilitado === true) {
            try {
                const c = cuenta.rows[0];
                const bal = await balanceDeCuenta({ apiKey: c.api_key, secret: descifrarSecreto(c.api_secret_cifrado), base: c.base_url });
                await pool.query('UPDATE cuentas_trading SET capital_inicial_ref = $1 WHERE usuario_id = $2', [bal.wallet, req.usuario.id]);
            } catch (e) {
                console.error(`[AutoTrading] No se pudo snapshotear capital de usuario ${req.usuario.id}:`, e.message);
            }
        }
        res.json({ ok: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// Estado liviano de la cuenta del usuario (para el indicador en la UI).
app.get('/api/mi-cuenta/status', autenticar, async (req, res) => {
    try {
        const r = await pool.query(
            `SELECT habilitado, estrategia_nombre, ultima_senal, ultima_senal_ts, position_usdt,
                    posicion_lado, posicion_qty, posicion_entry
             FROM cuentas_trading WHERE usuario_id = $1`,
            [req.usuario.id]
        );
        res.json(r.rows[0] || { configurada: false, habilitado: false });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// Historial de entradas de la cuenta del usuario.
app.get('/api/mi-cuenta/entradas', autenticar, async (req, res) => {
    try {
        const r = await pool.query(
            `SELECT id, ts, lado, precio_entrada, precio_tp, precio_sl,
                    estado, precio_cierre, razon_cierre, ts_cierre
             FROM auto_trading_entradas WHERE usuario_id = $1 ORDER BY ts DESC LIMIT 200`,
            [req.usuario.id]
        );
        res.json(r.rows);
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/backtest', autenticar, async (req, res) => {
    try {
        const p = {
            enableLongs:       req.body.enableLongs !== false,
            enableShorts:      req.body.enableShorts !== false,
            tpPerc:            parseFloat(req.body.tpPerc)  || 0.5,
            stopType:          req.body.stopType || 'Porcentaje',
            slPerc:            parseFloat(req.body.slPerc)  || 1.0,
            startHour:         Number.isFinite(parseInt(req.body.startHour)) ? parseInt(req.body.startHour) : 9,
            endHour:           Number.isFinite(parseInt(req.body.endHour))   ? parseInt(req.body.endHour)   : 20,
            usePullbackFilter: req.body.usePullbackFilter !== false,
            pullbackPerc:      parseFloat(req.body.pullbackPerc) || 0.20,
            pullbackEMAs:      Array.isArray(req.body.pullbackEMAs)
                                   ? req.body.pullbackEMAs.map(e => ({
                                       period: parseInt(e.period) || 200,
                                       tf:     ['1m','5m','15m'].includes(e.tf) ? e.tf : '1m'
                                   }))
                                   : [{ period:50,tf:'1m' },{ period:100,tf:'1m' },{ period:200,tf:'1m' },{ period:500,tf:'1m' }],
            useRsiFilter:      req.body.useRsiFilter !== false,
            rsiTf:             ['1m','5m','15m'].includes(req.body.rsiTf) ? req.body.rsiTf : '15m',
            rsiPeriod:         parseInt(req.body.rsiPeriod) || 14,
            rsiLongMin:        req.body.rsiLongMin  != null ? parseFloat(req.body.rsiLongMin)  : 60,
            rsiShortMax:       req.body.rsiShortMax != null ? parseFloat(req.body.rsiShortMax) : 40,
            useMacdFilter:     req.body.useMacdFilter !== false,
            macdTf:            ['1m','5m','15m'].includes(req.body.macdTf) ? req.body.macdTf : '5m',
            macdFast:          parseInt(req.body.macdFast)   || 12,
            macdSlow:          parseInt(req.body.macdSlow)   || 26,
            macdSignal:        parseInt(req.body.macdSignal) || 9,
            useEmaAlignment:   req.body.useEmaAlignment !== false,
            useMaxTradeTime:   req.body.useMaxTradeTime !== false,
            maxTradeMinutes:   parseInt(req.body.maxTradeMinutes) || 15,
            useCooldown:       req.body.useCooldown !== false,
            cooldownMinutes:   parseInt(req.body.cooldownMinutes) || 45,
            operaFinDeSemana:  req.body.operaFinDeSemana === true,
            useDeltaFilter:    req.body.useDeltaFilter === true,
            deltaVelas:        parseInt(req.body.deltaVelas) || 3,
            useWhaleFilter:    req.body.useWhaleFilter === true,
            whaleWindow:       parseInt(req.body.whaleWindow) || 30,
            whaleMinBTC:       parseFloat(req.body.whaleMinBTC) || 5,
            useADXFilter:        req.body.useADXFilter === true,
            adxThreshold:        parseInt(req.body.adxThreshold) || 25,
            useVwapFilter:       req.body.useVwapFilter === true,
            vwapTf:              ['1m','5m','15m'].includes(req.body.vwapTf) ? req.body.vwapTf : '5m',
            vwapSession:         ['daily','weekly','monthly'].includes(req.body.vwapSession) ? req.body.vwapSession : 'daily',
            vwapUseDirection:    req.body.vwapUseDirection === true,
            vwapUsePullback:     req.body.vwapUsePullback === true,
            vwapPullbackPerc:    parseFloat(req.body.vwapPullbackPerc) || 0.30,
            allowMultipleEntries: req.body.allowMultipleEntries === true,
            blockMultipleIfLosing: req.body.blockMultipleIfLosing === true,
            posicionTipo:         req.body.posicionTipo || 'porc_capital_actual',
            posicionValor:        parseFloat(req.body.posicionValor) || 100,
            palancaActivo:        req.body.palancaActivo === true,
            palancaValor:         parseFloat(req.body.palancaValor) || 1,
            commission:           Number.isFinite(parseFloat(req.body.commission))   ? parseFloat(req.body.commission)   : 0.04,
            slippagePerc:         Number.isFinite(parseFloat(req.body.slippagePerc))  ? parseFloat(req.body.slippagePerc)  : 0.02,
            fundingPerc:          Number.isFinite(parseFloat(req.body.fundingPerc))   ? parseFloat(req.body.fundingPerc)   : 0.01,
            initialCapital:       parseFloat(req.body.initialCapital) || 1000,
        };
        const days = Math.min(Math.max(parseInt(req.body.lookbackDays) || 7, 1), 365);
        const periodStart = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
        // Fuente de velas: 'bd' (cache local, default) o 'binance' (descarga en vivo).
        // El toggle en /estrategias permite comparar ambas para validar que coinciden.
        const fuente = req.body.fuenteDatos === 'binance' ? 'binance' : 'bd';
        const cargarKlines = fuente === 'binance'
            ? (tf, n) => fetchKlinesBatch(tf, n)
            : (tf)    => fetchKlinesDesdeBD(tf, days);
        const [bars1m, bars5m, bars15m, whaleRes] = await Promise.all([
            cargarKlines('1m',  days * 1440),
            cargarKlines('5m',  days * 288),
            cargarKlines('15m', days * 96),
            pool.query(
                `SELECT EXTRACT(EPOCH FROM fecha) as ts_sec, cantidad, es_venta
                 FROM ballenas WHERE fecha >= $1 AND cantidad >= $2 ORDER BY fecha ASC`,
                [periodStart.toISOString(), p.whaleMinBTC]
            ),
        ]);
        if (fuente === 'bd' && bars1m.length === 0) {
            throw new Error('La BD todavía no tiene velas cacheadas (el backfill inicial puede tardar unos minutos). Probá de nuevo en un rato o cambiá la fuente a Binance.');
        }
        const resultado = runBacktest(bars1m, bars5m, bars15m, whaleRes.rows, p);
        resultado.fuenteDatos = fuente;
        resultado.barsUsadas = { m1: bars1m.length, m5: bars5m.length, m15: bars15m.length };

        // ── Advertencias de calidad de datos que afectan la validez del backtest ──
        const warnings = [];
        if (p.useWhaleFilter) {
            const covRes = await pool.query(
                'SELECT MIN(fecha) AS primera FROM ballenas WHERE cantidad >= $1',
                [p.whaleMinBTC]
            );
            const primera = covRes.rows[0] && covRes.rows[0].primera ? new Date(covRes.rows[0].primera) : null;
            if (!primera) {
                warnings.push('Filtro de Ballenas activo pero no hay datos de ballenas guardados para ese mínimo de BTC: ningún trade pasará el filtro.');
            } else if (primera.getTime() > periodStart.getTime()) {
                const diasCubiertos = Math.max(0, (Date.now() - primera.getTime()) / 86400000);
                warnings.push(`Filtro de Ballenas activo: solo hay datos desde ${primera.toISOString().slice(0, 16).replace('T', ' ')} UTC (~${diasCubiertos.toFixed(1)} días). El tramo anterior del período NO genera trades; las métricas reflejan solo el subperíodo con cobertura de ballenas.`);
            }
            if (p.whaleMinBTC < limiteGuardadoBD) {
                warnings.push(`El "Mínimo BTC" del filtro (${p.whaleMinBTC}) es menor que el umbral de guardado en BD (${limiteGuardadoBD} BTC): solo existen trades ≥ ${limiteGuardadoBD} BTC, por lo que el filtro corre con datos incompletos.`);
            }
        }
        resultado.warnings = warnings;

        res.json(resultado);
    } catch (err) {
        console.error('Error backtest:', err);
        res.status(500).json({ error: err.message || 'Error al ejecutar backtest' });
    }
});

// Estado de la cache de velas: cobertura y si el backfill sigue corriendo.
app.get('/api/klines/estado', autenticar, async (req, res) => {
    try {
        const r = await pool.query(
            'SELECT COUNT(*)::int AS filas, MIN(open_time) AS primera, MAX(open_time) AS ultima FROM klines_1m'
        );
        const row = r.rows[0];
        const diasCobertura = row.primera
            ? (Number(row.ultima) - Number(row.primera)) / 86400000
            : 0;
        res.json({
            filas: row.filas,
            primera: row.primera ? Number(row.primera) : null,
            ultima:  row.ultima  ? Number(row.ultima)  : null,
            diasCobertura: Math.round(diasCobertura * 10) / 10,
            sincronizando: sincronizandoKlines,
            diasObjetivo: DIAS_CACHE_KLINES,
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
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
