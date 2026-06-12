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
if (JWT_SECRET === 'cambiar_este_secreto_en_produccion') {
    if (process.env.NODE_ENV === 'production') {
        console.error('FATAL: JWT_SECRET no configurado. Abortando para no exponer tokens predecibles en producción.');
        process.exit(1);
    }
    console.warn('⚠️ [Seguridad] JWT_SECRET con valor por defecto — solo aceptable en desarrollo local.');
}

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
    // Ratios de posicionamiento (Binance futures/data): top_pos = top traders por tamaño de
    // posición (smart money), global_acc = cuentas long/short global (retail). Ambos a 5m,
    // últimos ~30 días disponibles. Una fila por timestamp con las dos métricas.
    const queryTablaLSR = `
        CREATE TABLE IF NOT EXISTS long_short_ratio (
            tiempo BIGINT PRIMARY KEY,
            top_pos NUMERIC,
            global_acc NUMERIC
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
        await pool.query(queryTablaLSR);
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
        // Migración: columna mercado para distinguir flujo SPOT (stream.binance.com) del
        // flujo PERP (fstream.binance.com). Permite filtrar por mercado en análisis futuros.
        await pool.query(`ALTER TABLE ballenas ADD COLUMN IF NOT EXISTS mercado VARCHAR(5) DEFAULT 'SPOT'`);

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
                margin_type         VARCHAR(10) DEFAULT 'ISOLATED',
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

// Backfill de OI histórico. El recolector live solo acumula OI desde que el server arranca,
// pero los backtests corren sobre la cache de velas (hasta 365 días). Sin esto el filtro de OI
// no tendría dato en el tramo histórico y rechazaría todas las entradas.
// LÍMITE DURO: Binance solo expone OI histórico de los ÚLTIMOS ~30 DÍAS (futures/data/openInterestHist,
// máx 500 puntos/request). No hay forma de conseguir OI más viejo → el filtro de OI es fiable
// solo en backtests de ≤ 30 días; más atrás no hay cobertura (se avisa con warning).
async function backfillOpenInterestHistorico() {
    try {
        const PERIOD = '5m', LIMIT = 500, STEP_MS = 5 * 60000;
        const limiteInferior = Date.now() - 30 * 86400000;
        // Solo backfillear si falta cobertura histórica (evita re-trabajo en cada reinicio).
        const cob = await pool.query('SELECT MIN(tiempo) AS min FROM open_interest');
        const minSeg = cob.rows[0] && cob.rows[0].min ? Number(cob.rows[0].min) : null;
        if (minSeg && minSeg * 1000 <= limiteInferior + 2 * 86400000) {
            return; // ya hay datos que llegan a ~28+ días atrás
        }
        let endTime = Date.now(), pedidos = 0, insertados = 0;
        while (endTime > limiteInferior && pedidos < 25) {
            const url = `https://fapi.binance.com/futures/data/openInterestHist?symbol=BTCUSDT&period=${PERIOD}&limit=${LIMIT}&endTime=${endTime}`;
            const resp = await fetch(url);
            const datos = await resp.json();
            if (!Array.isArray(datos) || datos.length === 0) break;
            // datos: [{ sumOpenInterest, sumOpenInterestValue, timestamp(ms) }] en orden ascendente.
            // Guardamos sumOpenInterest (OI en BTC), consistente con el recolector live (/openInterest).
            const placeholders = [], params = [];
            for (const d of datos) {
                const valor  = parseFloat(d.sumOpenInterest);
                const tiempo = Math.floor(Number(d.timestamp) / 60000) * 60; // epoch seg alineado a minuto
                if (isNaN(valor) || isNaN(tiempo)) continue;
                const o = params.length;
                placeholders.push(`($${o + 1},$${o + 2})`);
                params.push(tiempo, valor);
            }
            if (params.length) {
                const r = await pool.query(
                    `INSERT INTO open_interest (tiempo, valor) VALUES ${placeholders.join(',')}
                     ON CONFLICT (tiempo) DO NOTHING`,
                    params
                );
                insertados += r.rowCount;
            }
            endTime = Number(datos[0].timestamp) - STEP_MS; // siguiente página, hacia atrás
            pedidos++;
        }
        console.log(`[OI] Backfill histórico: ${insertados} puntos nuevos en ${pedidos} requests (cobertura ~30 días).`);
    } catch (e) {
        console.error('[OI] Error en backfill histórico:', e.message);
    }
}
setTimeout(backfillOpenInterestHistorico, 5000);

// --- RECOLECTOR DE RATIOS DE POSICIONAMIENTO (top traders + retail) ---
// Los endpoints futures/data se actualizan cada 5m, así que polleamos a ese ritmo.
async function guardarLongShortRatio() {
    try {
        const [topR, globR] = await Promise.all([
            fetch('https://fapi.binance.com/futures/data/topLongShortPositionRatio?symbol=BTCUSDT&period=5m&limit=1').then(r => r.json()),
            fetch('https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol=BTCUSDT&period=5m&limit=1').then(r => r.json()),
        ]);
        const t = Array.isArray(topR) && topR[0], g = Array.isArray(globR) && globR[0];
        if (t && g) {
            const tiempo = Math.floor(Number(t.timestamp) / 60000) * 60;
            await pool.query(
                `INSERT INTO long_short_ratio (tiempo, top_pos, global_acc) VALUES ($1,$2,$3)
                 ON CONFLICT (tiempo) DO UPDATE SET top_pos = EXCLUDED.top_pos, global_acc = EXCLUDED.global_acc`,
                [tiempo, parseFloat(t.longShortRatio), parseFloat(g.longShortRatio)]
            );
        }
    } catch (e) {
        console.error('Error al guardar Long/Short ratio:', e.message);
    }
}
setInterval(guardarLongShortRatio, 5 * 60000);
guardarLongShortRatio();

// Backfill histórico de ratios (~30 días, 5m). Mismo límite duro que el OI: Binance solo
// expone los últimos ~30 días de futures/data. Top y global se alinean por timestamp.
async function backfillLongShortRatio() {
    try {
        const PERIOD = '5m', LIMIT = 500, STEP_MS = 5 * 60000;
        const limiteInferior = Date.now() - 30 * 86400000;
        const cob = await pool.query('SELECT MIN(tiempo) AS min FROM long_short_ratio');
        const minSeg = cob.rows[0] && cob.rows[0].min ? Number(cob.rows[0].min) : null;
        if (minSeg && minSeg * 1000 <= limiteInferior + 2 * 86400000) return;
        let endTime = Date.now(), pedidos = 0, insertados = 0;
        while (endTime > limiteInferior && pedidos < 25) {
            const [topR, globR] = await Promise.all([
                fetch(`https://fapi.binance.com/futures/data/topLongShortPositionRatio?symbol=BTCUSDT&period=${PERIOD}&limit=${LIMIT}&endTime=${endTime}`).then(r => r.json()),
                fetch(`https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol=BTCUSDT&period=${PERIOD}&limit=${LIMIT}&endTime=${endTime}`).then(r => r.json()),
            ]);
            if (!Array.isArray(topR) || topR.length === 0) break;
            const globMap = new Map((Array.isArray(globR) ? globR : [])
                .map(g => [Math.floor(Number(g.timestamp) / 60000) * 60, parseFloat(g.longShortRatio)]));
            const placeholders = [], params = [];
            for (const d of topR) {
                const tiempo  = Math.floor(Number(d.timestamp) / 60000) * 60;
                const topVal  = parseFloat(d.longShortRatio);
                const globVal = globMap.has(tiempo) ? globMap.get(tiempo) : null;
                if (isNaN(tiempo) || isNaN(topVal)) continue;
                const o = params.length;
                placeholders.push(`($${o + 1},$${o + 2},$${o + 3})`);
                params.push(tiempo, topVal, globVal);
            }
            if (params.length) {
                const r = await pool.query(
                    `INSERT INTO long_short_ratio (tiempo, top_pos, global_acc) VALUES ${placeholders.join(',')}
                     ON CONFLICT (tiempo) DO NOTHING`,
                    params
                );
                insertados += r.rowCount;
            }
            endTime = Number(topR[0].timestamp) - STEP_MS;
            pedidos++;
        }
        console.log(`[LSR] Backfill histórico: ${insertados} puntos nuevos en ${pedidos} requests (~30 días).`);
    } catch (e) {
        console.error('[LSR] Error en backfill histórico:', e.message);
    }
}
setTimeout(backfillLongShortRatio, 7000);

// ── Watchdog de WebSockets ────────────────────────────────────────────
// Un WS puede quedar mudo sin disparar 'close' (conexión zombi): el feed deja de grabar
// y nadie se entera — para una terminal cuya ventaja es el histórico, un hueco silencioso
// es el peor escenario. El aggTrade de BTCUSDT emite varias veces por segundo, así que
// minutos de silencio = conexión muerta: se fuerza terminate() y el handler de 'close'
// existente reconecta. Cubre también sockets colgados en CONNECTING (terminate los cierra).
const WS_SILENCIO_MAX_MS = 3 * 60 * 1000;
const wsSalud = new Map(); // nombre del feed → { ws, ultimoMsg }

function registrarSaludWS(nombre, ws) { wsSalud.set(nombre, { ws, ultimoMsg: Date.now() }); }
function latidoWS(nombre) { const s = wsSalud.get(nombre); if (s) s.ultimoMsg = Date.now(); }

setInterval(() => {
    for (const [nombre, s] of wsSalud) {
        if (Date.now() - s.ultimoMsg > WS_SILENCIO_MAX_MS) {
            console.warn(`⚠️ [Watchdog] Feed "${nombre}" sin mensajes hace >${WS_SILENCIO_MAX_MS / 60000} min — forzando reconexión.`);
            s.ultimoMsg = Date.now(); // no re-disparar mientras la reconexión está en curso
            try { s.ws.terminate(); } catch (_) {}
        }
    }
}, 60 * 1000);

// --- CAZADOR DE BALLENAS (SPOT + PERP) ---
// Dos feeds paralelos: spot (stream.binance.com) y futuros perp (fstream.binance.com).
// Cada trade se guarda con su columna `mercado` ('SPOT' o 'PERP') para poder filtrar
// después. El flujo de futuros captura el apalancamiento y las liquidaciones forzadas,
// que no están disponibles en el feed de spot.
const wsBallenasConectando = new Set();

function iniciarRastreadorBallenas(wsUrl, mercado) {
    if (wsBallenasConectando.has(mercado)) return;
    wsBallenasConectando.add(mercado);

    const ws = new WebSocket(wsUrl);
    registrarSaludWS(`ballenas-${mercado}`, ws);

    ws.on('open', () => {
        wsBallenasConectando.delete(mercado);
        console.log(`✅ Cazador de ballenas (${mercado}) conectado.`);
    });

    ws.on('message', async (data) => {
        latidoWS(`ballenas-${mercado}`);
        try {
            const evento = JSON.parse(data);
            const cantidad = parseFloat(evento.q);
            const precio   = parseFloat(evento.p);
            const es_venta = evento.m;
            if (cantidad >= limiteGuardadoBD) {
                await pool.query(
                    `INSERT INTO ballenas (precio, cantidad, es_venta, mercado) VALUES ($1, $2, $3, $4)`,
                    [precio, cantidad, es_venta, mercado]
                );
            }
        } catch (error) {
            console.error(`Error al guardar trade (${mercado}):`, error);
        }
    });

    ws.on('error', (err) => {
        console.error(`Error en WebSocket ballenas (${mercado}):`, err.message);
        ws.terminate();
    });

    ws.on('close', () => {
        wsBallenasConectando.delete(mercado);
        console.log(`⚠️ Ballenas (${mercado}) reconectando en 5 segundos...`);
        setTimeout(() => iniciarRastreadorBallenas(wsUrl, mercado), 5000);
    });
}

iniciarRastreadorBallenas('wss://stream.binance.com:9443/ws/btcusdt@aggTrade', 'SPOT');
iniciarRastreadorBallenas('wss://fstream.binance.com/ws/btcusdt@aggTrade',     'PERP');

// Retención del tape de ballenas: alineada a la ventana de la cache de velas (365 días),
// que es lo máximo que un backtest puede consultar — más atrás la tabla solo crece sin uso.
// Configurable con BALLENAS_RETENCION_DIAS en el entorno; 0 o negativo = no podar nunca.
const DIAS_RETENCION_BALLENAS = process.env.BALLENAS_RETENCION_DIAS !== undefined
    ? parseInt(process.env.BALLENAS_RETENCION_DIAS)
    : 365;

async function podarBallenas() {
    if (!(DIAS_RETENCION_BALLENAS > 0)) return;
    try {
        const r = await pool.query(
            `DELETE FROM ballenas WHERE fecha < NOW() - make_interval(days => $1)`,
            [DIAS_RETENCION_BALLENAS]
        );
        if (r.rowCount > 0) console.log(`[Ballenas] Retención: ${r.rowCount} trades de más de ${DIAS_RETENCION_BALLENAS} días eliminados.`);
    } catch (e) {
        console.error('[Ballenas] Error en retención:', e.message);
    }
}
setInterval(podarBallenas, 24 * 3600 * 1000);
setTimeout(podarBallenas, 60000); // al arrancar, diferido para no competir con la inicialización


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
            ...(process.env.NODE_ENV === 'production' ? { secure: true } : {}),
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
    // Ventana acotada: el gráfico marca ballenas solo sobre velas cargadas y el delta usa 24h;
    // devolver la tabla entera (100k filas) era puro payload. Default 7 días, máx 30.
    const horas   = Math.min(Math.max(parseInt(req.query.horas) || 168, 1), 720);
    const mercado = ['SPOT', 'PERP'].includes(req.query.mercado) ? req.query.mercado : null;
    try {
        const params = [horas];
        if (mercado) params.push(mercado);
        const query = `
            SELECT precio, cantidad, es_venta, mercado, EXTRACT(EPOCH FROM fecha) as tiempo_segundos
            FROM ballenas
            WHERE fecha >= NOW() - make_interval(hours => $1)
              ${mercado ? 'AND mercado = $2' : ''}
            ORDER BY fecha DESC
            LIMIT 100000
        `;
        const resultado = await pool.query(query, params);
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

// Histórico de ratios de posicionamiento para el panel del gráfico.
app.get('/api/long-short', autenticar, async (req, res) => {
    try {
        const r = await pool.query(`
            SELECT tiempo, top_pos, global_acc FROM (
                SELECT tiempo, top_pos, global_acc FROM long_short_ratio ORDER BY tiempo DESC LIMIT 10000
            ) t ORDER BY tiempo ASC
        `);
        res.json(r.rows);
    } catch (error) {
        res.status(500).json({ error: 'Error obteniendo ratios de posicionamiento' });
    }
});

// Valor actual de los ratios (proxy a Binance, sin CORS) para el tail en vivo del panel.
app.get('/api/ls-live', autenticar, async (req, res) => {
    try {
        const [topR, globR] = await Promise.all([
            fetch('https://fapi.binance.com/futures/data/topLongShortPositionRatio?symbol=BTCUSDT&period=5m&limit=1').then(r => r.json()),
            fetch('https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol=BTCUSDT&period=5m&limit=1').then(r => r.json()),
        ]);
        const t = Array.isArray(topR) && topR[0], g = Array.isArray(globR) && globR[0];
        if (!t) return res.json({});
        res.json({
            tiempo:      Math.floor(Number(t.timestamp) / 60000) * 60,
            topRatio:    parseFloat(t.longShortRatio),
            globalRatio: g ? parseFloat(g.longShortRatio) : null,
        });
    } catch (error) {
        res.status(500).json({ error: 'Error obteniendo ratios en vivo' });
    }
});

app.get('/api/filtro-bd', autenticar, (req, res) => {
    res.json({ umbral: limiteGuardadoBD });
});

app.post('/api/filtro-bd', autenticar, soloAdmin, async (req, res) => {
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
    const mercado = ['SPOT', 'PERP'].includes(req.query.mercado) ? req.query.mercado : null;
    try {
        const params = [horas, bucket];
        if (mercado) params.push(mercado);
        const result = await pool.query(`
            SELECT
                (FLOOR(precio::numeric / $2) * $2)::bigint AS nivel,
                ROUND(SUM(CASE WHEN es_venta = false THEN cantidad ELSE 0 END)::numeric, 2) AS compras,
                ROUND(SUM(CASE WHEN es_venta = true  THEN cantidad ELSE 0 END)::numeric, 2) AS ventas
            FROM ballenas
            WHERE fecha >= NOW() - make_interval(hours => $1)
              ${mercado ? 'AND mercado = $3' : ''}
            GROUP BY nivel
            HAVING SUM(cantidad) > 0
            ORDER BY nivel DESC
        `, params);
        res.json(result.rows);
    } catch (error) {
        console.error('Error whale-histogram:', error);
        res.status(500).json({ error: 'Error interno' });
    }
});


// ============================================================
// NIVELES DE PRECIO POR VOLUMEN (HVN)
// ============================================================
// Perfil de volumen-por-precio sobre klines_1m con decaimiento exponencial por
// antigüedad (el volumen de hace meses pesa menos que el reciente), detección de
// zonas de alto volumen (buckets contiguos por encima de umbral × mediana del
// perfil suavizado) y enriquecimiento con volumen ballena + historial de toques.
// El perfil es el mismo para todos los usuarios y solo cambia con velas nuevas,
// así que se cachea en memoria por combinación de parámetros.
const cacheNiveles = new Map(); // clave de params → { ts, data }
const TTL_NIVELES_MS = 5 * 60 * 1000;

// Cuenta visitas del precio a la zona [desde, hasta) sobre velas 15m: un "toque"
// arranca cuando el rango de la vela intersecta la zona viniendo de afuera; la
// visita termina cuando un cierre queda fuera. Si sale por el mismo lado por el
// que entró es un rebote (la zona actuó); si la atraviesa, no. Para que las
// oscilaciones de un rango no cuenten cada una como toque, se exige que el
// precio haya pasado sepMin velas (8×15m = 2h) sin tocar la zona antes de que
// el contacto cuente como visita nueva.
function contarToquesZona(velas, desde, hasta, sepMin = 8) {
    let toques = 0, rebotes = 0, ultimoToque = null;
    let enVisita = false, lado = null, ladoEntrada = null;
    let fuera = Infinity;
    for (const v of velas) {
        const high = v[2], low = v[3], close = v[4];
        const toca = low < hasta && high > desde;
        if (!enVisita && toca && fuera >= sepMin) {
            enVisita = true;
            ladoEntrada = lado;
            toques++;
            ultimoToque = v[0];
        }
        if (enVisita && (close >= hasta || close < desde)) {
            const ladoSalida = close >= hasta ? 'arriba' : 'abajo';
            if (ladoEntrada && ladoSalida === ladoEntrada) rebotes++;
            enVisita = false;
        }
        fuera = toca ? 0 : fuera + 1;
        if (close >= hasta) lado = 'arriba';
        else if (close < desde) lado = 'abajo';
    }
    return { toques, rebotes, ultimoToque };
}

async function calcularNiveles({ bucket, dias, tau, umbral, maxZonas }) {
    const ahora = Date.now();
    const desde = ahora - dias * 86400000;

    const [perfilR, ballenasR, velas15m] = await Promise.all([
        // 1) Volumen por bucket de precio. Cada vela 1m aporta todo su volumen al
        //    bucket de su precio típico (H+L+C)/3 — con velas de 1m el rango es
        //    menor que el bucket, así que el error es despreciable.
        pool.query(
            `SELECT (FLOOR(((high + low + close) / 3) / $2) * $2)::bigint AS nivel,
                    SUM(volume)         AS vol_total,
                    SUM(taker_buy_base) AS vol_compra,
                    SUM(volume * EXP(-(($3::bigint - open_time) / 86400000.0) / $4)) AS vol_pond
             FROM klines_1m
             WHERE open_time >= $1::bigint
             GROUP BY nivel ORDER BY nivel ASC`,
            [desde, bucket, ahora, tau]
        ),
        // 2) Volumen ballena por bucket en la misma ventana.
        pool.query(
            `SELECT (FLOOR(precio / $2) * $2)::bigint AS nivel,
                    SUM(CASE WHEN es_venta = false THEN cantidad ELSE 0 END) AS compras,
                    SUM(CASE WHEN es_venta = true  THEN cantidad ELSE 0 END) AS ventas
             FROM ballenas
             WHERE fecha >= to_timestamp($1 / 1000.0)
             GROUP BY nivel`,
            [desde, bucket]
        ),
        // 3) Velas 15m para contar toques/rebotes de cada zona y el precio actual.
        fetchKlinesDesdeBD('15m', dias),
    ]);

    const buckets = perfilR.rows.map(r => ({
        nivel: Number(r.nivel),
        volTotal: Number(r.vol_total),
        volCompra: Number(r.vol_compra),
        volPond: Number(r.vol_pond),
    }));
    if (buckets.length < 5 || velas15m.length === 0) {
        return { precio: null, generado: ahora, niveles: [] };
    }
    const precio = velas15m[velas15m.length - 1][4];

    // Suavizado del perfil (media móvil de 3 buckets) para que el ruido no
    // fragmente un nodo real en varias zonas pegadas.
    const suave = buckets.map((b, i) => {
        const vecinos = [buckets[i - 1], b, buckets[i + 1]].filter(Boolean);
        return vecinos.reduce((s, x) => s + x.volPond, 0) / vecinos.length;
    });
    const mediana = [...suave].sort((a, b) => a - b)[Math.floor(suave.length / 2)];
    const corte = mediana * umbral;

    // Corridas: buckets CONTIGUOS en precio (no solo en el array) sobre el corte.
    const runs = [];
    let run = null;
    buckets.forEach((b, i) => {
        const alto = suave[i] >= corte;
        if (alto && run && b.nivel === buckets[run[run.length - 1]].nivel + bucket) {
            run.push(i);
        } else if (alto) {
            if (run) runs.push(run);
            run = [i];
        } else if (run) {
            runs.push(run);
            run = null;
        }
    });
    if (run) runs.push(run);
    if (runs.length === 0) return { precio, generado: ahora, niveles: [] };

    // Una corrida puede contener varios nodos pegados (típico alrededor del precio
    // actual, donde el decaimiento hace que casi todo supere el corte). Se divide
    // en los valles (<85% del pico vecino más bajo) entre picos del perfil suavizado.
    function dividirRunEnNodos(idxs) {
        if (idxs.length < 6) return [idxs];
        const picos = [];
        for (let i = 0; i < idxs.length; i++) {
            const v   = suave[idxs[i]];
            const izq = i === 0 ? -Infinity : suave[idxs[i - 1]];
            const der = i === idxs.length - 1 ? -Infinity : suave[idxs[i + 1]];
            if (v >= izq && v > der) picos.push(i); // "> der" evita doble pico en mesetas
        }
        if (picos.length <= 1) return [idxs];
        const cortes = [];
        for (let p = 0; p < picos.length - 1; p++) {
            let vMin = Infinity, iMin = -1;
            for (let i = picos[p] + 1; i < picos[p + 1]; i++) {
                if (suave[idxs[i]] < vMin) { vMin = suave[idxs[i]]; iMin = i; }
            }
            const techo = Math.min(suave[idxs[picos[p]]], suave[idxs[picos[p + 1]]]);
            if (iMin > 0 && vMin <= 0.85 * techo) cortes.push(iMin);
        }
        if (cortes.length === 0) return [idxs];
        const segs = [];
        let ini = 0;
        cortes.forEach(c => { segs.push(idxs.slice(ini, c)); ini = c; });
        segs.push(idxs.slice(ini));
        return segs.filter(s => s.length > 0);
    }

    // Tope duro de ancho: una "zona" de miles de dólares no es un nivel operable.
    // Si tras el corte por valles un segmento sigue ancho (perfil en joroba única,
    // sin valles marcados), se parte recursivamente por su valle interior más
    // profundo hasta quedar bajo ANCHO_MAX buckets.
    const ANCHO_MAX = 12;
    function dividirPorAncho(seg) {
        if (seg.length <= ANCHO_MAX) return [seg];
        let vMin = Infinity, iMin = -1;
        for (let i = 1; i < seg.length - 1; i++) {
            if (suave[seg[i]] < vMin) { vMin = suave[seg[i]]; iMin = i; }
        }
        return [...dividirPorAncho(seg.slice(0, iMin)), ...dividirPorAncho(seg.slice(iMin))];
    }

    const zonas = [];
    runs.forEach(r => dividirRunEnNodos(r).flatMap(dividirPorAncho).forEach(seg => {
        zonas.push({
            desde: buckets[seg[0]].nivel,
            hasta: buckets[seg[seg.length - 1]].nivel + bucket,
            buckets: seg.map(i => buckets[i]),
        });
    }));

    const ballenasPorNivel = new Map(ballenasR.rows.map(r => [Number(r.nivel), r]));
    const volPondTotal = buckets.reduce((s, b) => s + b.volPond, 0);

    const niveles = zonas.map(zona => {
        const volPond  = zona.buckets.reduce((s, b) => s + b.volPond, 0);
        const volTotal = zona.buckets.reduce((s, b) => s + b.volTotal, 0);
        const volCompra = zona.buckets.reduce((s, b) => s + b.volCompra, 0);
        const centro = zona.buckets.reduce((s, b) => s + (b.nivel + bucket / 2) * b.volPond, 0) / volPond;
        let ballenasCompra = 0, ballenasVenta = 0;
        for (let n = zona.desde; n < zona.hasta; n += bucket) {
            const w = ballenasPorNivel.get(n);
            if (w) { ballenasCompra += Number(w.compras); ballenasVenta += Number(w.ventas); }
        }
        const { toques, rebotes, ultimoToque } = contarToquesZona(velas15m, zona.desde, zona.hasta);
        const rol = precio >= zona.hasta ? 'soporte' : precio < zona.desde ? 'resistencia' : 'en_precio';
        return {
            centro: Math.round(centro),
            banda: [zona.desde, zona.hasta],
            volPond: Math.round(volPond),
            pctPerfil: Math.round(volPond / volPondTotal * 1000) / 10,
            deltaPct: volTotal > 0 ? Math.round(volCompra / volTotal * 100) : 50,
            ballenasCompra: Math.round(ballenasCompra * 10) / 10,
            ballenasVenta: Math.round(ballenasVenta * 10) / 10,
            toques, rebotes,
            ultimoToque,
            rol,
        };
    });

    // Score 0–10: peso del volumen (40%), ballenas alineadas al rol (25%),
    // historial de rebotes (20%), recencia del último toque (15%). El score
    // ballena combina dirección (fracción alineada al rol) con magnitud
    // (relativa a la zona con más volumen ballena, con sqrt para suavizar):
    // 30 BTC comprados sin ventas no valen lo mismo que 1500.
    const maxVolPond = Math.max(...niveles.map(n => n.volPond));
    const maxW = Math.max(...niveles.map(n => n.ballenasCompra + n.ballenasVenta), 1);
    niveles.forEach(n => {
        const volScore = 4 * (n.volPond / maxVolPond);
        const totalW = n.ballenasCompra + n.ballenasVenta;
        const fAlineada = totalW === 0 ? 0
            : n.rol === 'soporte'     ? n.ballenasCompra / totalW
            : n.rol === 'resistencia' ? n.ballenasVenta / totalW
            : 0.5;
        const whaleScore  = 2.5 * fAlineada * Math.sqrt(totalW / maxW);
        const reboteScore = 2 * Math.min(n.rebotes, 4) / 4;
        const recencia    = n.ultimoToque ? 1.5 * Math.exp(-((ahora - n.ultimoToque) / 86400000) / 14) : 0;
        n.score = Math.round((volScore + whaleScore + reboteScore + recencia) * 10) / 10;
    });

    // Supresión de no-máximos: dos niveles casi pegados no aportan información
    // distinta; se queda el de mayor score y se descarta todo centro a menos de
    // sepMin (≈0.4% del precio, mínimo 2 buckets).
    niveles.sort((a, b) => b.score - a.score);
    const sepMin = Math.max(2 * bucket, precio * 0.004);
    const elegidos = [];
    for (const n of niveles) {
        if (elegidos.length >= maxZonas) break;
        if (elegidos.every(e => Math.abs(e.centro - n.centro) >= sepMin)) elegidos.push(n);
    }
    return { precio, generado: ahora, niveles: elegidos };
}

app.get('/api/niveles', autenticar, async (req, res) => {
    const bucket   = [50, 100, 200, 500].includes(parseInt(req.query.bucket)) ? parseInt(req.query.bucket) : 100;
    const dias     = Math.min(Math.max(parseInt(req.query.dias) || 90, 7), 365);
    const tau      = Math.min(Math.max(parseFloat(req.query.tau) || 14, 1), 90);
    const umbral   = Math.min(Math.max(parseFloat(req.query.umbral) || 1.5, 1), 5);
    const maxZonas = Math.min(Math.max(parseInt(req.query.max) || 8, 1), 30);

    const clave = `${bucket}|${dias}|${tau}|${umbral}|${maxZonas}`;
    const cacheado = cacheNiveles.get(clave);
    if (cacheado && Date.now() - cacheado.ts < TTL_NIVELES_MS) return res.json(cacheado.data);

    try {
        const data = await calcularNiveles({ bucket, dias, tau, umbral, maxZonas });
        cacheNiveles.set(clave, { ts: Date.now(), data });
        res.json(data);
    } catch (e) {
        console.error('Error niveles:', e);
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

// ATR de Wilder. Devuelve array alineado a los closes (null hasta calentar).
function calcATR(highs, lows, closes, period = 14) {
    const n = closes.length;
    const atr = new Array(n).fill(null);
    if (n <= period) return atr;
    const tr = new Array(n).fill(0);
    for (let i = 1; i < n; i++) {
        tr[i] = Math.max(
            highs[i] - lows[i],
            Math.abs(highs[i] - closes[i - 1]),
            Math.abs(lows[i]  - closes[i - 1])
        );
    }
    let a = 0;
    for (let i = 1; i <= period; i++) a += tr[i];
    a /= period;
    atr[period] = a;
    for (let i = period + 1; i < n; i++) { a = (a * (period - 1) + tr[i]) / period; atr[i] = a; }
    return atr;
}

// Pendiente de la EMA normalizada por ATR: slopeNorm = (ema[i] - ema[i-slopeBars]) / atr[i].
// Mismo cálculo que el indicador "Angulación EMA" del terminal (público/index.html).
function calcEMAangSlope(closes, highs, lows, emaLen = 200, atrLen = 14, slopeBars = 10) {
    const n = closes.length;
    const slope = new Array(n).fill(null);
    const ema = calcEMA(closes, emaLen);
    const atr = calcATR(highs, lows, closes, atrLen);
    for (let i = slopeBars; i < n; i++) {
        if (ema[i] == null || ema[i - slopeBars] == null || atr[i] == null || atr[i] === 0) continue;
        slope[i] = (ema[i] - ema[i - slopeBars]) / atr[i];
    }
    return slope;
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

// Agrega velas 1m en buckets más grandes (p.ej. 1h) en el mismo formato array que
// Binance. Se usa para temporalidades que no guardamos (la BD solo cachea 1m y deriva
// 5m/15m por SQL), así el resultado es idéntico con fuente BD o Binance. Igual que
// fetchKlinesDesdeBD, descarta el bucket EN CURSO: sus velas 1m están incompletas y
// los indicadores calculados sobre él cambiarían al re-evaluar un minuto después.
function agregarVelas1m(bars1m, bucketMs) {
    const out = [];
    let cur = null, curB = -1;
    for (const b of bars1m) {
        const bk = Math.floor(parseInt(b[0]) / bucketMs);
        if (bk !== curB) {
            if (cur) out.push(cur);
            curB = bk;
            cur = [bk * bucketMs, parseFloat(b[1]), parseFloat(b[2]), parseFloat(b[3]),
                   parseFloat(b[4]), parseFloat(b[5]), bk * bucketMs + bucketMs - 1, 0, 0, parseFloat(b[9])];
        } else {
            cur[2] = Math.max(cur[2], parseFloat(b[2]));
            cur[3] = Math.min(cur[3], parseFloat(b[3]));
            cur[4] = parseFloat(b[4]);
            cur[5] += parseFloat(b[5]);
            cur[9] += parseFloat(b[9]);
        }
    }
    if (cur) out.push(cur);
    const lastClose1m = bars1m.length ? parseInt(bars1m[bars1m.length - 1][6]) : 0;
    while (out.length && out[out.length - 1][6] > lastClose1m) out.pop();
    return out;
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
// El funding es DIRECCIONAL: funding positivo → longs pagan, shorts cobran (costo negativo).
// side = 1 (long) o -1 (short). Si se omite, se asume que se paga siempre (conservador).
function costoOperacion(p, palanca, minutesHeld, side = 1) {
    const fees = (p.commission   / 100) * 2 * palanca;
    const slip = (p.slippagePerc / 100) * 2 * palanca;
    // side * fundingPerc: long con funding positivo paga (+), short cobra (-).
    const fund = side * (p.fundingPerc / 100) * (Math.max(0, minutesHeld) / 480) * palanca;
    return fees + slip + fund;
}

function runBacktest(bars1m, bars5m, bars15m, whalesArr, p, oiArr, lsArr) {
    const c1m = bars1m.map(b => parseFloat(b[4]));

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
    // ADX — temporalidad configurable (1m/5m/15m/1h). La de 1h no se guarda en ningún
    // lado: se deriva agregando las velas 1m del período (agregarVelas1m).
    const adxTf = ['1m', '5m', '15m', '1h'].includes(p.adxTf) ? p.adxTf : '15m';
    let adxDirect = null, adxByTs = null, tsAdx = null;
    if (p.useADXFilter) {
        if (adxTf === '1m') {
            adxDirect = calcADX(bars1m.map(b => parseFloat(b[2])), bars1m.map(b => parseFloat(b[3])), c1m);
        } else {
            const barsAdx = adxTf === '5m' ? bars5m : adxTf === '15m' ? bars15m : agregarVelas1m(bars1m, 3600000);
            const arr = calcADX(barsAdx.map(b => parseFloat(b[2])), barsAdx.map(b => parseFloat(b[3])), barsAdx.map(b => parseFloat(b[4])));
            adxByTs = new Map(barsAdx.map((b, i) => [parseInt(b[6]), arr[i]]));
            tsAdx   = barsAdx.map(b => parseInt(b[6])).sort((a, b) => a - b);
        }
    }

    // Angulación de EMA — pendiente normalizada por ATR, con temporalidad propia.
    // gate = umbral mínimo de pendiente; 'strong' exige la banda fuerte. Long pide
    // slope >= +gate (alcista), Short pide slope <= -gate (bajista). Default OFF.
    const useEmaAngFilter = p.useEmaAngFilter === true;
    const emaAngTf        = p.emaAngTf        || '15m';
    const emaAngLen       = p.emaAngLen       || 200;
    const emaAngSlopeBars = p.emaAngSlopeBars || 10;
    const emaAngAtr       = p.emaAngAtr       || 14;
    const emaAngGate      = p.emaAngMode === 'strong'
        ? (p.emaAngStrongSlope ?? 0.60)
        : (p.emaAngMinSlope    ?? 0.25);
    let emaAngDirect = null, emaAngByTs = null, tsEmaAng = null;
    if (useEmaAngFilter) {
        if (emaAngTf === '1m') {
            emaAngDirect = calcEMAangSlope(c1m, bars1m.map(b => parseFloat(b[2])), bars1m.map(b => parseFloat(b[3])), emaAngLen, emaAngAtr, emaAngSlopeBars);
        } else if (emaAngTf === '5m') {
            const h5m = bars5m.map(b => parseFloat(b[2])), l5m = bars5m.map(b => parseFloat(b[3]));
            const arr = calcEMAangSlope(c5m_pb, h5m, l5m, emaAngLen, emaAngAtr, emaAngSlopeBars);
            emaAngByTs = new Map(bars5m.map((b, i) => [parseInt(b[6]), arr[i]]));
            tsEmaAng   = bars5m.map(b => parseInt(b[6])).sort((a, b) => a - b);
        } else {
            const arr = calcEMAangSlope(c15m, h15m, l15m, emaAngLen, emaAngAtr, emaAngSlopeBars);
            emaAngByTs = new Map(bars15m.map((b, i) => [parseInt(b[6]), arr[i]]));
            tsEmaAng   = bars15m.map(b => parseInt(b[6])).sort((a, b) => a - b);
        }
    }

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

    // Open Interest — "OI subiendo confirma": solo se permite entrar (long o short) si el OI
    // creció ≥ oiThreshold% en las últimas oiLookbackMin minutos = entra dinero nuevo respaldando
    // el movimiento. El OI se muestrea cada 1m (live) / 5m (backfill histórico); lookupHTF toma la
    // última muestra ≤ tsClose, sin look-ahead. Si no hay dato (período sin cobertura) → no entra.
    const useOIFilter  = p.useOIFilter === true;
    const oiLookbackMs = (p.oiLookbackMin || 30) * 60000;
    const oiThreshold  = Number.isFinite(p.oiThreshold) ? p.oiThreshold : 0.5;
    let oiTs = null, oiByTs = null;
    if (useOIFilter && Array.isArray(oiArr) && oiArr.length) {
        oiByTs = new Map();
        for (const o of oiArr) oiByTs.set(Number(o.tiempo) * 1000, parseFloat(o.valor));
        oiTs = [...oiByTs.keys()].sort((a, b) => a - b);
    }

    // Posicionamiento de traders — a diferencia del OI, importa el NIVEL actual del ratio, no el
    // cambio: lookupHTF toma la última muestra (5m) ≤ tsClose. Top traders = seguir smart money
    // (long si están netos long); retail (global) = fade contrarian (no comprar si el retail está
    // sobrecargado long). Umbrales simétricos alrededor del neutro 1.0 (ratio long/short).
    const useTopTraderFilter  = p.useTopTraderFilter === true;
    const useRetailFilter     = p.useRetailFilter === true;
    // Pendiente del ratio — smart money ACUMULANDO (slope > threshold) vs DISTRIBUYENDO.
    // Independiente del filtro de nivel: podés usar solo el slope, solo el nivel, o ambos.
    const useTopSlopeFilter   = p.useTopSlopeFilter === true;
    const topSlopeLookbackMs  = (p.topSlopeLookbackMin ?? 15) * 60000;
    const topTraderRatio = Number.isFinite(p.topTraderRatio) ? p.topTraderRatio : 1.05;
    const retailExtreme  = Number.isFinite(p.retailExtreme)  ? p.retailExtreme  : 2.0;
    let topTs = null, topByTs = null, globTs = null, globByTs = null;
    if ((useTopTraderFilter || useRetailFilter || useTopSlopeFilter) && Array.isArray(lsArr) && lsArr.length) {
        topByTs = new Map(); globByTs = new Map();
        for (const r of lsArr) {
            const tms = Number(r.tiempo) * 1000;
            if (r.top_pos    != null) topByTs.set(tms, parseFloat(r.top_pos));
            if (r.global_acc != null) globByTs.set(tms, parseFloat(r.global_acc));
        }
        topTs  = [...topByTs.keys()].sort((a, b) => a - b);
        globTs = [...globByTs.keys()].sort((a, b) => a - b);
    }

    // Delta de volumen — prefix sum para rolling sum O(1) por barra
    const deltaPfx = new Array(bars1m.length + 1).fill(0);
    for (let i = 0; i < bars1m.length; i++) {
        const bv = parseFloat(bars1m[i][9]); // takerBuyBaseAssetVolume
        const tv = parseFloat(bars1m[i][5]); // total volume
        deltaPfx[i + 1] = deltaPfx[i] + (bv - (tv - bv));
    }

    // CVD (Cumulative Volume Delta) — reutiliza deltaPfx. Slope = diferencia del acumulado
    // entre i y i-cvdLookback: positivo = presión compradora neta, negativo = vendedora.
    // No necesita estructura extra; lookup O(1) en el loop.
    const useCVDFilter = p.useCVDFilter === true;
    const cvdLookback  = p.cvdLookback || 20;

    // Ballenas — ordenadas por tiempo para sliding window O(n)
    const whaleTrades = (whalesArr || [])
        .map(w => ({ ts: parseFloat(w.ts_sec) * 1000, btc: parseFloat(w.cantidad), isSell: w.es_venta }))
        .sort((a, b) => a.ts - b.ts);
    const whaleWindowMs = p.whaleWindow * 60000;
    let wLeft = 0, wRight = -1, wBuys = 0, wSells = 0;

    // Stop EMAs configurables (modo Ruptura EMA): pre-compute una vez, lookup por barra en el loop
    const stopEMACfgs = p.stopType === 'Ruptura EMA'
        ? (Array.isArray(p.stopEMAs) && p.stopEMAs.length ? p.stopEMAs : [{ period:200, tf:'1m' }, { period:500, tf:'1m' }])
        : [];
    const stopEmaArrays = stopEMACfgs.map(({ period, tf }) => {
        if (tf === '5m') {
            const vals = calcEMA(c5m_pb, period);
            return { arr: null, tsArr: bars5m.map(b => parseInt(b[6])).sort((a,b)=>a-b), map: new Map(bars5m.map((b,i)=>[parseInt(b[6]),vals[i]])) };
        } else if (tf === '15m') {
            const vals = calcEMA(c15m, period);
            return { arr: null, tsArr: bars15m.map(b => parseInt(b[6])).sort((a,b)=>a-b), map: new Map(bars15m.map((b,i)=>[parseInt(b[6]),vals[i]])) };
        }
        return { arr: calcEMA(c1m, period), tsArr: null, map: null };
    });

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
        const alignVals = pbEMAConfig.map(({ period, tf }) => {
            if (tf === '1m')  return pbArr1m[period]?.[i] ?? null;
            if (tf === '5m')  return pbArr5m[period]  ? lookupHTF(pbArr5m[period].ts,  pbArr5m[period].map,  tsClose) : null;
            if (tf === '15m') return pbArr15m[period] ? lookupHTF(pbArr15m[period].ts, pbArr15m[period].map, tsClose) : null;
            return null;
        });
        if (alignVals.some(v => !v)) continue;
        const stopEmaVals = stopEmaArrays.map(s => s.arr ? s.arr[i] : lookupHTF(s.tsArr, s.map, tsClose)).filter(v => v != null);

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

            // Breakeven: el stop ya fue movido a entrada ± offset en una vela ANTERIOR (ver
            // disparo más abajo). Se evalúa antes que TP/SL/EMA: es un nivel duro intra-vela
            // que ejecuta antes que cualquier chequeo al cierre. Si la misma vela toca TP y
            // breakeven, prioriza breakeven (misma convención conservadora que el caso TP+SL).
            if (!exitPrice && pos.beAplicado) {
                if      (pos.side === 1  && low  <= pos.sl) { exitPrice = pos.sl; exitReason = 'BE'; }
                else if (pos.side === -1 && high >= pos.sl) { exitPrice = pos.sl; exitReason = 'BE'; }
            }

            if (!exitPrice) {
                if (pos.side === 1) {
                    if (p.stopType === 'Porcentaje') {
                        if      (high >= pos.tp && low <= pos.sl) { exitPrice = pos.sl; exitReason = 'SL'; }
                        else if (high >= pos.tp)                   { exitPrice = pos.tp; exitReason = 'TP'; }
                        else if (low  <= pos.sl)                   { exitPrice = pos.sl; exitReason = 'SL'; }
                    } else {
                        // Si rompe alguna stop EMA al cierre y también toca TP, prioriza la ruptura.
                        if      (stopEmaVals.some(v => close < v)) { exitPrice = close;  exitReason = 'EMA'; }
                        else if (high >= pos.tp)                    { exitPrice = pos.tp; exitReason = 'TP'; }
                    }
                    if (!exitPrice && p.useMaxTradeTime && barsIn >= p.maxTradeMinutes) { exitPrice = close; exitReason = 'Tiempo'; }
                } else {
                    if (p.stopType === 'Porcentaje') {
                        if      (low <= pos.tp && high >= pos.sl) { exitPrice = pos.sl; exitReason = 'SL'; }
                        else if (low  <= pos.tp)                   { exitPrice = pos.tp; exitReason = 'TP'; }
                        else if (high >= pos.sl)                   { exitPrice = pos.sl; exitReason = 'SL'; }
                    } else {
                        // Si rompe alguna stop EMA al cierre y también toca TP, prioriza la ruptura.
                        if      (stopEmaVals.some(v => close > v)) { exitPrice = close;   exitReason = 'EMA'; }
                        else if (low <= pos.tp)                     { exitPrice = pos.tp;  exitReason = 'TP'; }
                    }
                    if (!exitPrice && p.useMaxTradeTime && barsIn >= p.maxTradeMinutes) { exitPrice = close; exitReason = 'Tiempo'; }
                }
            }

            if (exitPrice) {
                const raw = pos.side === 1
                    ? (exitPrice - pos.entry) / pos.entry
                    : (pos.entry - exitPrice) / pos.entry;
                const palanca = p.palancaActivo ? (p.palancaValor || 1) : 1;
                const net = raw * palanca - costoOperacion(p, palanca, barsIn, pos.side);
                // En margen aislado la liquidación consume todo el margen (el de
                // mantenimiento restante se lo lleva el fee de liquidación): pérdida = -margen.
                // Sin este caso especial, a palanca alta se sub-contabiliza la pérdida.
                const pnlAbs = exitReason === 'LIQ'
                    ? -pos.capitalAtEntry
                    : Math.max(pos.capitalAtEntry * net, -pos.capitalAtEntry);
                capital += pnlAbs;
                trades.push({ type: pos.side === 1 ? 'Long' : 'Short', entryTs: parseInt(bars1m[pos.entryBarIdx][0]), exitTs: ts, entryPrice: pos.entry, exitPrice, tp: pos.tp, sl: pos.sl, pnlPerc: (pnlAbs / pos.capitalAtEntry) * 100, pnlAbs, reason: exitReason, capital, oiSlope: pos.oiSlope, topRatio: pos.topRatio, globalRatio: pos.globalRatio });
                lastClosedBarIdx = i;
                posiciones.splice(j, 1);
            } else if (p.useBreakeven && !pos.beAplicado) {
                // Disparo del breakeven: si la vela avanzó el % de trigger a favor, mover el
                // stop a entrada ± offset (el offset cubre comisión + slippage de ida y vuelta).
                // Rige recién desde la PRÓXIMA vela: dentro de una vela 1m no se sabe si el
                // extremo favorable ocurrió antes o después del retroceso, así que asumir
                // disparo-y-salida en la misma vela sería look-ahead optimista.
                const disparo = pos.side === 1
                    ? high >= pos.entry * (1 + p.breakevenTrigger / 100)
                    : low  <= pos.entry * (1 - p.breakevenTrigger / 100);
                if (disparo) {
                    pos.beAplicado = true;
                    pos.sl = pos.side === 1
                        ? pos.entry * (1 + p.breakevenOffset / 100)
                        : pos.entry * (1 - p.breakevenOffset / 100);
                }
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
            const argDate = new Date(ts - 3 * 3600000); // horario Argentina (UTC-3)
            const barHour = argDate.getUTCHours();
            const argDay  = argDate.getUTCDay(); // 0=Dom, 6=Sáb
            const barsSinceClose = lastClosedBarIdx !== null ? i - lastClosedBarIdx : 999999;

            if (
                barHour >= p.startHour && barHour < p.endHour &&
                (p.operaFinDeSemana || (argDay !== 0 && argDay !== 6)) &&
                !(p.useCooldown && barsSinceClose < p.cooldownMinutes)
            ) {
                const above = alignVals.every(v => close > v);
                const below = alignVals.every(v => close < v);
                const bullAlign = alignVals.length < 2 || alignVals.every((v, j) => j === 0 || alignVals[j-1] > v);
                const bearAlign = alignVals.length < 2 || alignVals.every((v, j) => j === 0 || alignVals[j-1] < v);
                const pbVals = alignVals;
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

                // ADX: mide fuerza de tendencia (>= umbral = tendencia fuerte), en su temporalidad propia
                const adxRaw = !p.useADXFilter ? null
                    : adxTf === '1m' ? adxDirect[i] : lookupHTF(tsAdx, adxByTs, tsClose);
                const adxOk  = !p.useADXFilter || (adxRaw != null && adxRaw >= (p.adxThreshold ?? 25));

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

                // Angulación de EMA — pendiente de la EMA en su temporalidad propia
                const emaAngSlope = !useEmaAngFilter ? null
                    : emaAngTf === '1m' ? emaAngDirect[i] : lookupHTF(tsEmaAng, emaAngByTs, tsClose);
                const emaAngOkLong  = !useEmaAngFilter || (emaAngSlope !== null && emaAngSlope >=  emaAngGate);
                const emaAngOkShort = !useEmaAngFilter || (emaAngSlope !== null && emaAngSlope <= -emaAngGate);

                // Open Interest — confirma que el OI viene subiendo (dinero nuevo). Aplica a ambos lados.
                let oiOk = !useOIFilter, oiSlopeVal = null;
                if (useOIFilter && oiTs) {
                    const oiNow  = lookupHTF(oiTs, oiByTs, tsClose);
                    const oiPast = lookupHTF(oiTs, oiByTs, tsClose - oiLookbackMs);
                    if (oiNow != null && oiPast != null && oiPast > 0) {
                        oiSlopeVal = (oiNow - oiPast) / oiPast * 100;
                        oiOk = oiSlopeVal >= oiThreshold;
                    } else {
                        oiOk = false;
                    }
                }

                // Posicionamiento — top traders (seguir) y retail global (fade), por nivel.
                const topRatioVal  = (useTopTraderFilter || useTopSlopeFilter) && topTs  ? lookupHTF(topTs,  topByTs,  tsClose) : null;
                const globRatioVal = useRetailFilter    && globTs ? lookupHTF(globTs, globByTs, tsClose) : null;
                const topOkLong    = !useTopTraderFilter || (topRatioVal  != null && topRatioVal  >= topTraderRatio);
                const topOkShort   = !useTopTraderFilter || (topRatioVal  != null && topRatioVal  <= 1 / topTraderRatio);
                const retailOkLong  = !useRetailFilter || (globRatioVal != null && globRatioVal <= retailExtreme);
                const retailOkShort = !useRetailFilter || (globRatioVal != null && globRatioVal >= 1 / retailExtreme);

                // Pendiente del ratio de top traders — smart money acumulando (slope > 0) ahora.
                const topRatioPast    = useTopSlopeFilter && topTs ? lookupHTF(topTs, topByTs, tsClose - topSlopeLookbackMs) : null;
                const topSlopeVal     = topRatioVal != null && topRatioPast != null ? topRatioVal - topRatioPast : null;
                const topSlopeOkLong  = !useTopSlopeFilter || (topSlopeVal !== null && topSlopeVal >  (p.topSlopeMin ?? 0));
                const topSlopeOkShort = !useTopSlopeFilter || (topSlopeVal !== null && topSlopeVal < -(p.topSlopeMin ?? 0));

                // CVD slope — net taker flow en los últimos cvdLookback velas (O(1) vía deltaPfx).
                const cvdSlope   = i >= cvdLookback ? deltaPfx[i + 1] - deltaPfx[i + 1 - cvdLookback] : null;
                const cvdOkLong  = !useCVDFilter || (cvdSlope !== null && cvdSlope > 0);
                const cvdOkShort = !useCVDFilter || (cvdSlope !== null && cvdSlope < 0);

                if (p.enableLongs && above && alignLong && (!useRsiFilter || rsiVal >= rsiLongMin) && (!useMacdFilter || macd5 > sig5) && pullOK && deltaOkLong && whaleOkLong && adxOk && vwapOkLong && emaAngOkLong && oiOk && topOkLong && retailOkLong && topSlopeOkLong && cvdOkLong) {
                    const capEntrada = calcCapitalEntrada(p, capital, posiciones);
                    if (capEntrada <= 0) { /* sin capital disponible, no entrar */ }
                    else {
                        const tp = close * (1 + p.tpPerc / 100);
                        const sl = p.stopType === 'Porcentaje' ? close * (1 - p.slPerc / 100) : (stopEmaVals.length ? Math.max(...stopEmaVals) : close * 0.99);
                        posiciones.push({ side: 1, entry: close, entryBarIdx: i, tp, sl, capitalAtEntry: capEntrada, oiSlope: oiSlopeVal, topRatio: topRatioVal, globalRatio: globRatioVal });
                    }
                } else if (p.enableShorts && below && alignShort && (!useRsiFilter || rsiVal <= rsiShortMax) && (!useMacdFilter || macd5 < sig5) && pullOK && deltaOkShort && whaleOkShort && adxOk && vwapOkShort && emaAngOkShort && oiOk && topOkShort && retailOkShort && topSlopeOkShort && cvdOkShort) {
                    const capEntrada = calcCapitalEntrada(p, capital, posiciones);
                    if (capEntrada <= 0) { /* sin capital disponible, no entrar */ }
                    else {
                        const tp = close * (1 - p.tpPerc / 100);
                        const sl = p.stopType === 'Porcentaje' ? close * (1 + p.slPerc / 100) : (stopEmaVals.length ? Math.min(...stopEmaVals) : close * 1.01);
                        posiciones.push({ side: -1, entry: close, entryBarIdx: i, tp, sl, capitalAtEntry: capEntrada, oiSlope: oiSlopeVal, topRatio: topRatioVal, globalRatio: globRatioVal });
                    }
                }
            }
        }

        // Marcado a mercado de la vela: equity realizada + PnL no realizado de todas las posiciones abiertas
        let markedEquity = capital;
        for (const pos of posiciones) {
            const raw = pos.side === 1 ? (close - pos.entry) / pos.entry : (pos.entry - close) / pos.entry;
            const palanca = p.palancaActivo ? (p.palancaValor || 1) : 1;
            markedEquity += Math.max(pos.capitalAtEntry * (raw * palanca - costoOperacion(p, palanca, i - pos.entryBarIdx, pos.side)), -pos.capitalAtEntry);
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
            const net = raw * palanca - costoOperacion(p, palanca, barsIn, pos.side);
            const pnlAbs = Math.max(pos.capitalAtEntry * net, -pos.capitalAtEntry);
            capital += pnlAbs;
            trades.push({ type: pos.side === 1 ? 'Long' : 'Short', entryTs: parseInt(bars1m[pos.entryBarIdx][0]), exitTs: lastTs, entryPrice: pos.entry, exitPrice: lastClose, tp: pos.tp, sl: pos.sl, pnlPerc: (pnlAbs / pos.capitalAtEntry) * 100, pnlAbs, reason: 'Fin', capital, oiSlope: pos.oiSlope, topRatio: pos.topRatio, globalRatio: pos.globalRatio });
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
        marginType: (row.margin_type || 'ISOLATED').toUpperCase(),
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
    const rClose = await ejecutarOrdenBinance(ctx, buildCloseUrl(ctx, pos.lado, pos.qty), `CIERRE-${razon}`);
    // Corregir precio_cierre con el fill real si la orden de cierre se ejecutó.
    if (rClose.ok && rClose.body?.avgPrice) {
        const fillClose = parseFloat(rClose.body.avgPrice);
        if (fillClose > 0) await pool.query(`UPDATE auto_trading_entradas SET precio_cierre=$1 WHERE id=$2`, [fillClose, pos.id]);
    }
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
async function gestionarPosicionAbierta(ctx, p, bars1m, bars5m, bars15m) {
    const arr = posDe(ctx.uid);
    if (arr.length === 0) return;
    const ahora = Date.now();

    const stopEMA = (p.stopType === 'Ruptura EMA' || p.stopType === 'Ruptura EMA 200' || p.stopType === 'Ruptura EMA 500');
    let precioActual = precioDeCtx(ctx), stopEmaVals = [];
    if (stopEMA && bars1m && bars1m.length >= 510) {
        const c1m_live = bars1m.map(b => parseFloat(b[4]));
        const last = bars1m.length - 1;
        precioActual = c1m_live[last];
        const tsClose_live = parseInt(bars1m[last][6]);
        const stopCfgs_live = (Array.isArray(p.stopEMAs) && p.stopEMAs.length)
            ? p.stopEMAs
            : p.stopType === 'Ruptura EMA 200' ? [{ period:200, tf:'1m' }]
            : p.stopType === 'Ruptura EMA 500' ? [{ period:500, tf:'1m' }]
            : [{ period:200, tf:'1m' }, { period:500, tf:'1m' }];
        for (const { period, tf } of stopCfgs_live) {
            let val = null;
            if (tf === '1m') {
                val = calcEMA(c1m_live, period)[last];
            } else if (tf === '5m' && bars5m && bars5m.length >= period) {
                const vals = calcEMA(bars5m.map(b => parseFloat(b[4])), period);
                const tsArr = bars5m.map(b => parseInt(b[6])).sort((a,b)=>a-b);
                val = lookupHTF(tsArr, new Map(bars5m.map((b,i)=>[parseInt(b[6]),vals[i]])), tsClose_live);
            } else if (tf === '15m' && bars15m && bars15m.length >= period) {
                const vals = calcEMA(bars15m.map(b => parseFloat(b[4])), period);
                const tsArr = bars15m.map(b => parseInt(b[6])).sort((a,b)=>a-b);
                val = lookupHTF(tsArr, new Map(bars15m.map((b,i)=>[parseInt(b[6]),vals[i]])), tsClose_live);
            }
            if (val != null) stopEmaVals.push(val);
        }
    }
    if (!precioActual && bars1m && bars1m.length) precioActual = parseFloat(bars1m[bars1m.length - 1][4]);
    if (!precioActual) return;

    const aCerrar = [];
    for (let i = arr.length - 1; i >= 0; i--) {
        const pos = arr[i];
        let razon = null;
        if (p.useMaxTradeTime && pos.entryTs && (ahora - pos.entryTs) / 60000 >= (p.maxTradeMinutes ?? 15)) {
            razon = 'Tiempo';
        } else if (stopEMA && stopEmaVals.length) {
            const rompe = pos.lado === 'long'
                ? stopEmaVals.some(v => precioActual < v)
                : stopEmaVals.some(v => precioActual > v);
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
    registrarSaludWS(`precio-${entorno}`, ws);

    ws.on('open', () => {
        monitorConectando.delete(entorno);
        console.log(`✅ Monitor de precio futuros (${entorno}) conectado.`);
    });

    ws.on('message', async (data) => {
        latidoWS(`precio-${entorno}`);
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
        const mt  = ctx.marginType || 'ISOLATED';
        const ts  = Date.now();
        const q   = `symbol=BTCUSDT&marginType=${mt}&timestamp=${ts}`;
        const url = `${ctx.base}/fapi/v1/marginType?${q}&signature=${firmarParams(q, ctx.secret)}`;
        const r   = await fetch(url, { method: 'POST', headers: { 'X-MBX-APIKEY': ctx.apiKey } });
        const b   = await r.json();
        if (r.ok || b.code === -4046) console.log(`[AutoTrading u${ctx.uid}] Margin type: ${mt} ✅`);
        else console.warn(`[AutoTrading u${ctx.uid}] ⚠️ No se pudo fijar margin type: ${b.code} ${b.msg}`);
    } catch (e) { console.error(`[AutoTrading u${ctx.uid}] Error margin type:`, e.message); }
}

// Reconcilia el estado en memoria de una cuenta con el exchange. Si el exchange está plano
// pero el libro tiene sub-posiciones, las marca como cerradas (el exchange las cerró vía
// sus propias órdenes Algo mientras el server no lo detectaba). Se llama al arrancar y
// periódicamente para eliminar la "posición fantasma" sin depender del reinicio.
async function reconciliarCuenta(ctx) {
    const arr = posDe(ctx.uid);
    try {
        const amt = await obtenerPosicionExchange(ctx);
        if (!arr.length) {
            // Caso inverso: libro vacío pero el exchange tiene posición. Pasa si el server se cae
            // entre el fill de la entrada y el INSERT en BD (posición sin registrar NI protegida),
            // o si el usuario operó manualmente en la misma cuenta. No se cierra automáticamente
            // (podría ser una posición manual legítima): se avisa fuerte en cada pasada.
            if (Math.abs(amt) >= 1e-8) {
                console.warn(`[AutoTrading u${ctx.uid}] 🚨 POSICIÓN HUÉRFANA: el exchange tiene ${amt} BTC pero el libro está vacío. Puede ser una entrada del bot sin registrar (sin TP/SL de protección) o una posición manual — revisar y cerrar/registrar a mano.`);
            }
            return;
        }
        if (Math.abs(amt) < 1e-8) {
            console.warn(`[AutoTrading u${ctx.uid}] ⚠️ Reconciliación: exchange PLANO pero libro con ${arr.length} sub-pos — marcando como cerradas.`);
            const ahoraMs = Date.now();
            for (const pos of arr) {
                await pool.query(
                    `UPDATE auto_trading_entradas SET estado='cerrada', razon_cierre='Exchange', ts_cierre=$1 WHERE id=$2`,
                    [ahoraMs, pos.id]
                );
            }
            posicionesPorCuenta.set(ctx.uid, []);
            await cancelarTodasLasOrdenes(ctx);
            await pool.query(`UPDATE cuentas_trading SET ultima_senal=NULL, ultima_cierre_ts=$1 WHERE usuario_id=$2`, [ahoraMs, ctx.uid]);
            await sincronizarPosicionBD(ctx.uid);
        } else {
            const sumLibro = arr.reduce((s, p) => s + p.qty, 0) * (arr[0].lado === 'long' ? 1 : -1);
            if (Math.abs(amt - sumLibro) > 0.0005) {
                console.warn(`[AutoTrading u${ctx.uid}] ⚠️ Discrepancia: exchange ${amt} BTC vs libro ${sumLibro} BTC. Revisar manualmente.`);
            }
        }
    } catch (e) { console.error(`[AutoTrading u${ctx.uid}] Error en reconciliación periódica:`, e.message); }
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
         (process.env.BINANCE_MARGIN_TYPE || 'ISOLATED').toUpperCase(), cfg.estrategia_nombre,
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
        // para todas las cuentas). Velas suficientes para que EMA/MACD/RSI/ADX converjan
        // (6000 de 1m = ~100 velas 1h derivadas, para que el ADX 1h converja como en backtest).
        const [bars1m, bars5m, bars15m] = await Promise.all([
            fetchKlinesBatch('1m',  6000),
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
    if (arr.length > 0) await gestionarPosicionAbierta(ctx, p, bars1m, bars5m, bars15m);

    // Cuenta apagada: solo gestionar salidas, no abrir nuevas entradas.
    if (!row.habilitado) return;

    // ¿Se permite abrir entrada? Sin posiciones, o con pyramiding habilitado.
    if (arr.length > 0 && !p.allowMultipleEntries) return;

    const whaleRes = await pool.query(
        `SELECT EXTRACT(EPOCH FROM fecha) as ts_sec, cantidad, es_venta
         FROM ballenas WHERE fecha >= NOW() - make_interval(mins => $1) AND cantidad >= $2 ORDER BY fecha ASC`,
        [(parseInt(p.whaleWindow) || 30) + 5, p.whaleMinBTC || 5]
    );

    const oiRows = p.useOIFilter
        ? (await pool.query(
            'SELECT tiempo, valor FROM open_interest WHERE tiempo >= EXTRACT(EPOCH FROM NOW())::bigint - $1 ORDER BY tiempo ASC',
            [((parseInt(p.oiLookbackMin) || 30) + 10) * 60]
          )).rows
        : [];

    const lsRows = (p.useTopTraderFilter || p.useRetailFilter || p.useTopSlopeFilter)
        ? (await pool.query(
            // Ventana suficiente para el lookback de la pendiente (mínimo 30 min para el nivel).
            'SELECT tiempo, top_pos, global_acc FROM long_short_ratio WHERE tiempo >= EXTRACT(EPOCH FROM NOW())::bigint - $1 ORDER BY tiempo ASC',
            [Math.max(1800, ((parseInt(p.topSlopeLookbackMin) || 15) + 10) * 60)]
          )).rows
        : [];

    // Cooldown: no entrar si pasó menos del tiempo configurado desde el último cierre.
    if (p.useCooldown && row.ultima_cierre_ts) {
        const min = (Date.now() - parseInt(row.ultima_cierre_ts)) / 60000;
        if (min < (p.cooldownMinutes ?? 45)) {
            console.log(`[AutoTrading u${row.usuario_id}] Cooldown — faltan ${((p.cooldownMinutes ?? 45) - min).toFixed(1)} min`);
            return;
        }
    }

    const resultado  = evaluarSenal(bars1m, bars5m, bars15m, whaleRes.rows, p, oiRows, lsRows);
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
    const notionalUsdt = qty * resultado.entry;
    if (notionalUsdt < 100) { console.log(`[AutoTrading u${row.usuario_id}] nocional $${notionalUsdt.toFixed(0)} < $100 mínimo Binance (-4164) — omitida`); return; }

    console.log(`[AutoTrading u${row.usuario_id}] Nueva señal: ${nuevaSenal.toUpperCase()} @ $${resultado.entry} | TP $${resultado.tp?.toFixed(0)} | SL $${resultado.sl?.toFixed(0)} | qty ${qty} BTC`);

    if (p.palancaActivo && p.palancaValor > 1) await setBinanceLeverage(ctx, p.palancaValor);

    const ordenEntrada = await ejecutarOrdenBinance(ctx, buildEntryUrl(ctx, nuevaSenal, qty), 'ENTRADA');
    if (ordenEntrada.ok) {
        const stopType = p.stopType ?? 'Porcentaje';
        // Usar el precio de fill real (avgPrice) en vez del cierre de vela estimado.
        const fillEntry = parseFloat(ordenEntrada.body?.avgPrice) || resultado.entry;
        const ins = await pool.query(
            `INSERT INTO auto_trading_entradas (ts, lado, precio_entrada, precio_tp, precio_sl, qty, stop_type, estado, usuario_id, account_id)
             VALUES ($1, $2, $3, $4, $5, $6, $7, 'abierta', $8, $8) RETURNING id`,
            [Date.now(), nuevaSenal, fillEntry, resultado.tp, resultado.sl, qty, stopType, row.usuario_id]
        );
        const sub = {
            id: ins.rows[0].id, lado: nuevaSenal, qty, entry: fillEntry,
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
                await reconciliarCuenta(ctx);
                // Si tras reconciliar siguen abiertas, recolocar protección a sub-pos sin órdenes.
                for (const sub of posDe(uid)) {
                    if (!sub.tpOrderId && !sub.slOrderId) await colocarProteccionExchange(ctx, sub);
                }
            } catch (e) { console.error(`[AutoTrading u${uid}] Error reconciliando al arrancar:`, e.message); }

            try { await asegurarConfiguracionCuenta(ctx); } catch (_) {}
        }
    } catch (e) {
        console.error('[AutoTrading] Error cargando posiciones desde BD:', e.message);
    }

    iniciarMonitorPrecio(WS_PRECIO_POR_ENTORNO.testnet, 'testnet');
    iniciarMonitorPrecio(WS_PRECIO_POR_ENTORNO.real,    'real');

    // Reconciliación periódica: detecta posiciones cerradas por el exchange (Algo TP/SL) y
    // posiciones huérfanas (exchange con posición, libro vacío) sin depender del reinicio.
    // Corre cada 5 minutos sobre todas las cuentas habilitadas con claves, tengan o no libro.
    setInterval(async () => {
        if (!ENCRYPTION_KEY) return;
        try {
            const r = await pool.query('SELECT * FROM cuentas_trading WHERE api_key IS NOT NULL');
            for (const row of r.rows) {
                const uid = row.usuario_id;
                if (!row.habilitado && !posDe(uid).length) continue;
                let ctx = ctxActivos.get(uid);
                if (!ctx) {
                    try { ctx = ctxDeCuenta(row); ctxActivos.set(uid, ctx); }
                    catch (_) { continue; }
                }
                await reconciliarCuenta(ctx).catch(() => {});
            }
        } catch (e) { console.error('[AutoTrading] Error en pasada de reconciliación:', e.message); }
    }, 5 * 60 * 1000);

    ejecutarAutoTrading();
    // Alinear el ciclo al cierre de vela (+2 s de buffer) para minimizar el slippage
    // estructural respecto al backtest, que entra exactamente al cierre.
    const msToNextClose = 60000 - (Date.now() % 60000) + 2000;
    setTimeout(() => {
        ejecutarAutoTrading();
        setInterval(ejecutarAutoTrading, 60 * 1000);
    }, msToNextClose);
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
function evaluarSenal(bars1m, bars5m, bars15m, whalesArr, p, oiArr, lsArr) {
    if (bars1m.length < 510) return { signal: null, reason: 'datos_insuficientes' };

    const c1m  = bars1m.map(b => parseFloat(b[4]));

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

    // ADX — temporalidad configurable (1m/5m/15m/1h), idéntico al backtest. La de 1h
    // se deriva agregando las velas 1m del ciclo.
    const adxTf_sn = ['1m', '5m', '15m', '1h'].includes(p.adxTf) ? p.adxTf : '15m';
    let adxValue = null;
    if (adxTf_sn === '1m') {
        adxValue = calcADX(bars1m.map(b => parseFloat(b[2])), bars1m.map(b => parseFloat(b[3])), c1m)[bars1m.length - 1];
    } else {
        const barsAdx = adxTf_sn === '5m' ? bars5m : adxTf_sn === '15m' ? bars15m : agregarVelas1m(bars1m, 3600000);
        const arr = calcADX(barsAdx.map(b => parseFloat(b[2])), barsAdx.map(b => parseFloat(b[3])), barsAdx.map(b => parseFloat(b[4])));
        const adxByTs_sn = new Map(barsAdx.map((b, idx) => [parseInt(b[6]), arr[idx]]));
        const adxTs_sn   = [...adxByTs_sn.keys()].sort((a, b) => a - b);
        adxValue = lookupHTF(adxTs_sn, adxByTs_sn, parseInt(bars1m[bars1m.length - 1][6]));
    }

    // Angulación de EMA — pendiente normalizada por ATR (idéntico al backtest)
    const useEmaAngFilter_sn = p.useEmaAngFilter === true;
    const emaAngTf_sn        = p.emaAngTf        || '15m';
    const emaAngLen_sn       = p.emaAngLen       || 200;
    const emaAngSlopeBars_sn = p.emaAngSlopeBars || 10;
    const emaAngAtr_sn       = p.emaAngAtr       || 14;
    const emaAngGate_sn      = p.emaAngMode === 'strong'
        ? (p.emaAngStrongSlope ?? 0.60)
        : (p.emaAngMinSlope    ?? 0.25);
    let emaAngSnDirect = null, emaAngSnByTs = null, emaAngSnTs = null;
    if (useEmaAngFilter_sn) {
        if (emaAngTf_sn === '1m') {
            emaAngSnDirect = calcEMAangSlope(c1m, bars1m.map(b => parseFloat(b[2])), bars1m.map(b => parseFloat(b[3])), emaAngLen_sn, emaAngAtr_sn, emaAngSlopeBars_sn);
        } else if (emaAngTf_sn === '5m') {
            const arr = calcEMAangSlope(c5m_sn, bars5m.map(b => parseFloat(b[2])), bars5m.map(b => parseFloat(b[3])), emaAngLen_sn, emaAngAtr_sn, emaAngSlopeBars_sn);
            emaAngSnByTs = new Map(bars5m.map((b, i) => [parseInt(b[6]), arr[i]]));
            emaAngSnTs   = [...emaAngSnByTs.keys()].sort((a, b) => a - b);
        } else {
            const arr = calcEMAangSlope(c15m_sn, bars15m.map(b => parseFloat(b[2])), bars15m.map(b => parseFloat(b[3])), emaAngLen_sn, emaAngAtr_sn, emaAngSlopeBars_sn);
            emaAngSnByTs = new Map(bars15m.map((b, i) => [parseInt(b[6]), arr[i]]));
            emaAngSnTs   = [...emaAngSnByTs.keys()].sort((a, b) => a - b);
        }
    }

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

    const alignVals = pbEMAConfig.map(({ period, tf }) => {
        if (tf === '1m')  return pbSn1m[period]?.[i] ?? null;
        if (tf === '5m')  return pbSn5m[period]  ? lookupHTF(pbSn5m[period].ts,  pbSn5m[period].map,  tsClose) : null;
        if (tf === '15m') return pbSn15m[period] ? lookupHTF(pbSn15m[period].ts, pbSn15m[period].map, tsClose) : null;
        return null;
    });
    if (alignVals.some(v => !v)) return { signal: null, reason: 'emas_no_calentadas', indicadores: {} };

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

    const argDate  = new Date(ts - 3 * 3600000); // horario Argentina (UTC-3)
    const barHour  = argDate.getUTCHours();
    const argDay   = argDate.getUTCDay(); // 0=Dom, 6=Sáb
    const horarioOk = barHour >= (p.startHour ?? 9) && barHour < (p.endHour ?? 20)
                   && (p.operaFinDeSemana || (argDay !== 0 && argDay !== 6));

    const above     = alignVals.every(v => close > v);
    const below     = alignVals.every(v => close < v);
    const bullAlign = alignVals.length < 2 || alignVals.every((v, j) => j === 0 || alignVals[j-1] > v);
    const bearAlign = alignVals.length < 2 || alignVals.every((v, j) => j === 0 || alignVals[j-1] < v);

    const pbSnVals = alignVals;
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

    // CVD slope — net taker flow acumulado en los últimos cvdLookback velas de 1m.
    // Comparte la misma fuente de datos que deltaRolling pero con una ventana más larga
    // para capturar la dirección del order flow en el contexto de velas recientes.
    const useCVDFilter_sn = p.useCVDFilter === true;
    const cvdLookback_sn  = p.cvdLookback || 20;
    let cvdSlope_sn = null;
    if (useCVDFilter_sn) {
        const start = Math.max(0, bars1m.length - cvdLookback_sn);
        let acc = 0;
        for (let k = start; k < bars1m.length; k++) {
            acc += 2 * parseFloat(bars1m[k][9]) - parseFloat(bars1m[k][5]);
        }
        cvdSlope_sn = acc;
    }
    const cvdOkLong_sn  = !useCVDFilter_sn || (cvdSlope_sn !== null && cvdSlope_sn > 0);
    const cvdOkShort_sn = !useCVDFilter_sn || (cvdSlope_sn !== null && cvdSlope_sn < 0);

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

    const adxOk = !p.useADXFilter || (adxValue != null && adxValue >= (p.adxThreshold ?? 25));

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

    // Angulación de EMA — pendiente de la EMA en su temporalidad propia
    const emaAngSlope_sn = !useEmaAngFilter_sn ? null
        : emaAngTf_sn === '1m' ? emaAngSnDirect[i] : lookupHTF(emaAngSnTs, emaAngSnByTs, tsClose);
    const emaAngOkLong_sn  = !useEmaAngFilter_sn || (emaAngSlope_sn !== null && emaAngSlope_sn >=  emaAngGate_sn);
    const emaAngOkShort_sn = !useEmaAngFilter_sn || (emaAngSlope_sn !== null && emaAngSlope_sn <= -emaAngGate_sn);

    // Open Interest — confirma que el OI viene subiendo (idéntico al backtest). Aplica a ambos lados.
    const useOIFilter_sn = p.useOIFilter === true;
    const oiLookbackMs_sn = (p.oiLookbackMin || 30) * 60000;
    const oiThreshold_sn  = Number.isFinite(p.oiThreshold) ? p.oiThreshold : 0.5;
    let oiSlope_sn = null, oiOk_sn = !useOIFilter_sn;
    if (useOIFilter_sn && Array.isArray(oiArr) && oiArr.length) {
        const oiByTs_sn = new Map();
        for (const o of oiArr) oiByTs_sn.set(Number(o.tiempo) * 1000, parseFloat(o.valor));
        const oiTs_sn = [...oiByTs_sn.keys()].sort((a, b) => a - b);
        const oiNow  = lookupHTF(oiTs_sn, oiByTs_sn, tsClose);
        const oiPast = lookupHTF(oiTs_sn, oiByTs_sn, tsClose - oiLookbackMs_sn);
        if (oiNow != null && oiPast != null && oiPast > 0) {
            oiSlope_sn = (oiNow - oiPast) / oiPast * 100;
            oiOk_sn = oiSlope_sn >= oiThreshold_sn;
        }
    }

    // Posicionamiento de traders — por nivel del ratio (idéntico al backtest).
    const useTopTraderFilter_sn = p.useTopTraderFilter === true;
    const useRetailFilter_sn    = p.useRetailFilter === true;
    const useTopSlopeFilter_sn  = p.useTopSlopeFilter === true;
    const topSlopeLookbackMs_sn = (p.topSlopeLookbackMin ?? 15) * 60000;
    const topTraderRatio_sn = Number.isFinite(p.topTraderRatio) ? p.topTraderRatio : 1.05;
    const retailExtreme_sn  = Number.isFinite(p.retailExtreme)  ? p.retailExtreme  : 2.0;
    let topRatioVal_sn = null, globRatioVal_sn = null;
    if ((useTopTraderFilter_sn || useRetailFilter_sn || useTopSlopeFilter_sn) && Array.isArray(lsArr) && lsArr.length) {
        const topByTs_sn = new Map(), globByTs_sn = new Map();
        for (const r of lsArr) {
            const tms = Number(r.tiempo) * 1000;
            if (r.top_pos    != null) topByTs_sn.set(tms, parseFloat(r.top_pos));
            if (r.global_acc != null) globByTs_sn.set(tms, parseFloat(r.global_acc));
        }
        const topTs_sn  = [...topByTs_sn.keys()].sort((a, b) => a - b);
        const globTs_sn = [...globByTs_sn.keys()].sort((a, b) => a - b);
        if ((useTopTraderFilter_sn || useTopSlopeFilter_sn) && topTs_sn.length) topRatioVal_sn  = lookupHTF(topTs_sn,  topByTs_sn,  tsClose);
        if (useRetailFilter_sn    && globTs_sn.length) globRatioVal_sn = lookupHTF(globTs_sn, globByTs_sn, tsClose);
    }
    const topOkLong_sn    = !useTopTraderFilter_sn || (topRatioVal_sn  != null && topRatioVal_sn  >= topTraderRatio_sn);
    const topOkShort_sn   = !useTopTraderFilter_sn || (topRatioVal_sn  != null && topRatioVal_sn  <= 1 / topTraderRatio_sn);
    const retailOkLong_sn  = !useRetailFilter_sn || (globRatioVal_sn != null && globRatioVal_sn <= retailExtreme_sn);
    const retailOkShort_sn = !useRetailFilter_sn || (globRatioVal_sn != null && globRatioVal_sn >= 1 / retailExtreme_sn);

    // Pendiente del ratio de top traders — smart money acumulando AHORA vs hace N minutos.
    let topSlopeVal_sn = null;
    if (useTopSlopeFilter_sn && topRatioVal_sn != null) {
        const topByTs_temp = new Map(), topTs_temp = [];
        if (Array.isArray(lsArr)) {
            for (const r of lsArr) {
                if (r.top_pos != null) { const t = Number(r.tiempo) * 1000; topByTs_temp.set(t, parseFloat(r.top_pos)); topTs_temp.push(t); }
            }
            topTs_temp.sort((a, b) => a - b);
        }
        const topPast_sn = topTs_temp.length ? lookupHTF(topTs_temp, topByTs_temp, tsClose - topSlopeLookbackMs_sn) : null;
        topSlopeVal_sn = topPast_sn != null ? topRatioVal_sn - topPast_sn : null;
    }
    const topSlopeOkLong_sn  = !useTopSlopeFilter_sn || (topSlopeVal_sn !== null && topSlopeVal_sn >  (p.topSlopeMin ?? 0));
    const topSlopeOkShort_sn = !useTopSlopeFilter_sn || (topSlopeVal_sn !== null && topSlopeVal_sn < -(p.topSlopeMin ?? 0));

    let signal = null;
    if (p.enableLongs !== false && horarioOk && above && alignLong && (!useRsiFilter_sn || rsiVal >= rsiLongMin_sn) && (!useMacdFilter_sn || macd5 > sig5) && nearEMA && deltaOkLong && whaleOkLong && adxOk && vwapOkLong_sn && emaAngOkLong_sn && oiOk_sn && topOkLong_sn && retailOkLong_sn && topSlopeOkLong_sn && cvdOkLong_sn)
        signal = 'long';
    else if (p.enableShorts !== false && horarioOk && below && alignShort && (!useRsiFilter_sn || rsiVal <= rsiShortMax_sn) && (!useMacdFilter_sn || macd5 < sig5) && nearEMA && deltaOkShort && whaleOkShort && adxOk && vwapOkShort_sn && emaAngOkShort_sn && oiOk_sn && topOkShort_sn && retailOkShort_sn && topSlopeOkShort_sn && cvdOkShort_sn)
        signal = 'short';

    const tpPerc = p.tpPerc ?? 0.5;
    const slPerc = p.slPerc ?? 1.0;
    const stopType = p.stopType ?? 'Porcentaje';

    const tp = signal === 'long'  ? close * (1 + tpPerc / 100) :
               signal === 'short' ? close * (1 - tpPerc / 100) : null;

    let sl = null;
    if (signal) {
        if (stopType === 'Porcentaje') {
            sl = signal === 'long' ? close * (1 - slPerc / 100) : close * (1 + slPerc / 100);
        } else {
            const stopCfgs_sl = Array.isArray(p.stopEMAs) && p.stopEMAs.length
                ? p.stopEMAs : [{ period:200, tf:'1m' }, { period:500, tf:'1m' }];
            const stopVals_sl = stopCfgs_sl.map(({ period, tf }) => {
                if (tf === '1m') return calcEMA(c1m, period)[i];
                const c = tf === '5m' ? c5m_sn : c15m_sn;
                const bars = tf === '5m' ? bars5m : bars15m;
                const vals = calcEMA(c, period);
                const map = new Map(bars.map((b, idx) => [parseInt(b[6]), vals[idx]]));
                const tsArr = bars.map(b => parseInt(b[6])).sort((a,b) => a-b);
                return lookupHTF(tsArr, map, tsClose);
            }).filter(v => v != null);
            sl = signal === 'long'
                ? (stopVals_sl.length ? Math.max(...stopVals_sl) : close * 0.99)
                : (stopVals_sl.length ? Math.min(...stopVals_sl) : close * 1.01);
        }
    }

    return {
        signal, timestamp: ts, entry: close, tp, sl,
        indicadores: { rsi15: rsiVal, rsiTf: rsiTf_sn, macd5, macdTf: macdTf_sn, adx: adxValue, adxTf: adxTf_sn, vwap: vwapVal_sn, vwapTf: vwapTf_sn, emaAngSlope: emaAngSlope_sn, emaAngTf: emaAngTf_sn, oiSlope: oiSlope_sn, topRatio: topRatioVal_sn, topSlope: topSlopeVal_sn, globalRatio: globRatioVal_sn, cvdSlope: cvdSlope_sn, horarioOk, above, below, nearEMA, deltaRolling, whaleDelta, alignVals }
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

        const [bars1m, bars5m, bars15m, whaleRes, oiRes, lsRes] = await Promise.all([
            // Suficientes velas para que EMA/MACD/RSI/ADX (incluso de período alto en HTF)
            // converjan igual que en el backtest y no diverja la señal en vivo
            // (6000 de 1m = ~100 velas 1h derivadas para el ADX 1h).
            fetchKlinesBatch('1m',  6000),
            fetchKlinesBatch('5m',  800),
            fetchKlinesBatch('15m', 800),
            pool.query(
                `SELECT EXTRACT(EPOCH FROM fecha) as ts_sec, cantidad, es_venta
                 FROM ballenas WHERE fecha >= NOW() - make_interval(mins => $1) AND cantidad >= $2 ORDER BY fecha ASC`,
                [(parseInt(p.whaleWindow) || 30) + 5, p.whaleMinBTC || 5]
            ),
            p.useOIFilter
                ? pool.query(
                    'SELECT tiempo, valor FROM open_interest WHERE tiempo >= EXTRACT(EPOCH FROM NOW())::bigint - $1 ORDER BY tiempo ASC',
                    [((parseInt(p.oiLookbackMin) || 30) + 10) * 60]
                  )
                : Promise.resolve({ rows: [] }),
            (p.useTopTraderFilter || p.useRetailFilter || p.useTopSlopeFilter)
                ? pool.query(
                    'SELECT tiempo, top_pos, global_acc FROM long_short_ratio WHERE tiempo >= EXTRACT(EPOCH FROM NOW())::bigint - $1 ORDER BY tiempo ASC',
                    [Math.max(1800, ((parseInt(p.topSlopeLookbackMin) || 15) + 10) * 60)]
                  )
                : Promise.resolve({ rows: [] }),
        ]);

        res.json(evaluarSenal(bars1m, bars5m, bars15m, whaleRes.rows, p, oiRes.rows, lsRes.rows));
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

// Límite de backtests simultáneos: una corrida de 365 días carga ~525k velas 1m más todos
// los indicadores en memoria; varias en paralelo pueden tirar el proceso entero (y con él,
// el auto-trading y los recolectores). Los excedentes reciben 429 y la UI muestra el mensaje.
let backtestsActivos = 0;
const MAX_BACKTESTS_SIMULTANEOS = 2;

app.post('/api/backtest', autenticar, async (req, res) => {
    if (backtestsActivos >= MAX_BACKTESTS_SIMULTANEOS) {
        return res.status(429).json({ error: `Ya hay ${MAX_BACKTESTS_SIMULTANEOS} backtests corriendo; esperá a que terminen y reintentá.` });
    }
    backtestsActivos++;
    try {
        const p = {
            enableLongs:       req.body.enableLongs !== false,
            enableShorts:      req.body.enableShorts !== false,
            tpPerc:            parseFloat(req.body.tpPerc)  || 0.5,
            stopType:          (() => { const t = req.body.stopType || 'Porcentaje'; return (t === 'Ruptura EMA 200' || t === 'Ruptura EMA 500') ? 'Ruptura EMA' : t; })(),
            slPerc:            parseFloat(req.body.slPerc)  || 1.0,
            useBreakeven:      req.body.useBreakeven === true,
            breakevenTrigger:  Number.isFinite(parseFloat(req.body.breakevenTrigger)) ? parseFloat(req.body.breakevenTrigger) : 0.3,
            breakevenOffset:   Number.isFinite(parseFloat(req.body.breakevenOffset))  ? parseFloat(req.body.breakevenOffset)  : 0.12,
            stopEMAs:          (() => {
                const t = req.body.stopType;
                if (t === 'Ruptura EMA 200') return [{ period:200, tf:'1m' }, { period:200, tf:'1m' }];
                if (t === 'Ruptura EMA 500') return [{ period:500, tf:'1m' }, { period:500, tf:'1m' }];
                return Array.isArray(req.body.stopEMAs) && req.body.stopEMAs.length
                    ? req.body.stopEMAs.slice(0,2).map(e => ({ period: Math.max(1, parseInt(e.period)||200), tf: ['1m','5m','15m'].includes(e.tf) ? e.tf : '1m' }))
                    : [{ period:200, tf:'1m' }, { period:500, tf:'1m' }];
            })(),
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
            useEmaAngFilter:   req.body.useEmaAngFilter === true,
            emaAngTf:          ['1m','5m','15m'].includes(req.body.emaAngTf) ? req.body.emaAngTf : '15m',
            emaAngLen:         parseInt(req.body.emaAngLen)       || 200,
            emaAngSlopeBars:   parseInt(req.body.emaAngSlopeBars) || 10,
            emaAngAtr:         parseInt(req.body.emaAngAtr)       || 14,
            emaAngMinSlope:    Number.isFinite(parseFloat(req.body.emaAngMinSlope))    ? parseFloat(req.body.emaAngMinSlope)    : 0.25,
            emaAngStrongSlope: Number.isFinite(parseFloat(req.body.emaAngStrongSlope)) ? parseFloat(req.body.emaAngStrongSlope) : 0.60,
            emaAngMode:        req.body.emaAngMode === 'strong' ? 'strong' : 'min',
            useOIFilter:       req.body.useOIFilter === true,
            oiLookbackMin:     Math.min(Math.max(parseInt(req.body.oiLookbackMin) || 30, 5), 1440),
            oiThreshold:       Number.isFinite(parseFloat(req.body.oiThreshold)) ? parseFloat(req.body.oiThreshold) : 0.5,
            useTopTraderFilter: req.body.useTopTraderFilter === true,
            topTraderRatio:     Number.isFinite(parseFloat(req.body.topTraderRatio)) ? parseFloat(req.body.topTraderRatio) : 1.05,
            useRetailFilter:    req.body.useRetailFilter === true,
            retailExtreme:      Number.isFinite(parseFloat(req.body.retailExtreme)) ? parseFloat(req.body.retailExtreme) : 2.0,
            useTopSlopeFilter:  req.body.useTopSlopeFilter === true,
            topSlopeLookbackMin: Math.min(Math.max(parseInt(req.body.topSlopeLookbackMin) || 15, 5), 1440),
            topSlopeMin:        Number.isFinite(parseFloat(req.body.topSlopeMin)) ? parseFloat(req.body.topSlopeMin) : 0,
            useCVDFilter:       req.body.useCVDFilter === true,
            cvdLookback:        Math.min(Math.max(parseInt(req.body.cvdLookback) || 20, 2), 1440),
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
            adxTf:               ['1m','5m','15m','1h'].includes(req.body.adxTf) ? req.body.adxTf : '15m',
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
        // Un offset >= trigger pondría el stop de breakeven en o por encima del precio de
        // disparo: la posición cerraría casi seguro en la vela siguiente. Configuración inválida.
        if (p.useBreakeven && p.breakevenOffset >= p.breakevenTrigger) {
            return res.status(400).json({ error: 'Breakeven: el offset debe ser menor que el % de disparo.' });
        }
        const days = Math.min(Math.max(parseInt(req.body.lookbackDays) || 7, 1), 365);
        const periodStart = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
        // Fuente de velas: 'bd' (cache local, default) o 'binance' (descarga en vivo).
        // El toggle en /estrategias permite comparar ambas para validar que coinciden.
        const fuente = req.body.fuenteDatos === 'binance' ? 'binance' : 'bd';
        const cargarKlines = fuente === 'binance'
            ? (tf, n) => fetchKlinesBatch(tf, n)
            : (tf)    => fetchKlinesDesdeBD(tf, days);
        // Serie de OI del período (solo si el filtro está activo). Traemos un poco antes del inicio
        // para que el lookback de las primeras velas tenga muestra previa.
        const oiDesdeSeg = Math.floor((periodStart.getTime() - (p.oiLookbackMin || 30) * 60000) / 1000);
        const [bars1m, bars5m, bars15m, whaleRes, oiRes, lsRes] = await Promise.all([
            cargarKlines('1m',  days * 1440),
            cargarKlines('5m',  days * 288),
            cargarKlines('15m', days * 96),
            pool.query(
                `SELECT EXTRACT(EPOCH FROM fecha) as ts_sec, cantidad, es_venta
                 FROM ballenas WHERE fecha >= $1 AND cantidad >= $2 ORDER BY fecha ASC`,
                [periodStart.toISOString(), p.whaleMinBTC]
            ),
            p.useOIFilter
                ? pool.query('SELECT tiempo, valor FROM open_interest WHERE tiempo >= $1 ORDER BY tiempo ASC', [oiDesdeSeg])
                : Promise.resolve({ rows: [] }),
            (p.useTopTraderFilter || p.useRetailFilter || p.useTopSlopeFilter)
                // Margen extra hacia atrás para que el lookback de la pendiente tenga muestra previa
                // en las primeras velas del período (igual que el OI con oiDesdeSeg).
                ? pool.query('SELECT tiempo, top_pos, global_acc FROM long_short_ratio WHERE tiempo >= $1 ORDER BY tiempo ASC', [Math.floor((periodStart.getTime() - (p.topSlopeLookbackMin + 10) * 60000) / 1000)])
                : Promise.resolve({ rows: [] }),
        ]);
        if (fuente === 'bd' && bars1m.length === 0) {
            throw new Error('La BD todavía no tiene velas cacheadas (el backfill inicial puede tardar unos minutos). Probá de nuevo en un rato o cambiá la fuente a Binance.');
        }
        const resultado = runBacktest(bars1m, bars5m, bars15m, whaleRes.rows, p, oiRes.rows, lsRes.rows);
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
        if (p.useOIFilter) {
            const oiCov = await pool.query('SELECT MIN(tiempo) AS primera, COUNT(*)::int AS n FROM open_interest');
            const primeraOI = oiCov.rows[0] && oiCov.rows[0].primera ? Number(oiCov.rows[0].primera) * 1000 : null;
            if (!primeraOI || oiCov.rows[0].n === 0) {
                warnings.push('Filtro de Open Interest activo pero no hay datos de OI guardados: ningún trade pasará el filtro.');
            } else if (primeraOI > periodStart.getTime()) {
                const diasCubiertos = Math.max(0, (Date.now() - primeraOI) / 86400000);
                warnings.push(`Filtro de Open Interest activo: solo hay datos de OI desde ${new Date(primeraOI).toISOString().slice(0, 16).replace('T', ' ')} UTC (~${diasCubiertos.toFixed(1)} días). Binance solo expone OI de los últimos ~30 días, así que el tramo anterior del período NO genera trades; las métricas reflejan solo el subperíodo con cobertura de OI.`);
            }
        }
        if (p.useTopTraderFilter || p.useRetailFilter || p.useTopSlopeFilter) {
            const lsCov = await pool.query('SELECT MIN(tiempo) AS primera, COUNT(*)::int AS n FROM long_short_ratio');
            const primeraLS = lsCov.rows[0] && lsCov.rows[0].primera ? Number(lsCov.rows[0].primera) * 1000 : null;
            if (!primeraLS || lsCov.rows[0].n === 0) {
                warnings.push('Filtro de Posicionamiento activo pero no hay datos de ratios guardados: ningún trade pasará el filtro.');
            } else if (primeraLS > periodStart.getTime()) {
                const diasCubiertos = Math.max(0, (Date.now() - primeraLS) / 86400000);
                warnings.push(`Filtro de Posicionamiento activo: solo hay datos de ratios desde ${new Date(primeraLS).toISOString().slice(0, 16).replace('T', ' ')} UTC (~${diasCubiertos.toFixed(1)} días). Binance solo expone los últimos ~30 días, así que el tramo anterior NO genera trades; las métricas reflejan solo el subperíodo con cobertura.`);
            }
        }
        resultado.warnings = warnings;

        res.json(resultado);
    } catch (err) {
        console.error('Error backtest:', err);
        res.status(500).json({ error: err.message || 'Error al ejecutar backtest' });
    } finally {
        backtestsActivos--;
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


// ── Snapshot de tramo ──────────────────────────────────────────────────────
// "Foto" completa de un rango del gráfico para analizarlo fuera de la terminal
// (p.ej. pegándolo en un LLM): velas + indicadores + ballenas + OI + posiciona-
// miento, con un resumen agregado y eventos destacados. Tres capas: resumen
// (lectura rápida), eventos (qué pasó puntualmente) y series (detalle fino).
app.get('/api/snapshot', autenticar, async (req, res) => {
    try {
        const desde = parseInt(req.query.desde);
        let   hasta = Math.min(parseInt(req.query.hasta), Date.now());
        if (!Number.isFinite(desde) || !Number.isFinite(hasta) || hasta <= desde) {
            return res.status(400).json({ error: 'Parámetros desde/hasta inválidos (timestamps en ms, hasta > desde).' });
        }
        if (hasta - desde > 48 * 3600000) {
            return res.status(400).json({ error: 'El rango máximo del snapshot es 48 horas.' });
        }

        // Warmup previo al rango para que EMA500/RSI/ADX/MACD lleguen calentados al
        // inicio (mismo criterio que el backtest); 2 días de 1m cubre todos los períodos.
        const WARMUP_MS  = 2 * 86400000;
        const CONTEXT_MS = 2 * 3600000; // ventana de "tendencia previa" del resumen

        const [klRes, whaleRes, oiRes, lsRes] = await Promise.all([
            pool.query(
                `SELECT open_time, open, high, low, close, volume, close_time, taker_buy_base
                 FROM klines_1m WHERE open_time >= $1::bigint AND open_time <= $2::bigint ORDER BY open_time ASC`,
                [desde - WARMUP_MS, hasta]
            ),
            pool.query(
                `SELECT EXTRACT(EPOCH FROM fecha) AS ts_sec, precio, cantidad, es_venta
                 FROM ballenas WHERE fecha >= $1 AND fecha <= $2 ORDER BY fecha ASC`,
                [new Date(desde).toISOString(), new Date(hasta).toISOString()]
            ),
            pool.query(
                'SELECT tiempo, valor FROM open_interest WHERE tiempo >= $1 AND tiempo <= $2 ORDER BY tiempo ASC',
                [Math.floor(desde / 1000), Math.floor(hasta / 1000)]
            ),
            pool.query(
                'SELECT tiempo, top_pos, global_acc FROM long_short_ratio WHERE tiempo >= $1 AND tiempo <= $2 ORDER BY tiempo ASC',
                [Math.floor(desde / 1000), Math.floor(hasta / 1000)]
            ),
        ]);

        const bars1m = klRes.rows.map(f => [
            Number(f.open_time), Number(f.open), Number(f.high), Number(f.low),
            Number(f.close), Number(f.volume), Number(f.close_time), 0, 0, Number(f.taker_buy_base)
        ]);
        const idx0 = bars1m.findIndex(b => b[0] >= desde); // primera vela 1m del rango
        if (idx0 < 0) {
            return res.status(404).json({ error: 'No hay velas cacheadas en ese rango (la BD cubre ~365 días hacia atrás).' });
        }

        // Indicadores sobre el array completo (warmup incluido). 5m/15m derivadas de 1m.
        const c1m = bars1m.map(b => b[4]);
        const ema50  = calcEMA(c1m, 50),  ema100 = calcEMA(c1m, 100);
        const ema200 = calcEMA(c1m, 200), ema500 = calcEMA(c1m, 500);
        const bars5m  = agregarVelas1m(bars1m, 300000);
        const bars15m = agregarVelas1m(bars1m, 900000);
        const c5m  = bars5m.map(b => b[4]);
        const c15m = bars15m.map(b => b[4]);
        const rsi15Arr = calcRSI(c15m, 14);
        const adx15Arr = calcADX(bars15m.map(b => b[2]), bars15m.map(b => b[3]), c15m);
        const { macd: macd5Arr, signal: sig5Arr } = calcMACDArr(c5m, 12, 26, 9);
        const vwap5Arr = calcVWAP(bars5m, 'daily');
        const ts5m  = bars5m.map(b => b[6]);
        const ts15m = bars15m.map(b => b[6]);
        const rsiByTs  = new Map(bars15m.map((b, i) => [b[6], rsi15Arr[i]]));
        const adxByTs  = new Map(bars15m.map((b, i) => [b[6], adx15Arr[i]]));
        const macdByTs = new Map(bars5m.map((b, i)  => [b[6], { macd: macd5Arr[i], sig: sig5Arr[i] }]));
        const vwapByTs = new Map(bars5m.map((b, i)  => [b[6], vwap5Arr[i]]));

        const iso = ms => new Date(ms).toISOString().slice(0, 16) + 'Z';
        const rnd = (v, d = 2) => v == null ? null : Math.round(v * 10 ** d) / 10 ** d;

        // Serie de velas con granularidad adaptativa para mantener el JSON manejable.
        const rangoBars  = bars1m.slice(idx0);
        const tfSeries   = (hasta - desde) <= 6 * 3600000 ? '1m' : '5m';
        const seriesBars = tfSeries === '1m' ? rangoBars : agregarVelas1m(rangoBars, 300000);

        // Para cada vela de la serie, los indicadores vistos al cierre de su última vela 1m
        // (los HTF vía lookupHTF = última vela 5m/15m CERRADA, igual que el backtest).
        let p1m = idx0;
        const velasOut = seriesBars.map(sb => {
            while (p1m + 1 < bars1m.length && bars1m[p1m + 1][6] <= sb[6]) p1m++;
            const tsC = bars1m[p1m][6];
            const macdV = lookupHTF(ts5m, macdByTs, tsC);
            return {
                ts: iso(sb[0]),
                o: sb[1], h: sb[2], l: sb[3], c: sb[4],
                vol: rnd(sb[5]), delta: rnd(2 * sb[9] - sb[5]),
                ema50: rnd(ema50[p1m], 1), ema100: rnd(ema100[p1m], 1),
                ema200: rnd(ema200[p1m], 1), ema500: rnd(ema500[p1m], 1),
                rsi15m: rnd(lookupHTF(ts15m, rsiByTs, tsC), 1),
                adx15m: rnd(lookupHTF(ts15m, adxByTs, tsC), 1),
                macd5m: macdV ? rnd(macdV.macd, 1) : null,
                macdSig5m: macdV ? rnd(macdV.sig, 1) : null,
                vwap5m: rnd(lookupHTF(ts5m, vwapByTs, tsC), 1),
            };
        });

        // Series de OI y posicionamiento, submuestreadas a ≤200 puntos.
        const dsStep = n => Math.max(1, Math.ceil(n / 200));
        const oiRango = oiRes.rows.map(r => ({ ts: Number(r.tiempo) * 1000, valor: Math.round(parseFloat(r.valor)) }));
        const oiSerie = oiRango.filter((_, i) => i % dsStep(oiRango.length) === 0 || i === oiRango.length - 1)
            .map(o => ({ ts: iso(o.ts), valor: o.valor }));
        const lsRango = lsRes.rows.map(r => ({
            ts: Number(r.tiempo) * 1000,
            top:    r.top_pos    != null ? parseFloat(r.top_pos)    : null,
            retail: r.global_acc != null ? parseFloat(r.global_acc) : null,
        }));
        const lsSerie = lsRango.filter((_, i) => i % dsStep(lsRango.length) === 0 || i === lsRango.length - 1)
            .map(o => ({ ts: iso(o.ts), top: rnd(o.top, 3), retail: rnd(o.retail, 3) }));

        // ── Resumen ──
        const first = rangoBars[0], last = rangoBars[rangoBars.length - 1];
        let maxP = -Infinity, minP = Infinity, volTotal = 0, deltaAcum = 0;
        for (const b of rangoBars) {
            if (b[2] > maxP) maxP = b[2];
            if (b[3] < minP) minP = b[3];
            volTotal  += b[5];
            deltaAcum += 2 * b[9] - b[5];
        }
        const whales   = whaleRes.rows.map(w => ({ ts: parseFloat(w.ts_sec) * 1000, btc: parseFloat(w.cantidad), precio: parseFloat(w.precio), esVenta: w.es_venta }));
        const wCompras = whales.filter(w => !w.esVenta), wVentas = whales.filter(w => w.esVenta);
        const oiIni = oiRango[0], oiFin = oiRango[oiRango.length - 1];
        const lsIni = lsRango[0], lsFin = lsRango[lsRango.length - 1];

        // Tendencia previa: variación del cierre en las 2h anteriores al inicio del tramo.
        let i2 = idx0;
        while (i2 > 0 && bars1m[i2 - 1][0] >= desde - CONTEXT_MS) i2--;
        const tendenciaPrevia = i2 < idx0 ? rnd((first[1] - bars1m[i2][1]) / bars1m[i2][1] * 100, 2) : null;

        const resumen = {
            precio: {
                apertura: first[1], cierre: last[4], maximo: maxP, minimo: minP,
                variacionPerc: rnd((last[4] - first[1]) / first[1] * 100, 2),
                rangoPerc: rnd((maxP - minP) / minP * 100, 2),
            },
            volumen: { totalBTC: rnd(volTotal), deltaAcumuladoBTC: rnd(deltaAcum) },
            openInterest: oiIni && oiFin
                ? { inicioBTC: oiIni.valor, finBTC: oiFin.valor, cambioPerc: rnd((oiFin.valor - oiIni.valor) / oiIni.valor * 100, 2) }
                : null,
            posicionamiento: lsIni && lsFin
                ? { topRatioInicio: rnd(lsIni.top, 3), topRatioFin: rnd(lsFin.top, 3),
                    retailRatioInicio: rnd(lsIni.retail, 3), retailRatioFin: rnd(lsFin.retail, 3) }
                : null,
            ballenas: {
                compras: wCompras.length, ventas: wVentas.length,
                btcComprado: rnd(wCompras.reduce((s, w) => s + w.btc, 0), 1),
                btcVendido:  rnd(wVentas.reduce((s, w) => s + w.btc, 0), 1),
                btcNeto:     rnd(whales.reduce((s, w) => s + (w.esVenta ? -w.btc : w.btc), 0), 1),
                umbralGuardadoBTC: limiteGuardadoBD,
            },
            contexto: {
                tendenciaPrevia2hPerc: tendenciaPrevia,
                rsi15mInicio: rnd(lookupHTF(ts15m, rsiByTs, first[6]), 1),
                rsi15mFin:    rnd(lookupHTF(ts15m, rsiByTs, last[6]), 1),
                adx15mInicio: rnd(lookupHTF(ts15m, adxByTs, first[6]), 1),
                adx15mFin:    rnd(lookupHTF(ts15m, adxByTs, last[6]), 1),
                distEMA200InicioPerc: ema200[idx0] ? rnd((first[4] - ema200[idx0]) / ema200[idx0] * 100, 2) : null,
            },
        };

        // ── Eventos: ballenas (cap a las 40 más grandes) y saltos de OI (≥0.3% en ~5m) ──
        const eventos = [];
        const whalesEv = whales.length > 40
            ? [...whales].sort((a, b) => b.btc - a.btc).slice(0, 40).sort((a, b) => a.ts - b.ts)
            : whales;
        whalesEv.forEach(w => eventos.push({ ts: iso(w.ts), tipo: 'ballena', lado: w.esVenta ? 'venta' : 'compra', btc: rnd(w.btc, 1), precio: Math.round(w.precio) }));
        let lo = 0, lastOiEvTs = -Infinity;
        for (let k = 0; k < oiRango.length; k++) {
            while (oiRango[lo].ts < oiRango[k].ts - 300000) lo++;
            const past = oiRango[lo];
            if (past.ts >= oiRango[k].ts || past.valor <= 0) continue;
            const ch = (oiRango[k].valor - past.valor) / past.valor * 100;
            if (Math.abs(ch) >= 0.3 && oiRango[k].ts - lastOiEvTs >= 300000) {
                eventos.push({ ts: iso(oiRango[k].ts), tipo: 'salto_oi', cambio5mPerc: rnd(ch, 2) });
                lastOiEvTs = oiRango[k].ts;
            }
        }
        eventos.sort((a, b) => a.ts < b.ts ? -1 : 1);

        res.json({
            meta: {
                simbolo: 'BTCUSDT', exchange: 'Binance',
                desde: iso(desde), hasta: iso(hasta),
                duracionMin: Math.round((hasta - desde) / 60000),
                tfSeries, velasEnSerie: velasOut.length,
                generado: iso(Date.now()),
                notas: 'Velas 1m de la BD propia. delta = volumen taker de compra − venta (BTC). Indicadores: EMAs 50/100/200/500 sobre 1m; RSI(14) y ADX(14) sobre 15m; MACD(12,26,9) sobre 5m; VWAP sesión diaria sobre 5m; los HTF se leen de la última vela cerrada (sin look-ahead). Ratios: top = top traders por posición (long/short), retail = cuentas globales. Horarios en UTC.',
            },
            resumen,
            eventos,
            series: { velas: velasOut, openInterest: oiSerie, posicionamiento: lsSerie },
        });
    } catch (err) {
        console.error('Error snapshot:', err);
        res.status(500).json({ error: err.message || 'Error al generar el snapshot' });
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
