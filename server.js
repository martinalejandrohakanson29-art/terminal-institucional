require('dotenv').config();
const express = require('express');
const path = require('path');
const { Pool } = require('pg');
const WebSocket = require('ws');
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

    try {
        await pool.query(queryTablaBallenas);
        await pool.query(queryTablaConfig);
        await pool.query(queryTablaOI);
        await pool.query(queryTablaUsuarios);
        await pool.query(queryTablaConfigUsuario);

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
// RUTAS DE PÁGINAS (deben ir ANTES de express.static para que
// tomen prioridad sobre el auto-index de index.html en "/")
// ============================================================

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'login.html')));
app.get('/terminal', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));
app.get('/admin', (req, res) => res.sendFile(path.join(__dirname, 'public', 'admin.html')));

// index: false evita que express.static sirva index.html automáticamente para "/"
app.use(express.static(path.join(__dirname, 'public'), { index: false }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`¡Terminal Institucional encendida en puerto ${PORT}!`));
