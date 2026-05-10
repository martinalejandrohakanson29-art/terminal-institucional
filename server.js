require('dotenv').config();
const express = require('express');
const path = require('path');
const { Pool } = require('pg');
const WebSocket = require('ws');

const app = express();
app.use(express.json());

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
    // BIGINT para evitar overflow en ~2038
    const queryTablaOI = `
        CREATE TABLE IF NOT EXISTS open_interest (
            tiempo BIGINT PRIMARY KEY,
            valor NUMERIC NOT NULL
        );
    `;

    try {
        await pool.query(queryTablaBallenas);
        await pool.query(queryTablaConfig);
        await pool.query(queryTablaOI);

        // Migrar INTEGER → BIGINT si la tabla ya existía con el tipo viejo
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

        // Índice para acelerar queries por fecha
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_ballenas_fecha ON ballenas(fecha DESC)`);

        await pool.query(`INSERT INTO configuracion (clave, valor) VALUES ('limite_bd', 1.0) ON CONFLICT (clave) DO NOTHING`);

        const configRes = await pool.query(`SELECT valor FROM configuracion WHERE clave = 'limite_bd'`);
        if (configRes.rows.length > 0) {
            limiteGuardadoBD = parseFloat(configRes.rows[0].valor);
            console.log(`🔧 Límite de guardado cargado: > ${limiteGuardadoBD} BTC`);
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


// --- RUTAS DE LA API ---
app.get('/api/ballenas', async (req, res) => {
    try {
        // Últimos 7 días, máximo 5000 registros para no saturar la red
        const query = `
            SELECT precio, cantidad, es_venta, EXTRACT(EPOCH FROM fecha) as tiempo_segundos
            FROM ballenas
            WHERE fecha >= NOW() - INTERVAL '7 days'
            ORDER BY fecha DESC
            LIMIT 5000
        `;
        const resultado = await pool.query(query);
        res.json(resultado.rows);
    } catch (error) {
        res.status(500).json({ error: 'Error interno del servidor' });
    }
});

app.get('/api/open-interest', async (req, res) => {
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

// Proxy para OI en vivo — evita problemas de CORS y bloqueos regionales en el frontend
app.get('/api/oi-live', async (req, res) => {
    try {
        const respuesta = await fetch('https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT');
        const datos = await respuesta.json();
        res.json(datos);
    } catch (error) {
        res.status(500).json({ error: 'Error obteniendo OI en vivo' });
    }
});

app.get('/api/filtro-bd', (req, res) => {
    res.json({ umbral: limiteGuardadoBD });
});

app.post('/api/filtro-bd', async (req, res) => {
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

app.use(express.static(path.join(__dirname, 'public')));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`¡Terminal Institucional encendida en puerto ${PORT}!`));
