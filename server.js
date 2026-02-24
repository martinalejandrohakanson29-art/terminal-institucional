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

    // NUEVO: Tabla para guardar el Interés Abierto. 
    // Usamos el "tiempo" como llave primaria para asegurar que haya exactamente 1 solo registro por minuto.
    const queryTablaOI = `
        CREATE TABLE IF NOT EXISTS open_interest (
            tiempo INTEGER PRIMARY KEY,
            valor NUMERIC NOT NULL
        );
    `;

    try {
        await pool.query(queryTablaBallenas);
        await pool.query(queryTablaConfig);
        await pool.query(queryTablaOI);
        
        await pool.query(`INSERT INTO configuracion (clave, valor) VALUES ('limite_bd', 1.0) ON CONFLICT (clave) DO NOTHING`);
        
        const configRes = await pool.query(`SELECT valor FROM configuracion WHERE clave = 'limite_bd'`);
        if (configRes.rows.length > 0) {
            limiteGuardadoBD = parseFloat(configRes.rows[0].valor);
            console.log(`🔧 Memoria del servidor cargada. Límite de guardado: > ${limiteGuardadoBD} BTC`);
        }

        console.log('✅ Base de datos lista y conectada.');
    } catch (error) {
        console.error('❌ Error al crear las tablas:', error);
    }
}
inicializarBaseDeDatos();

// --- NUEVO: RECOLECTOR DE OPEN INTEREST ---
async function guardarOpenInterest() {
    try {
        // Node.js v18+ tiene fetch incorporado, igual que el navegador
        const respuesta = await fetch('https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT');
        const datos = await respuesta.json();
        
        if (datos && datos.openInterest) {
            const valor = parseFloat(datos.openInterest);
            // Calculamos el inicio del minuto actual (ej: 14:05:00) en formato Epoch (segundos)
            const tiempoVelaActual = Math.floor(Date.now() / 60000) * 60; 
            
            // Lo guardamos o lo actualizamos si ya existe ese minuto
            const query = `
                INSERT INTO open_interest (tiempo, valor) 
                VALUES ($1, $2)
                ON CONFLICT (tiempo) DO UPDATE SET valor = EXCLUDED.valor
            `;
            await pool.query(query, [tiempoVelaActual, valor]);
        }
    } catch (error) {
        console.error('Error al guardar Open Interest en BD:', error);
    }
}

// Lo ejecutamos cada 1 minuto (60000 milisegundos) exacto
setInterval(guardarOpenInterest, 60000);
guardarOpenInterest(); // Y lo ejecutamos 1 vez apenas arranca el servidor


// --- CAZADOR DE BALLENAS ---
function iniciarRastreadorBallenas() {
    const ws = new WebSocket('wss://stream.binance.com:9443/ws/btcusdt@aggTrade');

    ws.on('open', () => console.log('✅ Conectado a la Cinta de Binance.'));

    ws.on('message', async (data) => {
        try {
            const evento = JSON.parse(data);
            const cantidad = parseFloat(evento.q);
            const precio = parseFloat(evento.p);
            const es_venta = evento.m; 

            if (cantidad >= limiteGuardadoBD) {
                const queryInsertar = `INSERT INTO ballenas (precio, cantidad, es_venta) VALUES ($1, $2, $3)`;
                await pool.query(queryInsertar, [precio, cantidad, es_venta]);
            }
        } catch (error) {
            console.error('Error al guardar trade:', error);
        }
    });

    ws.on('close', () => {
        console.log('⚠️ Reconectando en 5 segundos...');
        setTimeout(iniciarRastreadorBallenas, 5000);
    });
}
iniciarRastreadorBallenas();


// --- RUTAS DE LA API ---
app.get('/api/ballenas', async (req, res) => {
    try {
        const query = `
            SELECT precio, cantidad, es_venta, EXTRACT(EPOCH FROM fecha) as tiempo_segundos 
            FROM ballenas 
            ORDER BY fecha ASC
            LIMIT 2000
        `;
        const resultado = await pool.query(query);
        res.json(resultado.rows); 
    } catch (error) {
        res.status(500).json({ error: 'Error interno del servidor' });
    }
});

// NUEVA RUTA: Entregar el historial de Open Interest
app.get('/api/open-interest', async (req, res) => {
    try {
        // Pedimos los últimos 1440 minutos (equivalente a 24 horas)
        const query = `
            SELECT tiempo, valor 
            FROM open_interest 
            ORDER BY tiempo ASC
            LIMIT 1440
        `;
        const resultado = await pool.query(query);
        res.json(resultado.rows); 
    } catch (error) {
        res.status(500).json({ error: 'Error interno obteniendo OI' });
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
            console.log(`🔧 Filtro de BD actualizado y guardado. Ahora solo guardamos > ${limiteGuardadoBD} BTC`);
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
