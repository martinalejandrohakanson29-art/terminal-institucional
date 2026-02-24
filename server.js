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
    
    // NUEVO: Creamos una tabla para guardar la configuración del servidor
    const queryTablaConfig = `
        CREATE TABLE IF NOT EXISTS configuracion (
            clave VARCHAR(50) PRIMARY KEY,
            valor NUMERIC NOT NULL
        );
    `;

    try {
        await pool.query(queryTablaBallenas);
        await pool.query(queryTablaConfig);
        
        // Insertamos el valor por defecto solo si la tabla está vacía
        await pool.query(`INSERT INTO configuracion (clave, valor) VALUES ('limite_bd', 1.0) ON CONFLICT (clave) DO NOTHING`);
        
        // Leemos la configuración guardada y se la asignamos a la memoria del servidor
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
        console.error('Error al obtener el historial:', error);
        res.status(500).json({ error: 'Error interno del servidor' });
    }
});

// RUTAS PARA EL CONTROL DE LA BASE DE DATOS
app.get('/api/filtro-bd', (req, res) => {
    res.json({ umbral: limiteGuardadoBD });
});

app.post('/api/filtro-bd', async (req, res) => {
    const nuevoUmbral = parseFloat(req.body.umbral);
    if (!isNaN(nuevoUmbral) && nuevoUmbral > 0) {
        limiteGuardadoBD = nuevoUmbral;
        
        // NUEVO: Guardamos el cambio en la tabla de configuración para que no se borre si se reinicia el servidor
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
