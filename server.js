const express = require('express');
const path = require('path');
const { Pool } = require('pg'); 
const WebSocket = require('ws'); 

const app = express();

// 1. CONFIGURACIÓN DE LA BASE DE DATOS
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

// 2. CREAR TABLAS
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
    try {
        await pool.query(queryTablaBallenas);
        console.log('✅ Base de datos lista y conectada.');
    } catch (error) {
        console.error('❌ Error al crear las tablas:', error);
    }
}
inicializarBaseDeDatos();

// 3. EL CAZADOR DE BALLENAS
function iniciarRastreadorBallenas() {
    const ws = new WebSocket('wss://stream.binance.com:9443/ws/btcusdt@aggTrade');
    const UMBRAL_BTC = 1.0; 

    ws.on('open', () => console.log('✅ Conectado a la Cinta de Binance.'));

    ws.on('message', async (data) => {
        try {
            const evento = JSON.parse(data);
            const cantidad = parseFloat(evento.q);
            const precio = parseFloat(evento.p);
            const es_venta = evento.m; 

            if (cantidad >= UMBRAL_BTC) {
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

// 4. NUEVA RUTA: LA PUERTA DE DATOS (API) PARA EL GRÁFICO
// Cuando la web pida datos aquí, el servidor busca en la base de datos
app.get('/api/ballenas', async (req, res) => {
    try {
        // Traemos las últimas 2000 órdenes guardadas. 
        // Convertimos la fecha a "segundos" (EPOCH) porque así lo requiere el gráfico visual
        const query = `
            SELECT precio, cantidad, es_venta, EXTRACT(EPOCH FROM fecha) as tiempo_segundos 
            FROM ballenas 
            ORDER BY fecha ASC
            LIMIT 2000
        `;
        const resultado = await pool.query(query);
        res.json(resultado.rows); // Se lo enviamos a la página web
    } catch (error) {
        console.error('Error al obtener el historial:', error);
        res.status(500).json({ error: 'Error interno del servidor' });
    }
});


// 5. CONFIGURACIÓN WEB
app.use(express.static(path.join(__dirname, 'public')));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`¡Terminal Institucional encendida en puerto ${PORT}!`));
