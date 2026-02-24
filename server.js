const express = require('express');
const path = require('path');
const { Pool } = require('pg'); 
const WebSocket = require('ws'); 

const app = express();
// NUEVO: Le enseñamos al servidor a entender datos enviados desde la web en formato JSON
app.use(express.json()); 

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

// NUEVO: Esta variable será la memoria del servidor para saber el límite de guardado
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
    try {
        await pool.query(queryTablaBallenas);
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

            // AQUÍ USAMOS LA VARIABLE DINÁMICA en lugar del número fijo
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

// --- NUEVAS RUTAS PARA EL CONTROL DE LA BASE DE DATOS ---

// Cuando la web carga, le pregunta al servidor qué límite está usando
app.get('/api/filtro-bd', (req, res) => {
    res.json({ umbral: limiteGuardadoBD });
});

// Cuando cambias el número en la web, se envía aquí para actualizar el servidor
app.post('/api/filtro-bd', (req, res) => {
    const nuevoUmbral = parseFloat(req.body.umbral);
    if (!isNaN(nuevoUmbral) && nuevoUmbral > 0) {
        limiteGuardadoBD = nuevoUmbral;
        console.log(`🔧 Filtro de BD actualizado. Ahora solo guardamos > ${limiteGuardadoBD} BTC`);
        res.json({ status: 'ok', umbral: limiteGuardadoBD });
    } else {
        res.status(400).json({ error: 'Número inválido' });
    }
});

app.use(express.static(path.join(__dirname, 'public')));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`¡Terminal Institucional encendida en puerto ${PORT}!`));
