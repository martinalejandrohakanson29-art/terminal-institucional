const express = require('express');
const path = require('path');
const { Pool } = require('pg'); 
const WebSocket = require('ws'); // Traemos la nueva herramienta para escuchar a Binance

const app = express();

// 1. CONFIGURACIÓN DE LA BASE DE DATOS
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: {
        rejectUnauthorized: false 
    }
});

// 2. FUNCIÓN PARA CREAR LAS TABLAS
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

// 3. EL CAZADOR DE BALLENAS (NUEVO)
function iniciarRastreadorBallenas() {
    // Nos conectamos al mismo tubo de datos que usa tu frontend
    const ws = new WebSocket('wss://stream.binance.com:9443/ws/btcusdt@aggTrade');
    const UMBRAL_BTC = 1.0; // Solo guardamos si es 1 BTC o más

    ws.on('open', () => {
        console.log('✅ Servidor conectado a la Cinta de Ballenas de Binance.');
    });

    ws.on('message', async (data) => {
        try {
            const evento = JSON.parse(data);
            const cantidad = parseFloat(evento.q);
            const precio = parseFloat(evento.p);
            const es_venta = evento.m; // true si el agresor vendió, false si compró

            // Si el trade es gigante, lo guardamos en la base de datos
            if (cantidad >= UMBRAL_BTC) {
                const queryInsertar = `
                    INSERT INTO ballenas (precio, cantidad, es_venta) 
                    VALUES ($1, $2, $3)
                `;
                const valores = [precio, cantidad, es_venta];
                
                await pool.query(queryInsertar, valores);
                console.log(`🐳 Guardado en BD: ${es_venta ? 'VENTA' : 'COMPRA'} de ${cantidad} BTC a $${precio}`);
            }
        } catch (error) {
            console.error('❌ Error al procesar o guardar el trade:', error);
        }
    });

    // Si Binance nos desconecta, intentamos reconectar a los 5 segundos
    ws.on('close', () => {
        console.log('⚠️ Binance cerró la conexión. Reconectando en 5 segundos...');
        setTimeout(iniciarRastreadorBallenas, 5000);
    });
}

// Encendemos el cazador de ballenas
iniciarRastreadorBallenas();


// 4. CONFIGURACIÓN DE TU PÁGINA WEB
app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// 5. ENCENDER EL SERVIDOR
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`¡Terminal Institucional encendida en el puerto ${PORT}!`);
});
