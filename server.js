const express = require('express');
const path = require('path');
const { Pool } = require('pg'); // Traemos la herramienta de PostgreSQL

const app = express();

// 1. CONFIGURACIÓN DE LA BASE DE DATOS
// Usamos la URL que Railway nos da automáticamente en process.env.DATABASE_URL
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: {
        rejectUnauthorized: false // Esto es obligatorio en Railway para conexiones seguras
    }
});

// 2. FUNCIÓN PARA CREAR LAS TABLAS (Si no existen)
async function inicializarBaseDeDatos() {
    // Vamos a crear una tabla especial para guardar las operaciones de ballenas
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

// Ejecutamos la función al encender el servidor
inicializarBaseDeDatos();


// 3. CONFIGURACIÓN DE TU PÁGINA WEB
// Le decimos al servidor que la carpeta "public" tiene nuestros archivos visibles
app.use(express.static(path.join(__dirname, 'public')));

// Cuando alguien entre a la web, le mandamos el index.html
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// 4. ENCENDER EL SERVIDOR
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`¡Terminal Institucional encendida en el puerto ${PORT}!`);
});
