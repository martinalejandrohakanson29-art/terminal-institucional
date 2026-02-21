const express = require('express');
const path = require('path');
const app = express();

// Le decimos al servidor que la carpeta "public" tiene nuestros archivos visibles
app.use(express.static(path.join(__dirname, 'public')));

// Cuando alguien entre a la web, le mandamos el index.html
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Le decimos en qué "puerto" escuchar (Railway asigna esto automáticamente)
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`¡Terminal Institucional encendida en el puerto ${PORT}!`);
});