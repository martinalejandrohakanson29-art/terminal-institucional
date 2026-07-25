// Script de prueba standalone — NO toca server.js.
// Objetivo: validar contra la API real de BingX (swap v2) tres puntos
// necesarios antes de migrar la fuente de datos de Binance a BingX:
//   1) formato de símbolo y disponibilidad de BTC-USDT
//   2) minNotional / minQty real para operar con ~$10
//   3) profundidad histórica disponible de OI y long/short ratio
//
// Uso: node scripts/test-bingx.js

const BASE = 'https://open-api.bingx.com';
const SYMBOL = 'BTC-USDT';

async function get(path, params = {}) {
  const qs = new URLSearchParams(params).toString();
  const url = `${BASE}${path}${qs ? '?' + qs : ''}`;
  const res = await fetch(url);
  const text = await res.text();
  let json;
  try {
    json = JSON.parse(text);
  } catch {
    json = { raw: text };
  }
  return { status: res.status, url, json };
}

function printSection(title) {
  console.log('\n' + '='.repeat(60));
  console.log(title);
  console.log('='.repeat(60));
}

async function main() {
  // 1) Info del contrato: formato de símbolo, minQty, tradeMinUSDT, precisión
  printSection('1) Contract info (símbolo, mínimos de orden)');
  try {
    const r = await get('/openApi/swap/v2/quote/contracts');
    if (r.json?.data) {
      const btc = r.json.data.find((c) => c.symbol === SYMBOL);
      if (btc) {
        console.log('Contrato encontrado:', JSON.stringify(btc, null, 2));
      } else {
        console.log('No se encontró', SYMBOL, '— primeros 3 símbolos disponibles:');
        console.log(r.json.data.slice(0, 3).map((c) => c.symbol));
      }
    } else {
      console.log('Respuesta inesperada:', JSON.stringify(r.json).slice(0, 500));
    }
  } catch (e) {
    console.log('ERROR:', e.message);
  }

  // 2) Klines recientes (formato de vela, timestamps)
  printSection('2) Klines (velas 1m, últimas 3)');
  try {
    const r = await get('/openApi/swap/v3/quote/klines', {
      symbol: SYMBOL,
      interval: '1m',
      limit: 3,
    });
    console.log(JSON.stringify(r.json, null, 2));
  } catch (e) {
    console.log('ERROR:', e.message);
  }

  // 3) Open Interest actual
  printSection('3) Open Interest actual');
  try {
    const r = await get('/openApi/swap/v2/quote/openInterest', { symbol: SYMBOL });
    console.log(JSON.stringify(r.json, null, 2));
  } catch (e) {
    console.log('ERROR:', e.message);
  }

  // 4) Long/Short ratio — probar varias rutas candidatas hasta encontrar la válida
  printSection('4) Long/Short ratio (probando rutas candidatas)');
  const candidatePaths = [
    '/openApi/swap/v2/quote/topLongShortAccountRatio',
    '/openApi/swap/v1/quote/topLongShortAccountRatio',
    '/openApi/swap/v2/quote/topLongShortPositionRatio',
    '/openApi/swap/v2/quote/longShortRatio',
    '/openApi/swap/v1/market/longShortRatio',
  ];
  for (const path of candidatePaths) {
    try {
      const r = await get(path, { symbol: SYMBOL, period: '1h', limit: 5 });
      const ok = r.json?.code === 0;
      console.log(`${ok ? 'OK ' : 'ERR'} ${path} ->`, JSON.stringify(r.json).slice(0, 200));
    } catch (e) {
      console.log('ERR', path, '->', e.message);
    }
  }

  // 5) Funding rate actual + histórico
  printSection('5) Funding rate histórico (últimos 3)');
  try {
    const r = await get('/openApi/swap/v2/quote/fundingRate', {
      symbol: SYMBOL,
      limit: 3,
    });
    console.log(JSON.stringify(r.json, null, 2));
  } catch (e) {
    console.log('ERROR:', e.message);
  }

  printSection('Fin de la prueba');
  console.log(
    'Revisar arriba: minQty/tradeMinUSDT en (1) para confirmar si $10 alcanza,\n' +
    'y si (3)/(4) devuelven datos consistentes para armar el histórico.'
  );
}

main().catch((e) => {
  console.error('Fallo general:', e);
  process.exit(1);
});
