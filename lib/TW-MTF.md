# TW MTF · Pivots y Price Action

Disponible en **Indicadores → TW MTF · Pivots y Price Action**. El engranaje del chip abre los parámetros. «Aplicar y guardar» guarda los parámetros por usuario; el botón general «Guardar» conserva también si el indicador está activado.

Incluye soportes/resistencias HTF, envolventes, martillo, estrella fugaz, filtro de cercanía y una señal por visita. Detecta dobles techos/pisos, candidatos amarillos, neckline y confirmación/invalidation por cierre HTF. Las temporalidades de pivots y patrones son independientes de la del gráfico (1m, 5m, 15m, 1h, 4h, 1d).

## Adaptación del archivo original

`indicador.txt` termina en `retracePer`, antes de completar doble techo, doble piso y dibujo de señales. Se completó esa funcionalidad en JavaScript; no es una ejecución de Pine Script ni se afirma equivalencia exacta con TradingView.

- Se procesan exclusivamente velas cerradas, por orden de cierre. Un pivot aparece tras cerrar sus velas derechas de confirmación. La zona comienza en ese momento, sin dibujar información futura en el pasado.
- Los patrones se evalúan una vez por vela cerrada de su temporalidad. Una visita se reinicia cuando una vela deja de tocar la zona ampliada.
- La neckline usa el mínimo/máximo real entre los dos extremos. El cierre HTF confirma su ruptura o invalida el candidato cuando supera el extremo más el margen configurado. Si no se exige neckline, el patrón se señala al confirmar el segundo pivot.
- En gráficos más gruesos, las señales se anclan a la primera vela del gráfico cuya apertura sea igual o posterior a la confirmación; varias pueden coincidir. Las señales recientes pueden esperar la siguiente vela del gráfico para visualizarse.
- Los pivots con huecos dentro de su ventana y las envolventes con velas no consecutivas se descartan.
- Ocultar zonas solo afecta al dibujo. Desactivar su cálculo manteniendo la exigencia de cercanía impide emitir patrones de velas, como en el original. Los dobles techos/pisos son independientes de ese filtro.

## Datos y límites

Se utiliza `/api/klines` con el exchange seleccionado en el terminal. La carga inicial pide hasta 10.000 velas por temporalidad y luego actualiza las últimas 10 cada 15 segundos. Si hubo una interrupción prolongada, vuelve a cargar el historial. Cambiar parámetros, exchange o desactivar cancela solicitudes pendientes para no mezclar resultados.

La disponibilidad histórica depende de la caché del servidor; no se inventan velas faltantes. Se conservan hasta 20 zonas por lado y se dibujan las últimas 200 señales dentro del historial del gráfico. Las zonas invalidadas se eliminan; las señales históricas permanecen mientras estén dentro de la ventana cargada. Un error de lectura retira los dibujos y muestra un aviso hasta que la consulta se recupere.

Este módulo es un indicador visual: no modifica el motor de backtest ni las condiciones de ejecución automática.

## Verificación

`node --test scripts/test-tw-pivots.js`

Pruebas de confirmación causal, cierres, patrones, visitas, dobles techos/pisos, invalidación, MTF, huecos, parámetros, cancelación de respuestas antiguas e integración del menú y layout.

`node scripts/preview-tw-pivots.js`

Abre `http://127.0.0.1:4319` para una prueba visual aislada con velas sintéticas. No inicia `server.js`, conecta bases de datos ni envía órdenes. Esta prueba valida la interfaz; no sustituye una comparación con datos reales del exchange.
