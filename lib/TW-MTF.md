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

El indicador visual y los motores de backtest y AutoBot comparten `public/tw-pivots.js`.

## AutoBot: TW MTF

Guardá una estrategia con motor **TW MTF** en `/estrategias` y seleccioná ese nombre en el modal de AutoBot. La cuenta y el entorno del modal determinan dónde se ejecutan las órdenes (Binance o BingX, demo o real); el selector de datos determina de dónde salen las señales y los precios de salida.

- Se respetan modo rechazo/doble/ambos, temporalidades, patrones, zonas, direcciones habilitadas, tamaño de posición y apalancamiento guardados. TP/SL pueden usar el stop estructural con objetivo R o porcentajes manuales desde la entrada. La protección escalonada opcional comparte sus tres niveles con la terminal: confirma el disparo al cierre de 1m y el nuevo stop rige desde la vela siguiente. Los demás filtros, pyramiding y breakeven clásicos se desactivan como en el backtest. En vivo, TW exige apalancamiento entero entre 1 y 100; si el exchange no acepta fijarlo, no abre.
- Solo se opera una confirmación de la última frontera de minuto, con todas las temporalidades cerradas y actualizadas. Nunca se operan candidatos anticipados; los dobles requieren neckline. Un hueco reinicia el historial utilizable. Señales opuestas simultáneas se descartan; con el mismo lado se prioriza el doble confirmado.
- El ciclo evalúa cada 60 segundos. La entrada real es MARKET al detectar la confirmación, no una ejecución garantizada en la apertura del backtest. Se vuelven a calcular TP/SL con el precio disponible y con el fill: desde el riesgo estructural en modo R o desde los porcentajes guardados en modo manual. En ejecución cruzada, los niveles permanecen en el mercado de datos y se conserva el mecanismo existente de protección en el exchange de ejecución.
- Una señal se reclama atómicamente en BD antes de mandar la orden. No se reintenta esa confirmación tras un error ambiguo, cierre, cambio de configuración o reinicio. Se requiere otro evento. Una sola posición por cuenta.
- Se colocan TP y SL en el exchange y se controlan también por ticks. Si falta una protección o el fill excede el riesgo configurado, se solicita cierre inmediato (`TW Protec` / `TW Riesgo`). Como toda orden, el cierre depende de la respuesta del exchange.
- La duración máxima y los niveles de protección escalonada se congelan por entrada y se recuperan al reiniciar. Cambiar/borrar la estrategia o detener AutoBot no elimina la gestión de esas salidas. Los stops EMA y límites de tiempo clásicos no se aplican a posiciones TW.

El historial en vivo es una ventana de velas nativas: se amplía según las ventanas configuradas (al menos 800 velas por temporalidad TW, o las que ya trae el ciclo). Zonas anteriores a esa ventana no se reconstruyen; un backtest con más historia puede diferir. La comparación exacta requiere el mismo exchange, historial y parámetros; los fills reales además dependen de latencia, spread y deslizamiento.

Al desplegar y reiniciar `server.js`, la inicialización añade `cuentas_trading.ultima_tw_signal_ts` y `auto_trading_entradas.tw_max_hold_minutes` sin borrar datos. No hace falta volver a guardar las estrategias TW existentes.

Las pruebas automatizadas no arrancan el servidor ni usan credenciales: simulan BD y exchanges. No certifican permisos API, saldo, aceptación de órdenes ni ejecución real de una cuenta concreta.

## Verificación

`node --test scripts/test-tw-pivots.js scripts/test-tw-backtest.js scripts/test-tw-live.js`

Pruebas de confirmación causal, cierres, patrones, visitas, dobles techos/pisos, invalidación, MTF, huecos, parámetros, cancelación de respuestas antiguas e integración del menú y layout.

`node scripts/preview-tw-pivots.js`

Abre `http://127.0.0.1:4319` para una prueba visual aislada con velas sintéticas. No inicia `server.js`, conecta bases de datos ni envía órdenes. Esta prueba valida la interfaz; no sustituye una comparación con datos reales del exchange.
