# Optimizador de estrategias

Reemplaza la prueba manual de condicionales en `/estrategias` por un pipeline que parametriza
la estrategia, busca combinaciones con búsqueda bayesiana y valida fuera de muestra.

```bash
node scripts/optimizar.js --dias 150 --trials 300
node scripts/optimizar.js --ayuda        # o: npm run optimizar -- --ayuda
```

Necesita Node 18+ y el `DATABASE_URL` del `.env`. **No hace falta que el server esté
levantado**: se conecta a Postgres por su cuenta.

## Reproducibilidad

Dos corridas con los mismos argumentos dan el mismo CSV, byte a byte. Para que eso valga hay
que fijar **las dos** cosas que definen un estudio:

- `--semilla N` — la secuencia de muestreo.
- `--hasta YYYY-MM-DD` — el fin del período. Sin esto, el período se ancla a "ahora": la misma
  orden corrida mañana cubre otras velas. Y la BD además sigue sincronizando velas y
  acumulando ballenas/OI, así que el histórico de un mismo rango tampoco es fijo hacia atrás.

Los trials se despachan **en lotes** del tamaño del pool de workers, y los resultados se
registran en orden de índice antes de proponer el lote siguiente. Despachando de forma
continua se aprovecha algo mejor la CPU, pero el modelo del TPE recibiría los resultados en el
orden en que terminan —que depende de cuánto tardó cada backtest— y dos corridas con la misma
semilla divergirían desde el primer desempate. El precio del lote es que el modelo se
actualiza cada N trials en vez de cada uno (N = 3 en una máquina de 4 núcleos) y que se pierde
algo de CPU esperando al rezagado de cada lote.

## Cómo está armado

```
scripts/optimizar.js        CLI: parsea argumentos, orquesta, imprime el reporte
lib/optim/espacio.js        QUÉ se busca y en qué rangos   ← lo único que se toca a diario
lib/optim/metricas.js       Sharpe, Calmar, drawdown y la función objetivo
lib/optim/tpe.js            Muestreador bayesiano (TPE). No sabe nada de trading
lib/optim/datos.js          Carga el histórico una vez y lo rebana en ventanas
lib/optim/evaluador.js      Combinación + ventana → métricas + score
lib/optim/worker.js         Un hilo con una ventana cargada y su cache de indicadores
lib/optim/pool.js           Reparte trials entre los núcleos disponibles
lib/optim/walkforward.js    Optimizar en un tramo, validar en el siguiente
lib/optim/registro.js       CSV con una fila por trial + reporte JSON
lib/backtest-core.js        El motor, compartido con el server (NO es una copia)
```

La separación importa en un punto: `tpe.js` y `walkforward.js` no saben qué es un trade.
Reciben descriptores de parámetros y una función que devuelve un score. Agregar una estrategia
nueva es escribir su bloque en `espacio.js`, sin tocar el optimizador.

## El motor es uno solo

`lib/backtest-core.js` se extrajo de `server.js`, que ahora lo importa. No hay dos
implementaciones: el backtest que corre el optimizador es byte a byte el que corre
`/estrategias` y el que evalúa la señal en vivo. Si hubiera una copia, cualquier ajuste a la
lógica de entrada dejaría al optimizador buscando sobre una estrategia que ya no existe, y el
síntoma sería "el optimizador dice que gana pero en la UI da otra cosa".

El motor acepta cuatro parámetros opcionales que solo usa el optimizador y que no cambian en
nada el comportamiento por defecto: `warmupBars`, `hastaBarIdx`, `maxTradesDevueltos` y
`_cache`.

## Parametrización

Todo umbral, ventana, período y multiplicador es un parámetro con su rango declarado en
`espacio.js`. Los parámetros con `escala: 'log'` se muestrean en logaritmo: en un rango de 5 a
240 minutos, el muestreo lineal casi nunca propone valores por debajo de 30, que es justo donde
el comportamiento cambia.

Los parámetros condicionales (`si: c => c.useRsiFilter`) solo existen cuando su filtro está
encendido. No se muestrean ni entran al modelo, así que la dimensionalidad efectiva baja sola
a medida que la búsqueda descarta filtros.

`aParamsBacktest()` pasa toda combinación por `normalizarParams()` —la misma función que usa
`POST /api/backtest`— para que una combinación signifique exactamente lo mismo por CLI que
mandada desde la UI.

## Por qué TPE y no grid search

Con 61 parámetros, aunque se probaran solo 3 valores de cada uno, la grilla tiene 3⁶¹ puntos.
TPE (Tree-structured Parzen Estimator, el default de Optuna) modela dos distribuciones sobre
los parámetros —la de las combinaciones buenas y la del resto— y propone el candidato que
maximiza el cociente entre ambas. En la práctica, dentro de cada ventana el score sube de −60
a positivo en las primeras ~150 evaluaciones.

Está implementado en JS en vez de llamar a Optuna por subproceso porque un backtest tarda
~300 ms: el costo de serializar y cruzar un puente Python↔Node en cada trial sería una
fracción enorme del total.

**Pruning:** cada trial corre primero sobre el 40% de la ventana; si su score parcial está por
debajo de la mediana de los parciales ya vistos, se abandona. Como el prefijo reusa los
indicadores memoizados, abandonar cuesta ~40% de una corrida completa.

## Validación walk-forward

El período se parte en ventanas consecutivas. Por cada una: se optimiza sobre el in-sample y
el top-N se mide sobre el out-of-sample inmediatamente siguiente, **sin volver a optimizar**.

- `--modo rolling` — la ventana de optimización se desliza (se adapta a cambios de régimen).
- `--modo anclado` — el inicio queda fijo y el in-sample crece (usa toda la historia).

Después viene una **revalidación cruzada**: cada candidato se vuelve a medir en todas las
ventanas out-of-sample *posteriores* al fold que lo descubrió. Lo de "posteriores" no es un
detalle: medir un candidato del fold 5 sobre el out-of-sample del fold 1 no prueba nada,
porque esos datos ya estuvieron dentro del in-sample con el que se lo ajustó.

Una combinación se acepta si cumple las cuatro:

| Criterio | Default | Por qué |
|---|---|---|
| Ventanas OOS posteriores | ≥ 3 | Con dos alcanza que le toquen los dos tramos favorables |
| Ventanas con score positivo | ≥ 60% | Consistencia, no un pico aislado |
| Degradación IS → OOS | ≤ 50% | Si se cae más que eso, estaba ajustada al ruido |
| Auditoría de período completo | no contradice | Ver abajo |

La **auditoría de período completo** corre cada finalista de punta a punta sobre todo el
histórico del estudio. Eso no valida nada —el período incluye sus propios datos de ajuste—
pero sí desmiente: si sobre 150 días la combinación pierde plata o tiene profit factor menor a
1, se descarta por más lindas que hayan salido las ventanas sueltas. Es una prueba de descarte,
no de promoción.

Los dos últimos criterios salieron de un caso concreto: con el mínimo en 2 ventanas pasó una
combinación cuyo score OOS mediano era +1,66 y que sobre los 150 días completos daba **−12,9%
con 42,3% de drawdown**. Tenía 90,3% de win rate, ganaba 1,12% promedio y perdía 10,46%
promedio — es decir, estaba exactamente en su win rate de equilibrio, con esperanza −0,007% por
trade. Las medianas de tres ventanas cortas pueden tapar que la curva completa va hacia abajo.

**Que el ranking venga vacío es un resultado válido, no un error.** Significa que en ese
espacio y ese período no hay nada cuya ventaja se sostenga.

## Función objetivo

Default `sharpe_dd`: Sharpe anualizado sobre **retornos diarios**, menos una penalización que
crece cuando el drawdown supera `--dd-objetivo`.

Dos decisiones que importan:

- El Sharpe va sobre retornos diarios y no por trade. Con retornos por trade anualizados por
  `sqrt(trades/año)` salen números disparatados (Sharpe de 70) y se premia la alta frecuencia
  por sí misma: dos estrategias con la misma curva de equity puntuarían distinto solo porque
  una parte sus entradas en más pedazos.
- El mínimo de trades es una **tasa por mes** (`--min-trades-mes`), no un absoluto, y escala
  al largo de cada ventana. Con un absoluto, una ventana de validación de 15 días quedaría
  descalificada contra un umbral pensado para 45, y el score fuera de muestra terminaría
  midiendo cuánto opera la estrategia en vez de qué tan bien lo hace.

La penalización por pocos trades es una rampa y no un muro: un muro plano no le da al
muestreador ninguna pista de hacia dónde moverse.

Alternativas: `--objetivo calmar` (retorno / peor drawdown) y `--objetivo profit_factor`.

## Qué NO se optimiza

Palanca, sizing, comisión, slippage, funding y capital inicial son datos de la cuenta real, no
cosas que el optimizador deba descubrir. Se pasan por CLI (`--palanca`, `--posicion`,
`--capital`). Dejar que el optimizador suba la palanca es apalancar ruido, no encontrar señal.

Los filtros de posicionamiento (top traders / retail / pendiente del ratio) están fuera del
espacio de búsqueda: su histórico es mucho más corto que el de las velas y dejarlos entrar
recortaría en silencio el período con el que se puede validar.

## Cobertura de datos

Las velas de 1m van bastante más atrás que los datos auxiliares. Antes de arrancar, el script
avisa si la ventana pedida empieza antes de que existan ballenas u open interest: un estudio
que optimiza un filtro sobre un período sin esos datos no falla, simplemente no abre trades, y
el optimizador "aprende" que ese filtro es malísimo. Esa es una conclusión inválida sacada de
un hueco de datos.

Para ver la cobertura actual: `SELECT MIN(fecha) FROM ballenas`, `SELECT MIN(tiempo) FROM
open_interest`, `SELECT MIN(open_time) FROM klines_1m`.

## Salidas

- `resultados/trials-<fecha>.csv` — una fila por evaluación (in-sample, out-of-sample y
  revalidación), con métricas, una columna por parámetro y el JSON completo. Los trials
  descartados son los que dicen qué rangos no sirven.
- `resultados/reporte-<fecha>.json` — ranking con el detalle por ventana. Cada entrada trae
  `paramsBacktest`: el body exacto de `POST /api/estrategias`, así que una combinación se
  puede guardar y abrir en `/estrategias` sin traducir nada a mano.

## Agregar parámetros o estrategias

En `espacio.js`, agregar el descriptor al bloque que corresponda:

```js
{ nombre: 'miUmbral', tipo: 'float', min: 0.1, max: 5, escala: 'log', si: c => c.useMiFiltro }
```

Si el nombre no coincide con el que espera el backtest, traducirlo en `aParamsBacktest()`
(como se hace con los presets de EMAs). Si hay relaciones entre parámetros —que uno tenga que
ser menor que otro— agregarlas a `reparar()`: se reparan en vez de descartarse, porque
descartar desperdicia el trial y sesga el muestreo hacia las zonas donde por casualidad no hay
conflictos.
