# Cuaderno de la semana 202630 — auditoría · 31-ago-2026

Tablero de visibilidad semanal para Diego y su papá: ventas, instock, discrepancias piso-vs-sistema
y reportes de mercaderistas. Entregable: `CUADERNO-SEMANA-630.html`. Cubre de facto el espíritu del
**pendiente 8** (resumen tienda por tienda: hecho · falta · discrepancias).

## Fuentes

| Fuente | Qué aporta |
|---|---|
| Retail Link 202630 ×5 (en `../pedido-refuerzo/datos-crudos/`) | Venta, existencias, instock, reclamos; combos deduplicados (venta=SUMA, existencia=MAX) |
| HUB v4 · hoja 4. SELL-OUT | Semanas 628/629 por ítem y por tienda |
| Reporte mercaderistas v2 (JSON, 14–24/08) | 19 reportes estructurados + análisis transversal + brecha VN histórica |
| Chat WhatsApp GSP (26–29/08) | 14 conteos frescos, discrepancias, hallazgos cualitativos |
| Odoo vivo | (indirecto, vía sesión pedido-refuerzo) |

## Números verificados (cross-check triple)

- Venta 202630: **Q10,543.56 · 297 u** — reconcilia por ítem, por tienda y por curva diaria (3 vías).
- Curva: Vie+Sáb+Dom = 197 u = **66% de la semana** (sábado solo: 85 u = 29%).
- Serie: 628 Q10,325 · 629 Q10,906 · 630 Q10,544 · referencia 2024: Q20,486/sem.
- Instock sistema: **91%** de 428 combos con existencia > 0.
- **Fantasmas: 536 u** en 20 combos (RL 30/08 vs conteos físicos 26–29/08); VN concentra ~430.
- **Reclamos: 79 u ≈ Q3,630** al costo — 79% Sapporo; Pinula 50 u, Mazatenango 24 u.
- Heatmap: 428 celdas → 243 ok · 92 bajo · 42 cero · 31 alto · 20 fantasma.
- Cobertura mercaderistas: 14 tiendas con conteo fresco 26–29/08 · 23 acumuladas desde el 14/08.

## Decisiones de método

- Existencias del sistema = Retail Link del 30/08 (no las cifras de sistema que dictan los
  mercaderistas por chat, que difieren ±1-3 u por fecha); físico = conteo GSP más reciente.
- La evolución de Villa Nueva usa los pares sistema/físico **de cada fecha de conteo** (14, 21,
  26 y 28/08) — por eso su "471 vs 54" difiere unas unidades del corte RL del 30/08.
- "Paiz Pacific Center" mapeada tentativamente a Salida al Pacífico (458) — igual que en el pedido.
- Instock REAL agregado NO se publica como KPI: solo VN tiene conteo 100% completo; un agregado
  mezclando tiendas contadas a medias sería un número inventado.

## División de trabajo

Regla 15: Kimi K3 maquetó contra contrato de datos con muestras falsas (cero números reales a
Moonshot); cálculo, verificación e inyección local: Claude (`scripts/tablero1.py` → `tablero2.py`
→ `inject2.py`).

## v2 · 31-ago tarde — rediseño total tras feedback de Diego

La v1 (maqueta Kimi) falló la revisión de Diego en dos ejes: calidad visual (barras ilegibles,
cifras quebradas en dos líneas, sellos encimados) y concepto (era una "biblia" de lectura sin
capa de acción). La v2 la escribió Claude completa (`scripts/SEMANA-630.html` es la fuente):

- **Arquitectura nueva**: brief de 60 segundos + LOS RELOJES + **cola de 10 acciones** con
  responsable, canal, mensaje listo para copiar (voz Clan) y estado pendiente→enviada→resuelta
  persistido en localStorage con barra de progreso. La narrativa larga se eliminó.
- **Craft**: paleta de estados validada con el método dataviz (rojo #b3282d · ámbar #b7791f ·
  azul #1b6fa8 — todos los pares pasan CVD y contraste; "ok" en tinte callado), números SIEMPRE
  monospace tabulares alineados a la derecha, barras SVG a escala de columna, sparkline de
  tendencia 628→630, gauges con banda de color, heatmap con encabezados cortos y fantasma
  rayado con "!", tooltips propios, panel lateral por tienda.
- Datos idénticos a la v1 (triple-reconciliados) + bloque `acciones` (`scripts/datos_v3.py`).
- Bugs cazados en auditoría visual propia: gauge sin display:block (no pintaba) ·
  encabezados del mapa amontonados.
