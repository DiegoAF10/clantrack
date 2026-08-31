# Pedido de refuerzo por tienda — auditoría de la sesión · 31-ago-2026

Ejecuta el **pendiente 9** del expediente (restock recalculado con datos frescos) con el espíritu
del pendiente 5 (dónde necesita visibilidad Clan). Entregable: `PEDIDO-REFUERZO.html`.

## Fuentes usadas (todas verificadas en esta sesión)

| Fuente | Corte | Qué aportó |
|---|---|---|
| Retail Link ×5 (`datos-crudos/f1..f5`) | sem 202630 cerrada · bajados 30/08 | Sell-out, existencias, matriz ítem×tienda, mapa de modulado (f5), venta por día (f3) |
| OC 2401060275 (`datos-crudos/OC-*.pdf`) | 31/08 · cancela 04/09 | La orden automática: 576 u, Q16,726.84, **solo los 6 modulados** |
| Odoo vivo (xmlrpc, contexto company_id=1) | 31/08 | Stock libre de bodega por ítem (tabla abajo) |
| Chat WhatsApp GSP (zip 2) | mensajes al 29/08 | Reconteo Villa Nueva 26/08 **reconfirmado 28/08** + conteos de 11 tiendas 27-29/08 |
| HUB v4 (expediente `datos/`) | 27/08 | Modelo de pedido, vencimientos, sell-through, pesos por tienda (Q sem 628/629), catálogo u/caja |

Los 5 .xls de Retail Link son .xlsx con extensión vieja (trampa conocida del expediente).

## Validación de los sugeridos preliminares del arranque

| Ítem | Preliminar | Validación con datos frescos | Decisión en el plan recomendado |
|---|---|---|---|
| HB Helles | ~1,708 u | Confirmado en Modelo Pedido; PERO la tasa 4.4 u/tda/sem es la del mejor escenario — sem 630 vendió 18 u con solo ~9 tiendas abastecidas. Vigencia corre hasta 10/11 → cabe segunda ola tras el corte de uplift 05/09 | **1,020 u (51 cajas)** — 2 cajas/tienda elegible, 3 al top-8. El plan modelo (1,700 u) queda como preset alternativo |
| NC Old Rasputin | ~579 | Vende 7-9 u/sem red; 346 u ya en tiendas (las tiendas fuertes YA tienen stock — se saltan) | **280 u (14 cajas), CONDICIONADO** a extensión de vigencia (vence 02/09) |
| Belhaven Black | ~111 | Vendió 2u/629 + 1u/630; 68 u en 6 tiendas | **40 u (2 cajas)** quirúrgicas donde probó moverse |
| HB Session | ~40 | 0 ventas en 630; 137 u en tiendas (sobre-stock) | **0** |
| NC Scrimshaw | sobre-stock | Confirmado: 0 ventas, 508 u en tiendas | **0** — pero pedir extensión para poder vender lo existente |
| Belhaven Best / McCallums | sin tasa | Confirmado: 0 ventas ambos; 95/91 u en tiendas | **0** — piloto = empujar lo que ya está vía mercaderistas |

## Los dos planes del entregable

- **Recomendado (COO):** 67 cajas · 1,340 u · **Q29,314** al costo · 29 tiendas beneficiadas · mínimo 1 caja por tienda.
- **Modelo completo:** 122 cajas · 2,440 u · **Q55,434** (vs Q55,354 del HUB — diferencia = redondeo a cajas ✓).
- Ambos caben en bodega (Odoo libre: Helles 4,222 · Rasputin 918 · Black 827 · Session 1,492).
- Distribución: peso = promedio Q de venta por tienda semanas 628+629+630; overrides de físico real donde hubo conteo (26-29/08); redondeo a cajas de 20 u; se salta tiendas con stock sano del ítem.
- **Bug cazado en auditoría interna:** f1 trae varias filas por combo tienda×ítem (diarias + foto);
  la primera corrida contaba filas en vez de tiendas (el top-8 de Helles se volvía top-1) e inflaba
  `exist_red` donde había override físico. Corregido con dedupe explícito + assert (`compute.py`).

## Prerrequisito Villa Nueva — resuelto por evidencia

Reconteo del chat 26/08 19:01 **reconfirmado idéntico el 28/08 17:46** y contrastado contra Retail
Link fresco del 30/08: el sistema mantiene las mismas cifras infladas (Silver 68 vs físico 0 ·
Trooper 183-184 vs 23). **417 unidades fantasma en 13 de 14 ítems.** Conclusión: el físico es la
verdad; la tienda queda **EXCLUIDA del pedido** hasta que Walmart aplique ajuste de inventario
(incluido en la lista de ajustes para Cristian del entregable).

## Hallazgo colateral urgente (no pedido, pero crítico)

La OC automática de hoy pide **200 HB Original (bodega: 9 u)** y **120 Trooper Original (bodega:
64 u)**. Despachada tal cual → fill rate 57% la semana que estrena el tablero de los lunes.
Decisión de Diego: despacho parcial con aviso a Cristian antes del 04/09, o pedir ajuste de la OC.

## División de trabajo (regla 15 de la orquesta)

Números: calculados y verificados por Claude (este documento + `scripts/`). Maqueta HTML: Kimi K3
(worker frontend) contra un contrato de datos con muestras falsas — **ningún número real viajó a
Moonshot**; el JSON verificado (`datos_final.json`) se inyectó localmente.

## Reproducibilidad

`scripts/parse.py` (los 5 RL → CSVs) → `scripts/compute.py` (motor del plan recomendado + anexo VN)
→ `scripts/compute2.py` (payload final con ambos planes) → `scripts/inject.py` (inyección en la
maqueta). Stock bodega: `scripts/odoo_stock.py`.

## Qué NO quedó resuelto

- La **extensión de vigencia** (gestión ID 90884) sigue abierta con Max — condición dura del capítulo Rasputin y de la venta del stock existente de los 6 que vencen 02/09.
- "Paiz Pacific Center" del chat se mapeó tentativamente a Paiz Salida al Pacífico (458) — confirmar con GSP.
- La OC del buzón WebEDI: solo llegó el PDF de la 2401060275; si hay más OCs sin abrir, no pasaron por esta sesión.
