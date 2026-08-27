# Post-reunión Walmart 27-ago-2026 — arranque de la siguiente sesión

**Para la sesión que retome esto:** leé primero este documento entero; el contexto fino está en
el acta (`C:\Users\Diego\Escribano\2026-08-27 Reunion Walmart presencial\acta.md`) y en la
memoria `clan-reunion-walmart-27ago`. La reunión APROBÓ: 4-packs (Trooper Q150 · Hobgoblin
Q125 · pack alemán Hofbräu con vaso), 10 cervezas nuevas (PVP firmados abajo), piloto italiano
en top 5 tiendas, Twisted Tea. Calendario firmado: **entrega 20/09 · surtido 28–30/09 ·
góndola 1/10 · top 20 tiendas · exclusividad de prueba hasta Semana Santa.**

## Los PVP firmados (cliente final, con IVA · caja 24 salvo nota)

Guinness Draught lata Q50 · Murphy's Irish Stout lata Q30 · Blue Moon **botella** Q18.75
(introducción; se registran botella Y lata, la lata releva ~ene-2027) · Samuel Adams 16 oz Q25 ·
Yuengling Q25 · Grolsch lata 500 Q25 · Pilsner Urquell Q25 · Kronenbourg 1664 Q25 · San Miguel
Especial Q25 · Carlsberg Q25. Descartadas: Landshark, Lagunitas, Paulaner, Franziskaner.
4-packs sin precio impreso en el empaque.

## Pendientes en orden de fuego

### 1 · Creación masiva de ítems en «el robot» — URGENTE (Diego lo prometió el 27)
Generar con Claude el alta masiva según el formato de la plantilla de Walmart. Alcance: las 10
cervezas nuevas + Blue Moon lata (registro adicional, fecha selectiva ene-2027) + 3 packs
(Trooper, Hobgoblin, alemán con vaso — el alemán aún sin código: Diego dijo que saca el código
de barras en 2 días) + 4 variedades italianas Mastri Birrai Umbri (caja de 6) + Twisted Tea.
Diego provee la plantilla/formato del robot; los códigos de barra de los nuevos salen de Odoo
(`product.product.barcode`, credenciales en `~/.clan-odoo/credentials.json`) o de las cajas
físicas. Los UPC del catálogo ACTUAL (20 ítems) ya están extraídos en `upcs-retail-link.json`
(esta carpeta).

### 2 · Enviar el correo de seguimiento
El borrador quedó listo en la sesión del 27 (transcrito abajo, sección «Correo»). Diego lo
copia y lo manda a Max + Cristian, CC Carlos.

### 3 · Corregir los diseños de los 4-packs contra el troquel
[No se habló en la reunión] Hay líneas que no encajan con el plano + un par de cambios que
Diego indicará. Fuentes: `Downloads\4 Pack Hobboblin.pdf`, `Downloads\Tropper 4 Pack FINAL.pdf`,
`Downloads\Trooper 4 Pack sin etiquetas.pdf`, HTMLs 3D en Downloads. ⏰ La imprenta tarda
10–14 días y la entrega es el 20/09 → **los archivos deben estar en imprenta ~5/09.**

### 4 · Sugerido de tiendas: top 20 para packs + top 5 por ticket para italianas
Comprometido en la mesa. Insumos ya calculados: ranking de venta por tienda semana 202629
(Américas Q1,308 · Asunción Q865 · Pradera Q717 · Cobán Q631 · Mont Blanc Q479 · del Norte
Q445 · Col. V. Hermosa Q427 · Mega 6 Q377 · Pinula Q339 · Novicentro Q338 · WM Roosevelt Q336
· WM San Cristóbal Q332 · Atanasio Q325 · Aguilar Batres Q319 · Bosq. S. Nicolás Q289 ·
Huehue Q252 · Utatlán Q235 · P. San Cristóbal Q230 · El Naranjo Q230 · Mazatenango Q219 — ese
es el top 20 por venta; validar contra ranking del hub por Q recuperable antes de mandar).
Entregable: tabla simple para Walmart.

### 5 · Brief a mercaderistas: dónde necesita visibilidad Clan
Entregable: brief de 1 página + mensaje para el grupo de WhatsApp GSP. Insumos: Sapporo Silver
es el 23.7% de la venta (prioridad #1 de neveras y frentes), el plan de restock 3 capas, los
espacios adicionales que Walmart abre (lateral en negociación), y las colocaciones que ya
probaron efecto (Utatlán +56%/día tras espacio adicional).

### 6 · Propuesta formal MaxShelf + sugeridos en DOH para Cristian
Comprometida «el lunes» en el deck. Base: hoja MAX SHELF (VNDR9) del hub + Modelo Pedido
(stock de seguridad ya calculado) + venta por tienda. Formato: como Cristian lo pidió por
correo el 25/08 — stock de seguridad vs venta, DOH con tubería descontada.

### 7 · Piloto de cerveza importada barata (Q10–15) para Maxi Despensa / Despensa Familiar
Concesión pedida por Walmart en la mesa; Diego dijo «esta semana». Buscar candidata en Odoo
que aguante ese PVP con margen (Landshark: 12,863 u en bodega, lista Q15 — correr números;
pocas cajas, prueba de 3 meses).

### 8 · Resumen completo tienda por tienda — hecho · lo que falta · discrepancias
Consolidar 4 fuentes: JSON mercaderistas v2 (`Downloads\reporte_mercaderistas_WM_PAIZ (1).json`),
chat WhatsApp post-24/08 (`Downloads\WhatsApp Chat - CLAN CERVECERO WM-PAIZ (1).zip`), VISUAL
actualizado (`Downloads\VISUAL- CLAN CERVECERO.xlsx`, hoja DATOS columna AX = estado por
tienda), y Retail Link por tienda. Es material de decisión → **entregable HTML** (regla de la
casa).

### 9 · Restock recalculado con datos frescos → orden de compra («la otra semana»)
⚠️ PRERREQUISITO: **reconciliar Walmart Villa Nueva** — el reconteo del chat (26/08 19:01) y el
Excel maestro difieren en 8 de 20 ítems (Trooper Original: sistema 184 vs 21). Sin resolver
eso no se emite pedido de esa tienda. Insumos: sugerido 3 capas ya construido
(`documentos/sugerido-restock-27ago.html` en el repo infografias), tubería 380 u (HB Original
140 · Trooper Orig 100 · Silver 60 · FOTD 60 · Dunkel 20), y la semana 202630 que cierra el
viernes 28 (bajar Retail Link el 30–31/08 para números limpios).

### 10 · Visita al centro de distribución (validar empaquetados) — antes del 20/09
Coordinar con Walmart. Llevar los 3 packs y las cajas master.

### 11 · Tablero de los lunes #1 — lunes 31/08 o 1/09
Primer envío del tablero semanal (5 KPIs). Sumar el pedido elegante: el scorecard de fill
rate/OTIF de Clan. Con la semana 202630 cerrada, este primer tablero puede traer además la
primera lectura del uplift (corte formal 05/09).

## Backlog (sin fecha, no bloquean)
- Exploraciones que pidió Walmart: categoría 0.0 (Athletic Brewing), hard seltzers (White
  Claw), cheladas/micheladas con marcas no-Modelo, bebidas con proteína, Founders (sponsor de
  LaLiga — el papá iba a escribirles).
- Barril 5 L de sabores del proveedor alemán (ofrecido, sin decisión).
- Descuento de factura para invertir en visibilidad (Diego lo propuso, quedó por conversar).
- BORRADO Mega 6: cerrar con Cristian (¿cadena o local? — la visita del 21 contó 12 SKU, la
  del 26 contó 8; pedir el corte oficial del maestro).
- El informe de campo impreso dice «497 unidades»; la cifra fina es 492 + 28 en merma —
  corregir si se regenera.
- Infra: el deck y los documentos de esta saga quedaron commiteados en el repo `infografias`
  rama `obra/ventus-mes1` (una sesión paralela de VENTUS movió la rama) — cherry-pick a main
  cuando esa sesión suelte el repo.

## Materiales fuente (rutas completas)
- Acta + transcripción: `C:\Users\Diego\Escribano\2026-08-27 Reunion Walmart presencial\`
- Deck final (15 láminas, notas con N): `C:\Users\Diego\Downloads\Presentacion_Clan_Walmart_27ago.html`
  (fuente: `C:\Users\Diego\projects\infografias\documentos\deck-jueves27.html`)
- Sugerido restock: `Downloads\Sugerido_Restock_Clan_27ago.pdf` · Informe de campo:
  `Downloads\Informe_Campo_Mercaderistas_24ago.pdf` · Seguimiento v2:
  `Downloads\Seguimiento_Clan_Cervecero_Walmart_27ago_v2.pdf` · Infografía:
  `Downloads\Infografia_Clan_Walmart_24ago.pdf` (fuentes HTML de todos en
  `projects\infografias\documentos\`)
- Tabla de acciones para Max: `Downloads\Tabla_Acciones_Items_Max_27ago.xlsx`
- Selección de ítems (HTML interactivo): `Downloads\Seleccion_Items_Nuevos_Clan.html`
- Hub de datos v4: `Downloads\HUB_Walmart_Paiz_Clan_Cervecero_4.xlsx` · Retail Link crudo del
  27/08: `Downloads\oj3kh7a_113479488_...xls` (es un .xlsx con extensión vieja: copiarlo a
  .xlsx para abrirlo) · VISUAL: `Downloads\VISUAL- CLAN CERVECERO.xlsx` · Chat:
  `Downloads\WhatsApp Chat - CLAN CERVECERO WM-PAIZ (1).zip`
- Troqueles packs: `Downloads\4 Pack Hobboblin.pdf` · `Downloads\Tropper 4 Pack FINAL.pdf`
- Odoo en vivo: xmlrpc con `~/.clan-odoo/credentials.json` (dominio vacío = `[]`)
- UPC del catálogo actual: `upcs-retail-link.json` (esta carpeta)
- Branding Clan para cualquier pieza: `projects\infografias\BRANDING.md` + tokens

## Correo de seguimiento (listo para enviar — pendiente 2)

Para: Max.Sosa@walmart.com; Cristian.Lima@walmart.com · CC: Carlos.Batz@walmart.com
Asunto: Clan Cervecero — acuerdos de la reunión de hoy y calendario al 1 de octubre

Max, Cristian, buenas tardes:

Gracias por la reunión de hoy — a Max y Hansel por el tiempo y la claridad. Dejo por escrito lo acordado para que avancemos sobre lo mismo.

LO APROBADO. Los dos 4-packs (Trooper a Q150 y Hobgoblin a Q125, sin precio impreso) más el pack alemán con vaso, en exclusividad de prueba hasta Semana Santa; las diez cervezas nuevas de la propuesta, con Guinness (Q50), Blue Moon botella (Q18.75 de introducción) y Samuel Adams como prioridad; el piloto de las italianas en las cinco tiendas de mayor ticket (caja de 6, cuatro variedades); y el Twisted Tea con entrega inmediata. Entrada conservadora: top 20 tiendas, y ampliamos donde la venta lo pida.

EL CALENDARIO. Hoy dejo creados los ítems. Sigue la validación de empaques en el centro de distribución, entregamos el 20 de septiembre, surtido el 28–30 y arrancamos góndola el 1 de octubre.

LO QUE QUEDA EN CADA CANCHA. De nuestro lado: el sugerido de tiendas para los packs, la propuesta del piloto de cerveza importada de precio bajo para Maxi Despensa y Despensa Familiar (esta semana), y la imprenta de los empaques al visto bueno. De su lado: el espacio lateral que Hansel conversa con operaciones, el apoyo de publicidad para los lanzamientos, y con Cristian retomamos el estatus BORRADO de Mega 6 y la orden de compra del restock la próxima semana.

Seguimos con el tablero cada lunes.

Saludos,

Diego Arriaza
Clan Cervecero
diego@clancervecero.com
