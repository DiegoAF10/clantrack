# -*- coding: utf-8 -*-
"""Payload v3: brief + cola de acciones + datos (sin biblia)."""
import sys, io, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

d = json.load(open('tablero_data.json', encoding='utf-8'))
d.pop('narrativa', None)

d['brief'] = dict(
    parrafos=[
        'La semana vendió Q10,544 (−3.3%) — la mitad de lo que este mismo catálogo hacía en 2024. El problema sigue siendo ejecución, no demanda: el sistema reporta 91% de instock, pero los conteos físicos prueban 536 unidades fantasma. Góndolas llenas en papel, vacías en la realidad — y un motor de reposición que por eso no pide.',
        'Además: 79 unidades de reclamos (Q3,630) concentradas en Sapporo —50 en una sola tienda—, producto vencido en góndola en Bosques, y 200+ unidades empacadas sin plano en WM Roosevelt. Nada de esto se arregla solo. Abajo está la cola: cada problema con su responsable y el mensaje listo para mandar.',
    ],
    relojes=[
        dict(fecha='MAR 02/09', que='Vence la vigencia de 6 ítems (gestión ID 90884 con Max)'),
        dict(fecha='JUE 04/09', que='Cancela la OC 2401060275 — avisar el despacho parcial antes'),
        dict(fecha='VIE 05/09', que='Corte de uplift — define la segunda ola de Helles'),
    ])

d['acciones'] = [
 dict(id='A1', prioridad=1, plazo='HOY · vence mar 02/09', titulo='Cerrar la extensión de vigencia con Max (ID 90884)',
      evidencia='6 ítems se inactivan el martes. Sin extensión: no sale el refuerzo de Rasputin (Q12,264) y el stock ya en tiendas de esos 6 queda sin poder venderse.',
      quien='Max Sosa', canal='WhatsApp + correo',
      mensaje='Max, buenas tardes. Le escribo por la gestión ID 90884 (extensión de vigencia de los ítems temporales): seis ítems vencen este martes 2 de septiembre. Sin la extensión no podemos incluirlos en la orden de refuerzo que estamos por enviar a Cristian, y el producto que ya está en tiendas quedaría fuera de venta. ¿Nos ayuda a cerrarla hoy o mañana? Quedo pendiente.\n\nDiego Arriaza · Clan Cervecero'),
 dict(id='A2', prioridad=1, plazo='HOY · la OC cancela jue 04/09', titulo='Avisar a Cristian el despacho parcial de la OC 2401060275',
      evidencia='La OC pide 200 HB Original (bodega: 9 u) y 120 Trooper (bodega: 64). Despachando el resto completo, el fill rate queda en 70% avisado — no en 57% en silencio.',
      quien='Cristian Lima', canal='correo',
      mensaje='Asunto: OC 2401060275 — despacho parcial y reposición\n\nCristian, buenas tardes. Sobre la OC 2401060275 (31/08): despachamos completo Sapporo Silver (96), Sapporo Black (40), HB Dunkel (20) y Trooper Fear of the Dark (100). En HB Original y Trooper Original entregamos el disponible actual (9 y 64 unidades); el resto lo reponemos con el siguiente embarque y le confirmo fecha en el tablero del lunes. Preferimos avisarle antes del recibo para que el fill rate quede explicado y no como faltante ciego.\n\nSaludos,\nDiego Arriaza · Clan Cervecero'),
 dict(id='A3', prioridad=1, plazo='Esta semana', titulo='Firmar el pedido de refuerzo y enviarlo a Cristian',
      evidencia='Plan recomendado listo: 67 cajas · 1,340 u · Q29,314 a 29 tiendas (Helles 51 cajas). El manifiesto interactivo permite ajustar caja por caja y exportar la orden.',
      quien='Diego (firma) → Cristian', canal='manifiesto → correo',
      mensaje='[Abrí el manifiesto, ajustá si querés y usá su botón COPIAR ORDEN]\nfile:///C:/Users/Diego/projects/clan/coo/walmart/expediente-walmart-2026/pedido-refuerzo/PEDIDO-REFUERZO.html'),
 dict(id='A4', prioridad=2, plazo='Esta semana', titulo='Pedir a Cristian el ajuste de inventario de Villa Nueva',
      evidencia='417 u fantasma en 13 ítems, contadas 2 veces (26 y 28/08) con el mismo resultado: Silver 68→0, Trooper 183→23. Con instock "sano", la tienda jamás genera resurtido.',
      quien='Cristian Lima', canal='correo',
      mensaje='Asunto: WM Villa Nueva — ajuste de inventario (diferencia verificada dos veces)\n\nCristian, buenas tardes. En WM Villa Nueva (tienda 414) el conteo físico del 26/08 —repetido el 28/08 con resultado idéntico— muestra una diferencia de 417 unidades contra el sistema en 13 de nuestros ítems. Los casos mayores: Sapporo Silver sistema 68 / físico 0 · Trooper Original sistema 183 / físico 23 · Trooper FOTD 20/0 · Sapporo Black 20/0. En tienda quedó escalado al coordinador de abarrotes (bodega saturada). Mientras el sistema muestre esas existencias, la tienda no genera resurtido. ¿Nos apoya con el ajuste? Le adjunto el detalle por ítem.\n\nAnexo menor: Paiz Altos (Old Rasputin 17/1, Scrimshaw 17/8) y Paiz Roosevelt (Sapporo Black 20/0, FOTD 27/3).\n\nSaludos,\nDiego Arriaza · Clan Cervecero'),
 dict(id='A5', prioridad=2, plazo='Esta semana', titulo='Retirar los vencidos de WM Bosques',
      evidencia='Todo el Trooper FOTD de la tienda venció en 5/2026 y sigue exhibido; más 12 Sapporo Black, 12 Gold (vencidas desde 31/5/25) y 12 Suffolk. Es imagen y es espacio muerto.',
      quien='Victor Hugo (GSP)', canal='WhatsApp grupo',
      mensaje='Equipo, buen día. En WM Bosques quedó reportado producto vencido en sala: todo el Trooper FOTD (venció 5/2026), 12 Sapporo Black, 12 Sapporo Gold y 12 St. Peter\u2019s Suffolk. Por favor coordinar con el jefe de área el retiro a merma esta semana y mandar foto de antes y después. Prioridad alta: es imagen de marca y espacio de góndola.'),
 dict(id='A6', prioridad=2, plazo='Próxima visita', titulo='Averiguar la causa de los reclamos de Pinula y Mazatenango',
      evidencia='79 u reclamadas esta semana (≈Q3,630): 50 en Pinula (16 Black + 22 Gold + 12 Silver) y 24 en Mazatenango. Concentración así = causa única, no mala suerte.',
      quien='GSP (mercaderista de ruta)', canal='WhatsApp grupo',
      mensaje='Equipo, en Paiz Santa Catarina Pinula el sistema registró reclamos por 50 unidades de Sapporo la semana pasada (16 Black, 22 Gold, 12 Silver), y 24 más en Mazatenango. En la próxima visita: preguntar al encargado de recibo la causa registrada (recepción, daño o vencimiento) y mandar foto del reporte. Con eso decidimos si procede reclamo formal de nuestro lado.'),
 dict(id='A7', prioridad=3, plazo='Esta semana', titulo='Escalar el planograma de WM Roosevelt (producto empacado)',
      evidencia='Cuadra al 100% en papel, pero 68 Dunkel + 108 HB Original + 36 Silver llevan 15 días reempacados porque el modular nuevo no los contempla. Disponibles en sistema, invendibles en piso.',
      quien='Sergio (GSP · escalamiento planograma)', canal='WhatsApp',
      mensaje='Sergio, buen día. En WM Roosevelt hay 200+ unidades nuestras reempacadas desde el modular de hace 15 días (68 HB Dunkel, 108 HB Original, 36 Sapporo Silver, más FOTD y St. Peter\u2019s) que el plano nuevo no contempla. ¿Podés escalar con la tienda la reubicación o corrección del planograma? El sistema las da por disponibles pero no están a la venta.'),
 dict(id='A8', prioridad=3, plazo='Próxima visita', titulo='Rastrear la llegada perdida de Aguilar Batres',
      evidencia='12 Sapporo Silver llegaron el 16/08 y no aparecen ni en sistema ni en físico. Una caja completa evaporada.',
      quien='GSP + recibo de tienda', canal='WhatsApp grupo',
      mensaje='Equipo, en Paiz Aguilar Batres una llegada de 12 Sapporo Silver del 16/08 no aparece ni en sistema ni en físico. En la próxima visita verificar con recibo si quedó pendiente de ingresar o si se ingresó con otro código. Es una caja completa.'),
 dict(id='A9', prioridad=3, plazo='Con el correo del lunes', titulo='Cerrar con Cristian el estatus BORRADO de Mega 6',
      evidencia='12 de 18 SKU figuran BORRADOS en el maestro de esa tienda. No es quiebre: es baja de surtido. Pendiente desde la reunión del 27.',
      quien='Cristian Lima', canal='correo (mismo hilo)',
      mensaje='Cristian: en Paiz Mega 6 nos aparecen 12 de 18 códigos como BORRADO en el maestro de la tienda. ¿Es una decisión de cadena o local? Si es local, quisiéramos reactivarlos con el modular de octubre.'),
 dict(id='A10', prioridad=4, plazo='Sin prisa · con Max', titulo='Proponer reactivar Sapporo Gold',
      evidencia='Vendió Q1,018 esta semana (20 u) estando INACTIVO y en liquidación. Bosques ya lo metió a plano de cámara fría por su cuenta. La demanda está avisando.',
      quien='Max Sosa', canal='conversación',
      mensaje='Max: un dato de la semana — Sapporo Gold, aun inactivo y en liquidación, vendió Q1,018 (20 unidades) y una tienda ya lo integró a su plano de cámara fría. Creemos que vale reactivarlo formalmente para la temporada. ¿Lo conversamos con el modular de octubre?'),
]
json.dump(d, open('datos_v3.json', 'w', encoding='utf-8'), ensure_ascii=False)
print('datos_v3.json ·', len(json.dumps(d, ensure_ascii=False)), 'chars ·', len(d['acciones']), 'acciones')
