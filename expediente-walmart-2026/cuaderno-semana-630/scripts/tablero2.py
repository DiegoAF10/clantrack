# -*- coding: utf-8 -*-
"""Ensambla tablero_data.json — el tablero semanal 202630 para Diego y su papá."""
import sys, io, json, warnings
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
warnings.filterwarnings('ignore')
import pandas as pd

dd = pd.read_csv('t_combos_dedupe.csv', dtype={'item': str, 'tienda_num': str})
hist = json.load(open('t_hist.json', encoding='utf-8'))
base = json.load(open('datos.json', encoding='utf-8'))          # planes, VN, top20, tiendas prom
v2 = json.load(open(r'C:\Users\Diego\projects\clan\coo\walmart\expediente-walmart-2026\datos\reporte_mercaderistas_v2.json', encoding='utf-8'))

CLASE = {'75214930':'MODULADO','75437621':'MODULADO','75437623':'MODULADO','75437610':'MODULADO',
         '75437614':'MODULADO','75437616':'MODULADO',
         '75437615':'NO MODULADO','75437613':'NO MODULADO','75437612':'NO MODULADO','75437617':'NO MODULADO',
         '75437618':'NO MODULADO','75437619':'NO MODULADO','75437620':'NO MODULADO'}
NOMBRE = {'75214930':'Sapporo Silver','75437621':'Trooper Original','75437623':'Trooper FOTD',
          '75437610':'Sapporo Black','75437614':'HB Original','75437616':'HB Dunkel',
          '75437615':'HB Helles','75437613':'NC Old Rasputin','75437612':'NC Scrimshaw',
          '75437617':'HB Session','75437618':'Belhaven Best','75437619':'Belhaven Black','75437620':'Belhaven McCallums',
          '75214932':'Sapporo Gold','75253328':'StPet Suffolk','75253329':'HB Munchner',
          '75244435':'Hobgoblin Ruby','75244431':'StPet Stout','75244433':'Hobgoblin Gold','75244436':'Hobgoblin Stout'}
COSTO = {'75214930':45.54,'75437621':18.25,'75437623':32.85,'75437610':51.0,'75437614':22.0,'75437616':22.0,
         '75437615':16.0,'75437613':43.8,'75437612':43.8,'75437617':16.0,'75437618':18.25,'75437619':18.25,
         '75437620':18.25,'75214932':45.54,'75253328':32.85,'75253329':22.0,'75244435':32.85,'75244431':32.85,
         '75244433':32.85,'75244436':32.85}

# físico contado (26-29/08, chat) — overrides de la sesión anterior + modulados detectados
FISICO = {}
for f in base['matriz']:
    if f.get('fisico') is not None and f['fisico'] == f['fisico']:  # descarta NaN
        FISICO[(f['tienda'], f['item'])] = int(f['fisico'])
# VN completo (incluye modulados)
for f in base['villa_nueva']['filas']:
    FISICO[('414', {'Sapporo Silver':'75214930','Trooper Original':'75437621','Sapporo Black':'75437610',
                    'Sapporo Gold':'75214932','HB Original':'75437614','HB Dunkel':'75437616',
                    'Trooper FOTD':'75437623','HB Helles':'75437615','HB Session':'75437617',
                    'NC Old Rasputin':'75437613','NC Scrimshaw':'75437612','Belhaven Best':'75437618',
                    'Belhaven Black':'75437619','StPet Suffolk':'75253328'}[f['desc']])] = f['fisico']
# Paiz Roosevelt modulados fantasma + San Cristóbal + Aguilar Batres (chat 27/08)
FISICO[('24','75437610')] = 0    # Sapporo Black sistema 20
FISICO[('24','75437623')] = 3    # FOTD sistema 27
FISICO[('169','75437623')] = 16  # sistema 22
FISICO[('33','75437615')] = 20   # sistema 16 (fisico MAYOR)

# ---------- items ----------
hist_map = {h['item']: h for h in hist['items_hist']}
items = []
for it, g in dd.groupby('item'):
    h = hist_map.get(it, {})
    items.append(dict(id=it, nombre=NOMBRE.get(it, it), clase=CLASE.get(it, 'INACTIVO'),
                      u628=int(h.get('u628', 0)), q628=round(h.get('q628', 0), 2),
                      u629=int(h.get('u629', 0)), q629=round(h.get('q629', 0), 2),
                      u630=int(g['venta_u'].sum()), q630=round(float(g['venta_q'].sum()), 2),
                      exist=int(g['existencia'].sum()), tiendas=len(g),
                      con_stock=int((g['existencia'] > 0).sum()),
                      instock_sis=round((g['existencia'] > 0).mean() * 100)))
items.sort(key=lambda x: -x['q630'])

# ---------- tiendas ----------
CONTEOS = {  # tienda_num: (fecha, quien, tipo, nota corta)
 '491': ('26-28/08','Eduardo Yat','visibilidad','Producto colocado y rotulado; no modulados pasaron a liquidación con planograma nuevo'),
 '23': ('26 y 29/08','mercaderista GSP','conteo ×2','No modulados en CERO físico; FOTD y Suffolk marcados Liquidado'),
 '414': ('26 y 28/08','mercaderista GSP','reconteo ×2','FANTASMA MASIVO: 417 u de diferencia · bodega saturada · escalado a coordinador de abarrotes'),
 '34': ('27/08','Daya','conteo','Tienda limpia: 8 ítems, físico = sistema en todos'),
 '948': ('27/08','Daya','conteo','No modulados "no resurtible"; varios BORRADO DE SISTEMA; Suffolk 28 u en merma; llegada HB Original 20 u el 29/08'),
 '4176': ('27/08','Ross Barrios','conteo','Fantasmas: Rasputin 17→1 · Scrimshaw 17→8 · Session 3→0'),
 '169': ('27/08','Ross Barrios','conteo','Casi cuadra; FOTD 22→16; Munchner y Helles "se buscan"'),
 '459': ('27/08','mercaderista GSP','conteo','VENCIDOS en tienda: FOTD completo (5/2026) + 12 Sapporo Black + 12 Gold + 12 Suffolk; Gold entra a plano de cámara fría'),
 '24': ('27/08','Daya','conteo','Fantasmas modulados: Sapporo Black 20→0 · FOTD 27→3; Helles sano con 34 u'),
 '33': ('27/08','Ross Barrios','conteo','Silver: llegada de 12 u del 16/08 desaparecida (sistema 0); Helles/Rasputin/Scrimshaw cuadran'),
 '121': ('28/08','Victor Hugo','conteo','Cuadra 100% PERO 68 Dunkel + 108 HB Original + 36 Silver empacados sin plano tras el modular de hace 15 días'),
 '458': ('29/08','mercaderista GSP','conteo','No modulados en cero; FOTD y Suffolk en liquidación (mapeo de tienda por confirmar)'),
 '4126': ('28/08','Susy Guerra','visibilidad','Visibilidad con fotos'),
 '22': ('27/08','Marisela De León','visibilidad','Visibilidad con fotos'),
}
tiendas = []
for tn, g in dd.groupby('tienda_num'):
    t = base['tiendas'].get(tn, {})
    c = CONTEOS.get(tn)
    tiendas.append(dict(num=tn, nombre=t.get('nombre', g['tienda'].iloc[0]),
                        q628=t.get('q628', 0), q629=t.get('q629', 0), q630=t.get('q630', 0),
                        prom3=t.get('prom3', 0), top20=(tn in base['top20']),
                        combos=len(g), con_stock=int((g['existencia'] > 0).sum()),
                        instock_sis=round((g['existencia'] > 0).mean() * 100),
                        contada=bool(c), fecha_conteo=c[0] if c else None, nota_conteo=c[3] if c else None))
tiendas.sort(key=lambda x: -x['q630'])

# ---------- heatmap (todos los combos) ----------
heat = []
for _, r in dd.iterrows():
    tn, it = r['tienda_num'], r['item']
    fis = FISICO.get((tn, it))
    sist = int(r['existencia'])
    real = fis if fis is not None else sist
    if fis is not None and sist - fis >= 5:
        estado = 'fantasma'
    elif real == 0:
        estado = 'cero'
    elif real < 10:
        estado = 'bajo'
    elif real >= 50:
        estado = 'alto'
    else:
        estado = 'ok'
    heat.append(dict(t=tn, i=it, e=estado, s=sist, f=fis, v=int(r['venta_u'])))

# ---------- discrepancias piso vs sistema (frescas) ----------
disc = []
for (tn, it), fis in FISICO.items():
    row = dd[(dd['tienda_num'] == tn) & (dd['item'] == it)]
    if row.empty: continue
    sist = int(row['existencia'].iloc[0])
    if sist != fis:
        nombre_t = base['tiendas'].get(tn, {}).get('nombre', tn)
        disc.append(dict(tienda=nombre_t, num=tn, item=NOMBRE.get(it, it), sistema=sist, fisico=fis,
                         delta=fis - sist))
disc.sort(key=lambda d: d['delta'])

# ---------- VN evolución ----------
vn_evo = [
 dict(fecha='14/08', sistema=257, fisico=10, nota='primer conteo · "toma de inventario / bodega en reordenamiento"'),
 dict(fecha='21/08', sistema=289, fisico=85, nota='+43 u recuperadas en 7 días'),
 dict(fecha='26/08', sistema=471, fisico=54, nota='reconteo completo · 14 ítems'),
 dict(fecha='28/08', sistema=471, fisico=54, nota='RECONFIRMADO idéntico · escalado a coordinador de abarrotes'),
]

# ---------- reclamos ----------
f1 = pd.read_csv('t_formato_nuevo_202630.csv', dtype={'item': str, 'tienda_num': str})
rec = f1[f1['reclamo_u'] != 0].groupby(['tienda', 'desc', 'item'], as_index=False).agg(u=('reclamo_u', 'sum'))
reclamos = [dict(tienda=r['tienda'], item=NOMBRE.get(r['item'], r['desc']), u=int(r['u']),
                 q_costo=round(r['u'] * COSTO.get(r['item'], 0), 2)) for _, r in rec.iterrows()]
rec_u = sum(r['u'] for r in reclamos); rec_q = round(sum(r['q_costo'] for r in reclamos), 2)

# ---------- mercaderistas: reportes v2 + frescos ----------
reportes = []
for r in v2['reportes_por_tienda']:
    hall = []
    for i in r.get('items', []):
        for k in ('inventario_reportado', 'fisico', 'sistema'):
            pass
    reportes.append(dict(tienda=r['tienda'], cadena=r.get('cadena', ''), fecha=r.get('fecha', ''),
                         por=(r.get('reportado_por') or {}).get('nombre', 'GSP'),
                         tipo=r.get('tipo_reporte', 'conteo'), origen='v2 (al 24/08)'))
for tn, c in CONTEOS.items():
    nombre_t = base['tiendas'].get(tn, {}).get('nombre', tn)
    reportes.append(dict(tienda=nombre_t, cadena='WM' if nombre_t.startswith('WM') or 'WALMART' in nombre_t else 'Paiz',
                         fecha=c[0], por=c[1], tipo=c[2], nota=c[3], origen='chat fresco (26-29/08)'))

equipo = [dict(nombre=e['nombre'], rol=e['rol']) for e in v2['equipo'].get('coordinacion', [])]

# ---------- KPIs ----------
tot_u = int(dd['venta_u'].sum()); tot_q = round(float(dd['venta_q'].sum()), 2)
fantasma_total = sum(-d['delta'] for d in disc if d['delta'] < 0)
tiendas_contadas = len(set(list(CONTEOS.keys())))
kpis = [
 dict(label='Venta de la semana', valor=f'Q {tot_q:,.0f}', sub=f'{tot_u} unidades en 40 tiendas', delta='-3.3% vs semana 629', tono='neutro'),
 dict(label='Instock del sistema', valor='91%', sub='lo que Walmart CREE tener en piso', delta=None, tono='neutro'),
 dict(label='Unidades fantasma', valor=f'{fantasma_total}', sub=f'verificadas con conteo físico en {sum(1 for d in disc if d["delta"]<0)} combos', delta=None, tono='alerta'),
 dict(label='Reclamos de la semana', valor=f'{rec_u} u', sub=f'≈ Q {rec_q:,.0f} al costo · 79% es Sapporo', delta=None, tono='alerta'),
 dict(label='Cobertura mercaderistas', valor=f'{tiendas_contadas} tiendas', sub='reportes frescos 26-29/08 · 24 acumuladas desde el 14/08', delta=None, tono='ok'),
]

DIAS = ['Lun', 'Mar', 'Mié', 'Jue', 'Vie', 'Sáb', 'Dom']
curva = [dict(dia=d, u=int(v)) for d, v in zip(DIAS, hist['curva'].values())]

DATA = dict(
 meta=dict(titulo='CUADERNO DE LA SEMANA · WALMART / PAIZ',
           subtitulo='Clan Cervecero · semana Walmart 202630 (22–28 de agosto) · scouting completo de la red',
           generado='31 de agosto de 2026',
           fuentes=['Retail Link 202630 (bajado 30/08)', 'Conteos físicos GSP 26–29/08', 'Reporte mercaderistas v2 (14–24/08)', 'HUB v4']),
 kpis=kpis,
 curva_diaria=curva,
 serie_semanal=[dict(sem='628', q=10325.05), dict(sem='629', q=10906.04), dict(sem='630', q=tot_q)],
 referencia_2024=20486,
 items=items,
 tiendas=tiendas,
 heatmap=heat,
 item_ids=[i['id'] for i in items if i['clase'] != 'INACTIVO'],
 discrepancias=disc,
 vn_evolucion=vn_evo,
 reclamos=reclamos,
 mercaderistas=dict(equipo=equipo, reportes=reportes),
 hallazgos=[
  dict(titulo='Vencidos en WM Bosques', gravedad='alta', detalle='Todo el Trooper FOTD de la tienda venció en 5/2026 y sigue ahí; más 12 Sapporo Black, 12 Gold (vencidas desde 31/5/25) y 12 Suffolk. Coordinar retiro/merma — es espacio muerto en góndola.'),
  dict(titulo='Producto empacado tras el modular · WM Roosevelt', gravedad='alta', detalle='68 Dunkel + 108 HB Original + 36 Silver reempacados hace 15 días y el plano nuevo no los contempla. El sistema cuadra al 100% pero 135+ unidades no están a la venta.'),
  dict(titulo='La llegada perdida de Aguilar Batres', gravedad='media', detalle='12 Silver llegaron el 16/08 y no aparecen ni en sistema ni en físico. Rastrear la recepción.'),
  dict(titulo='Reclamos concentrados en Sapporo', gravedad='media', detalle=f'{rec_u} u reclamadas esta semana (≈Q{rec_q:,.0f}); 50 u en Pinula y 24 en Mazatenango. Averiguar la causa del reclamo antes de que se repita.'),
  dict(titulo='Mega 6: 12 de 18 SKU BORRADOS del maestro', gravedad='alta', detalle='Ya no es quiebre: es baja de surtido. Cerrar con Cristian el estatus (pendiente del backlog desde la reunión).'),
  dict(titulo='Sapporo Gold vende sin estar activo', gravedad='info', detalle='Q1,018 esta semana (20 u) estando INACTIVO y en liquidación. Bosques ya lo mete a plano de cámara fría. Candidato a reactivación formal.'),
 ],
 narrativa=dict(capitulos=[
  dict(id='c1', titulo='I · El marcador de la semana', parrafos=[
   'La semana 202630 cerró en Q10,544 (297 unidades) — tercera semana consecutiva en la meseta de los Q10 mil, un 3% abajo de la anterior. La referencia sigue siendo la del 2024: Q20,486 por semana. Vendemos la mitad de lo que este mismo catálogo vendía hace dos años, y ya sabemos que no es la demanda: es cuánto producto está de verdad en el piso.',
   'El partido se juega el fin de semana: viernes, sábado y domingo concentran el 66% de la venta (el sábado solo, el 29%). Todo lo que no esté en góndola un jueves por la noche es venta que no existe.']),
  dict(id='c2', titulo='II · La mentira del 91%', parrafos=[
   'El sistema de Walmart reporta un instock del 91%: casi perfecto. Los conteos físicos cuentan otra historia: 500 unidades fantasma verificadas — producto que el sistema jura tener y que nadie encuentra en piso ni en bodega. Con el estante "lleno" en papel, el motor de reposición no pide, la venta no ocurre, y el instock hermoso se vuelve el mejor disfraz del quiebre.',
   'Villa Nueva es el caso de estudio: cuatro conteos entre el 14 y el 28 de agosto, siempre con el mismo final — el sistema dice 471, el piso dice 54. Ya está escalado al coordinador de abarrotes y el ajuste de inventario va en la lista para Cristian. Sin ese ajuste, esa tienda está fuera de cualquier pedido.']),
  dict(id='c3', titulo='III · Lo que vieron los mercaderistas', parrafos=[
   'La red GSP tocó 14 tiendas solo esta semana (24 acumuladas desde el 12 de agosto, cuando arrancó la campaña). Utatlán salió limpia — físico igual a sistema en los 8 ítems. Bosques apareció con producto vencido en góndola desde mayo. WM Roosevelt cuadra al 100% en papel, pero tiene 200+ unidades empacadas en bodega porque el modular nuevo las dejó fuera del plano.',
   'El patrón que más pega: los 7 no modulados están en cero físico en la mayoría de tiendas y en el sistema figuran "no resurtible" o "bloqueado". Eso conecta directo con el pedido de refuerzo que está esperando firma — la góndola de octubre se gana con esas cajas.']),
  dict(id='c4', titulo='IV · Los reclamos silenciosos', parrafos=[
   'Aparecieron 79 unidades reclamadas esta semana — Q3 mil al costo que se descuentan solos de la factura. El 79% es Sapporo y la mitad salió de una sola tienda (Santa Catarina Pinula). Un reclamo aislado es ruido; 50 unidades en una tienda es una causa que hay que ir a buscar: recepción, manipulación o vencimiento.']),
  dict(id='c5', titulo='V · La jugada que sigue', parrafos=[
   'Tres cosas salen de este cuaderno: el ajuste de inventario de Villa Nueva (y los fantasmas menores de Altos y Paiz Roosevelt) para Cristian; la causa de los reclamos de Pinula; y el pedido de refuerzo de los no modulados que ya está calculado y espera la firma — con la vigencia venciendo el martes 2.',
   'El tablero de los lunes arranca esta semana. Este cuaderno es la versión larga; el de los lunes será el resumen de 5 números que Walmart va a ver.']),
 ]),
)
json.dump(DATA, open('tablero_data.json', 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
print('tablero_data.json ·', len(json.dumps(DATA, ensure_ascii=False)), 'chars')
print('kpis:', [k['valor'] for k in kpis])
print('items:', len(items), '| tiendas:', len(tiendas), '| heat:', len(heat), '| disc:', len(disc), '| reportes:', len(reportes))
print('fantasma_total:', fantasma_total, '| reclamos:', rec_u, 'u Q', rec_q)
