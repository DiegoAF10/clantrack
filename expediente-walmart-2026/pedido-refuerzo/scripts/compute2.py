# -*- coding: utf-8 -*-
"""Genera datos_final.json en el esquema del contrato con Kimi."""
import sys, io, json, math, warnings
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
warnings.filterwarnings('ignore')
import pandas as pd

base = json.load(open('datos.json', encoding='utf-8'))
tiendas_d = base['tiendas']
M = pd.DataFrame(base['matriz'])

ITEMS_META = {
    '75437615': dict(corto='HELLES', v629=35, v630=18),
    '75437613': dict(corto='RASPUTIN', v629=7, v630=9),
    '75437612': dict(corto='SCRIMSHAW', v629=5, v630=0),
    '75437617': dict(corto='SESSION', v629=1, v630=0),
    '75437618': dict(corto='BEST', v629=3, v630=0),
    '75437619': dict(corto='BLACK_BEL', v629=2, v630=1),
    '75437620': dict(corto='MCCALLUMS', v629=1, v630=1),
}
BODEGA_LIBRE = {'75437615': 4222, '75437613': 918, '75437612': 1032, '75437617': 1492,
                '75437618': 1075, '75437619': 827, '75437620': 2126}

exist_red = M.groupby('item')['real'].sum().to_dict()  # con overrides físicos aplicados

# ---------- plan recomendado (del compute 1, aplanado) ----------
plan_reco = []
for it, alloc in base['plan'].items():
    for tn, a in alloc.items():
        if a['cajas'] > 0:
            plan_reco.append(dict(tienda=tn, item=it, cajas=a['cajas'], motivo=a['motivo']))

# ---------- plan modelo: distribuir modelo_sug completo por peso ----------
def plan_modelo_item(item, total_u, excluir=('414',)):
    ucaja = base['items'][item]['ucaja']
    total_cajas = round(total_u / ucaja)
    sub = M[M['item'] == item].copy()
    sub = sub[~sub['tienda'].isin(excluir)]
    sub['peso'] = sub['tienda'].map(lambda t: tiendas_d.get(t, {}).get('prom3', 0))
    # stock real resta atractivo: peso efectivo = peso de venta / (1 + real/ucaja)
    sub['peso_ef'] = sub['peso'] / (1 + sub['real'] / ucaja)
    tot = sub['peso_ef'].sum()
    if tot <= 0 or total_cajas <= 0:
        return []
    sub['exacto'] = sub['peso_ef'] / tot * total_cajas
    sub['cajas'] = sub['exacto'].astype(int)
    resto = total_cajas - sub['cajas'].sum()
    sub = sub.sort_values('exacto', key=lambda s: s - s.astype(int), ascending=False).reset_index(drop=True)
    for i in range(int(resto)):
        sub.loc[i % len(sub), 'cajas'] += 1
    out = []
    for _, r in sub.iterrows():
        if r['cajas'] > 0:
            mot = f"proporcional al peso de venta (Q{tiendas_d.get(r['tienda'],{}).get('prom3',0):,.0f}/sem) · stock real {int(r['real'])} u"
            out.append(dict(tienda=r['tienda'], item=item, cajas=int(r['cajas']), motivo=mot))
    return out

plan_modelo = []
for it, total in [('75437615', 1708), ('75437613', 579), ('75437619', 111), ('75437617', 40)]:
    plan_modelo += plan_modelo_item(it, total)

def totales(plan):
    u = q = c = 0
    for p in plan:
        meta = base['items'][p['item']]
        u += p['cajas'] * meta['ucaja']
        q += p['cajas'] * meta['ucaja'] * meta['costo']
        c += p['cajas']
    return c, u, round(q, 2)

c_r, u_r, q_r = totales(plan_reco)
c_m, u_m, q_m = totales(plan_modelo)
print(f'reco: {c_r} cajas {u_r} u Q{q_r:,.2f} | modelo: {c_m} cajas {u_m} u Q{q_m:,.2f}')
for it in ['75437615', '75437613', '75437619', '75437617']:
    um = sum(p['cajas'] for p in plan_modelo if p['item'] == it) * base['items'][it]['ucaja']
    print(' modelo', ITEMS_META[it]['corto'], um, 'u vs bodega', BODEGA_LIBRE[it], 'OK' if um <= BODEGA_LIBRE[it] else 'EXCEDE')

DATA = dict(
    meta=dict(
        titulo='PEDIDO DE REFUERZO · RED WALMART / PAIZ',
        subtitulo='Clan Cervecero · manifiesto de decisión · tienda por tienda, en cajas',
        generado='31 de agosto de 2026', semana_datos='Semana Walmart 202630 (cerró 28/08 · Retail Link bajado 30/08)',
        deadline='02/09/2026', deadline_dias=2),
    kpis=[
        dict(label='Pedido recomendado', valor=f'{c_r} cajas', sub=f'{u_r:,} unidades a {len(set(p["tienda"] for p in plan_reco))} tiendas', tono='ok'),
        dict(label='Valor del pedido (costo)', valor=f'Q {q_r:,.0f}', sub='lo que factura Clan si Walmart lo corta entero', tono='neutro'),
        dict(label='Vigencia de 6 ítems', valor='VENCE 02/09', sub='gestión ID 90884 con Max · sin extensión no se pide', tono='alerta'),
        dict(label='Fill rate OC de hoy', valor='57%', sub='si se despacha tal cual: faltan 191 HB Original + 56 Trooper', tono='alerta'),
        dict(label='Villa Nueva', valor='417 u fantasma', sub='excluida del pedido · reconteo verificado 2 veces', tono='alerta'),
    ],
    oc=dict(numero='2401060275', fecha='31/08/2026', cancelacion='04/09/2026',
            destino='Bodega 5406 · Km 17 ruta al Pacífico, Bárcenas Villa Nueva (entrega centralizada; el CD regular del maestro es el 7406)',
            total_u=576, total_q=16726.84,
            lineas=[dict(desc=l['desc'], pedido=l['pedido'], bodega=l['bodega'], ok=l['ok'])
                    for l in base['oc_automatica']['lineas']],
            nota='El sistema de reposición de Walmart solo conoce a los 6 modulados: esta OC es todo lo que la red va a pedir sola. Los 7 no modulados dependen del pedido manual de este manifiesto. Alerta: 2 líneas exceden la bodega de Clan — decidir hoy si se despacha parcial avisando a Cristian, o se pide ajuste de la OC (cancela 04/09).'),
    items=[dict(id=it,
                nombre=base['items'][it]['nombre'], corto=ITEMS_META[it]['corto'],
                vigencia=base['items'][it]['vigencia'], urgente=base['items'][it]['urgente'],
                ucaja=base['items'][it]['ucaja'], costo=base['items'][it]['costo'], pvp=base['items'][it]['pvp'],
                bodega=BODEGA_LIBRE[it], modelo_sug=base['items'][it]['modelo_sug'],
                venta629_u=ITEMS_META[it]['v629'], venta630_u=ITEMS_META[it]['v630'],
                exist_red=int(exist_red.get(it, 0)), nota=base['items'][it]['nota'])
           for it in ITEMS_META],
    tiendas=[dict(num=tn, nombre=t['nombre'], prom3=t['prom3'], q628=t['q628'], q629=t['q629'],
                  q630=t['q630'], top20=(tn in base['top20']))
             for tn, t in tiendas_d.items()],
    planes=dict(recomendado=plan_reco, modelo=plan_modelo),
    villa_nueva=dict(
        titulo='WM VILLA NUEVA (tienda 414) — anexo de incidente',
        nota='Reconteo físico del 26/08 19:01 reconfirmado idéntico el 28/08 17:46 (mercaderista GSP; bodega saturada; escalado al coordinador de abarrotes). El sistema cree tener 417 unidades que no existen en piso ni se ubican en bodega. Con instock "sano", el motor de Walmart jamás va a resurtir esta tienda. La tienda NO entra al pedido hasta que Walmart aplique el ajuste de inventario; aplicado el ajuste, entra con el estándar del plan (su peso de venta la pone en media tabla).',
        fantasma_total=417,
        filas=[dict(desc=f['desc'], fisico=f['fisico'], sistema=f['sistema']) for f in base['villa_nueva']['filas']]),
    ajustes=[
        dict(tienda='WM Villa Nueva (414)', detalle='Ajuste de inventario: 417 u fantasma en 13 de 14 ítems (Silver 61→0 · Trooper 184→23 · detalle en el anexo). Sin ajuste no hay resurtido posible.'),
        dict(tienda='Paiz Altos (4176)', detalle='Fantasmas en no modulados: Old Rasputin sistema 17 vs físico 1 · Scrimshaw 17 vs 8 · Session 3 vs 0 (conteo 27/08).'),
        dict(tienda='Paiz Roosevelt (24)', detalle='Fantasmas en modulados: Sapporo Black sistema 20 vs físico 0 · Trooper FOTD sistema 27 vs físico 3 (conteo 27/08).'),
        dict(tienda='WM Bosques (459)', detalle='Producto vencido detectado en conteo 27/08: todo el FOTD (vencía 5/2026), 12 u Sapporo Black, 12 u Sapporo Gold, 12 u StPet Suffolk. Coordinar retiro/merma para liberar espacio.'),
        dict(tienda='WM Roosevelt (121)', detalle='Producto empacado sin ubicar en piso tras el modular de hace 15 días: 68 Dunkel + 108 HB Original + 36 Silver "reempacados". El plano nuevo no los contempla — pedir reubicación.'),
        dict(tienda='Paiz Aguilar Batres (33)', detalle='Sapporo Silver: llegada de 12 u el 16/08 desaparecida (sistema 0, físico 0). Rastrear recepción.'),
    ],
    narrativa=dict(capitulos=[
        dict(id='c1', titulo='I · La semana que cerró', parrafos=[
            'La semana 202630 vendió Q10,544 (297 unidades) en 40 tiendas. Es la meseta conocida: la red vende la mitad de lo que vendía en 2024, y ya demostramos que el problema es ejecución, no demanda — donde los mercaderistas tocaron el piso, la venta respondió (Silver +133% en la 629).',
            'Sapporo Silver sigue siendo la locomotora (Q2,341), pero el dato estratégico está más abajo: HB Helles, sin espacio propio y con la mitad de sus tiendas en cero, mantiene el mejor sell-through de todo el catálogo. La demanda existe; lo que falta es producto en el piso.']),
        dict(id='c2', titulo='II · El robot pide solo lo que conoce', parrafos=[
            'Hoy 31/08 el sistema de Walmart cortó su orden automática: 576 unidades, Q16,727 — exclusivamente de los 6 ítems modulados. Los otros 7 ítems del catálogo son invisibles para ese motor: en tienda aparecen como "no resurtible" o "bloqueado para pedido" (lo confirmaron los conteos del 27-29/08 en Naranjo, Bosques y WM Roosevelt).',
            'Ese es el porqué de este manifiesto: lo que no pidamos nosotros por la vía manual, no lo pide nadie. Y hay un incendio dentro de la OC automática: pide 200 HB Original y 120 Trooper cuando la bodega tiene 9 y 64. Despachar sin avisar es reventar el fill rate la misma semana en que estrenamos el tablero de los lunes.']),
        dict(id='c3', titulo='III · La apuesta — Helles y el refuerzo quirúrgico', parrafos=[
            'El pedido recomendado pone 2 cajas de Helles en cada tienda elegible (3 en las 8 más fuertes): 51 cajas, 1,020 unidades. Es la versión prudente del sugerido del modelo (1,708 u): la semana fresca vendió 18 u porque solo ~9 tiendas tenían producto; con 25 tiendas abastecidas y la vigencia corriendo hasta el 10/11, hay espacio para una segunda ola cuando el corte del 05/09 mida el uplift.',
            'Old Rasputin lleva 14 cajas condicionadas: es el premium que más factura por unidad (PVP Q56.70), pero su vigencia muere el 02/09. Si la gestión con Max no cierra antes del martes, esas 14 cajas no salen — y ahí está el 42% del valor del pedido. Belhaven Black lleva 2 cajas quirúrgicas donde probó moverse. Session, Scrimshaw, Best y McCallums: cero — ya tienen más inventario en tiendas del que rotan, y pedir ahí es financiar inventario muerto.']),
        dict(id='c4', titulo='IV · El fantasma de Villa Nueva', parrafos=[
            'Villa Nueva es la novela de terror del expediente: el sistema jura tener 417 unidades que nadie encuentra — 184 Trooper donde hay 23, 61 Silver donde hay 0. Se contó dos veces (26 y 28/08) con el mismo resultado, y quedó escalado al coordinador de abarrotes.',
            'Mientras ese inventario fantasma exista, para Walmart la tienda está "sana" y no pide nada. Por eso queda fuera de este pedido: primero el ajuste de inventario (está en la lista para Cristian), después el refuerzo. Meterle producto hoy sería esconder el problema debajo de más cajas.']),
        dict(id='c5', titulo='V · El reloj', parrafos=[
            'Tres relojes corren: la vigencia de 6 ítems muere el martes 02/09 (gestión ID 90884 con Max — sin eso, ni pedido ni venta de lo que ya está en tiendas); la OC automática cancela el 04/09 (decidir hoy el despacho parcial y avisar); y el corte de uplift es el 05/09, que define la segunda ola de Helles.',
            'La jugada de la semana: enviar este pedido con el correo del lunes, pedir la extensión de vigencia como condición explícita, y usar el mismo mensaje para avisar el parcial de la OC. Un solo correo, tres problemas resueltos.']),
    ]),
)
json.dump(DATA, open('datos_final.json', 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
print('datos_final.json listo ·', len(json.dumps(DATA, ensure_ascii=False)), 'chars')
