# -*- coding: utf-8 -*-
"""Motor del pedido de refuerzo · Clan Cervecero → Walmart/Paiz
Insumos: RL 202630 (f1 CSV) · HUB (pesos 628/629, modelo) · Odoo vivo · conteos chat 26-29/08.
Salida: datos.json (para el HTML) + auditoria legible."""
import sys, io, json, math, warnings
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
warnings.filterwarnings('ignore')
import pandas as pd

f1 = pd.read_csv('t_formato_nuevo_202630.csv', dtype={'item': str, 'tienda_num': str})
f2 = pd.read_csv('t_sugerido.csv', dtype={'item': str, 'tienda_num': str})

# ---------------- catálogo de los 7 no modulados ----------------
ITEMS = {
    '75437615': dict(nombre='HB Helles Lager lata', corto='HELLES', vigencia='10/11/2026', urgente=False,
                     ucaja=20, costo=16.00, pvp=20.76, bodega=4222, modelo_sug=1708,
                     nota='El mejor sell-through de la red (10.7% sem 629). La apuesta estelar.'),
    '75437613': dict(nombre='NC Old Rasputin lata', corto='RASPUTIN', vigencia='02/09/2026', urgente=True,
                     ucaja=20, costo=43.80, pvp=56.70, bodega=918, modelo_sug=579,
                     nota='Vende Q512/sem con 25 tiendas. Todo condicionado a extensión de vigencia.'),
    '75437612': dict(nombre='NC Scrimshaw lata', corto='SCRIMSHAW', vigencia='02/09/2026', urgente=True,
                     ucaja=20, costo=43.80, pvp=56.70, bodega=1032, modelo_sug=0,
                     nota='SOBRE-STOCK: 508 u en tiendas, 0 venta 202630. No pedir; pedir extensión para poder vender lo que ya está.'),
    '75437617': dict(nombre='HB Session Lager lata', corto='SESSION', vigencia='02/09/2026', urgente=True,
                     ucaja=20, costo=16.00, pvp=20.76, bodega=1492, modelo_sug=40,
                     nota='0 venta 202630, 137 u en tiendas. Recomendación: no pedir.'),
    '75437618': dict(nombre='Belhaven Best lata', corto='BEST', vigencia='02/09/2026', urgente=True,
                     ucaja=20, costo=18.25, pvp=23.66, bodega=1075, modelo_sug=0,
                     nota='Sin tasa: no vendió. Piloto = empujar las 95 u que ya están, sin pedir más.'),
    '75437619': dict(nombre='Belhaven Black lata', corto='BLACK_BEL', vigencia='02/09/2026', urgente=True,
                     ucaja=20, costo=18.25, pvp=23.66, bodega=827, modelo_sug=111,
                     nota='Vende poco pero vende (2u/629, 1u/630). Refuerzo quirúrgico donde probó moverse.'),
    '75437620': dict(nombre='Belhaven McCallums lata', corto='MCCALLUMS', vigencia='02/09/2026', urgente=True,
                     ucaja=20, costo=18.25, pvp=23.66, bodega=2126, modelo_sug=0,
                     nota='Sin tasa: no vendió. 91 u en tiendas. No pedir.'),
}

# ---------------- pesos por tienda: Q semanas 628 + 629 (HUB) + 630 (f1) ----------------
w628629 = {  # tienda_num: (Q628, Q629)  — HUB 4. SELL-OUT POR TIENDA
    '22': (502.44, 1308.44), '457': (466.70, 865.46), '108': (670.72, 717.22), '491': (801.82, 630.86),
    '47': (108.75, 479.11), '4037': (521.35, 444.99), '727': (385.65, 427.05), '46': (53.56, 376.85),
    '4433': (83.69, 339.18), '23': (871.34, 337.86), '121': (174.86, 335.72), '1054': (44.64, 331.98),
    '4414': (385.19, 325.45), '33': (106.42, 319.37), '459': (178.56, 288.88), '180': (218.30, 252.04),
    '34': (276.77, 235.46), '169': (645.58, 230.44), '4087': (381.25, 229.82), '419': (52.45, 218.92),
    '344': (0.0, 218.13), '4176': (77.76, 209.38), '4490': (349.44, 206.42), '4412': (366.24, 202.09),
    '4173': (196.70, 199.20), '4317': (174.65, 163.30), '4370': (66.96, 132.40), '24': (274.29, 131.54),
    '414': (106.25, 128.57), '460': (0.0, 109.73), '4126': (427.86, 90.18), '458': (128.05, 85.98),
    '35': (238.40, 85.62), '21': (119.92, 66.61), '20': (222.95, 66.61), '948': (45.08, 61.61),
    '164': (331.98, 44.64), '4116': (139.64, 8.93), '1087': (0.0, 0.0), '4491': (128.84, 0.0),
}
v630 = f1.groupby('tienda_num').agg(q630=('venta_q', 'sum')).to_dict()['q630']
nombres = f1.drop_duplicates('tienda_num').set_index('tienda_num')['tienda'].to_dict()

tiendas = {}
for tn in nombres:
    a, b = w628629.get(tn, (0.0, 0.0))
    c = float(v630.get(tn, 0.0))
    tiendas[tn] = dict(nombre=nombres[tn], q628=a, q629=b, q630=round(c, 2),
                       prom3=round((a + b + c) / 3, 2))

# top-20 por venta (pendiente 4, semana 202629 — lo comprometido en la mesa)
TOP20 = ['22','457','108','491','47','4037','727','46','4433','23','121','1054','4414','33','459','180','34','169','4087','419']

# ---------------- conteos físicos del chat (26-29/08) · solo no modulados + VN completo ----------------
# (tienda_num, item) -> físico contado. None = "0 unidades" reportado sin sistema.
FISICO = {
    # WM Villa Nueva 414 · reconteo 26/08 19:01 RECONFIRMADO 28/08 17:46
    ('414','75437618'): 0, ('414','75437619'): 0, ('414','75437620'): 0, ('414','75437615'): 0,
    ('414','75437617'): 0, ('414','75437613'): 0, ('414','75437612'): 0,
    # Paiz Altos 4176 (27/08) — fantasmas en no modulados
    ('4176','75437613'): 1, ('4176','75437612'): 8, ('4176','75437617'): 0, ('4176','75437615'): 0,
    # Paiz San Cristóbal 169 (27/08) — consistente
    ('169','75437619'): 8, ('169','75437620'): 20, ('169','75437615'): 0, ('169','75437617'): 13,
    ('169','75437613'): 16, ('169','75437612'): 19,
    # Paiz Roosevelt 24 (27/08) — consistente
    ('24','75437615'): 34, ('24','75437612'): 20, ('24','75437613'): 19,
    # Paiz Aguilar Batres 33 (27/08)
    ('33','75437615'): 20, ('33','75437613'): 17, ('33','75437612'): 19, ('33','75437618'): 0,
    ('33','75437619'): 0, ('33','75437620'): 0, ('33','75437617'): 0,
    # WM Roosevelt 121 (28/08) — no modulados en cero
    ('121','75437618'): 0, ('121','75437619'): 0, ('121','75437620'): 0, ('121','75437615'): 0,
    ('121','75437617'): 0, ('121','75437613'): 0, ('121','75437612'): 0,
    # WM Naranjo 948 (27/08) — "no resurtible" en cero
    ('948','75437618'): 0, ('948','75437619'): 0, ('948','75437620'): 0, ('948','75437615'): 0,
    ('948','75437617'): 0, ('948','75437613'): 0, ('948','75437612'): 0,
    # Paiz Novicentro 23 (29/08)
    ('23','75437618'): 0, ('23','75437619'): 0, ('23','75437620'): 0, ('23','75437615'): 0,
    ('23','75437617'): 0, ('23','75437613'): 0, ('23','75437612'): 0,
    # Paiz Salida al Pacífico 458 ("Pacific Center", 29/08 — mapeo tentativo)
    ('458','75437618'): 0, ('458','75437619'): 0, ('458','75437620'): 0, ('458','75437615'): 0,
    ('458','75437617'): 0, ('458','75437613'): 0, ('458','75437612'): 0,
    # Paiz Utatlán 34 (27/08)
    ('34','75437615'): 8,
}

# ---------------- matriz no modulados: estado por tienda ----------------
# f1 trae VARIAS filas por combo (diarias + foto de existencia): deduplicar primero.
# venta = suma de las filas · existencia/en_pedido = máximo (solo la foto los trae poblados).
nm_raw = f1[f1['item'].isin(ITEMS)].copy()
nm = nm_raw.groupby(['tienda_num', 'item'], as_index=False).agg(
    existencia=('existencia', 'max'), venta_u=('venta_u', 'sum'), en_pedido=('en_pedido', 'max'))

filas = []
for _, r in nm.iterrows():
    tn, it = r['tienda_num'], r['item']
    fis = FISICO.get((tn, it), None)
    sist = int(r['existencia'])
    real = fis if fis is not None else sist
    filas.append(dict(tienda=tn, item=it, sistema=sist, fisico=fis, real=real,
                      venta630=int(r['venta_u']), en_pedido=int(r['en_pedido']),
                      fantasma=(fis is not None and sist - fis >= 5)))
M = pd.DataFrame(filas)
assert not M.duplicated(['tienda', 'item']).any(), 'combos duplicados tras dedupe'

# ---------------- asignación · Plan Recomendado (COO) ----------------
def asigna(item, base_u, top_extra_u, top_n, min_stock_para_saltar, excluir=('414',)):
    """base_u a toda tienda elegible con stock real < min_stock; top_n tiendas por peso reciben base+extra."""
    sub = M[M['item'] == item].copy()
    sub['peso'] = sub['tienda'].map(lambda t: tiendas.get(t, {}).get('prom3', 0))
    sub = sub.sort_values('peso', ascending=False).reset_index(drop=True)
    out = {}
    for i, r in sub.iterrows():
        tn = r['tienda']
        if tn in excluir:
            out[tn] = dict(cajas=0, u=0, motivo='EXCLUIDA — reconciliación de inventario pendiente (fantasma verificado 2×)')
            continue
        target = base_u + (top_extra_u if i < top_n else 0)
        if r['real'] >= min_stock_para_saltar:
            out[tn] = dict(cajas=0, u=0, motivo=f"stock sano: {r['real']} u reales")
            continue
        need = max(0, target - int(r['real']) - int(r['en_pedido']))
        cajas = math.ceil(need / ITEMS[item]['ucaja']) if need > 0 else 0
        out[tn] = dict(cajas=cajas, u=cajas * ITEMS[item]['ucaja'],
                       motivo=('fantasma verificado — entra tras ajuste' if r['fantasma'] else
                               (f"cero demostrado ({'físico' if r['fisico'] is not None else 'sistema'})" if r['real'] == 0
                                else f"stock bajo: {r['real']} u")))
    return out

PLAN = {
    # item: (base_u, extra_top, top_n, salta_si_stock>=)
    '75437615': asigna('75437615', 40, 20, 8, 30),    # HELLES: 2 cajas base, 3 al top-8; salta si ya tiene 30+
    '75437613': asigna('75437613', 20, 20, 5, 12),    # RASPUTIN: 1 caja, 2 al top-5; salta si tiene 12+
    '75437619': asigna('75437619', 20, 0, 0, 8),      # BELHAVEN BLACK: 1 caja donde real < 8
    '75437617': {},                                    # SESSION: 0
    '75437612': {},                                    # SCRIMSHAW: 0
    '75437618': {},                                    # BEST: 0
    '75437620': {},                                    # MCCALLUMS: 0
}

# ---------------- resumen + validación bodega ----------------
resumen = {}
for it, alloc in PLAN.items():
    cajas = sum(a['cajas'] for a in alloc.values())
    u = cajas * ITEMS[it]['ucaja']
    resumen[it] = dict(cajas=cajas, unidades=u, q_costo=round(u * ITEMS[it]['costo'], 2),
                       tiendas_beneficiadas=sum(1 for a in alloc.values() if a['cajas'] > 0),
                       bodega=ITEMS[it]['bodega'], cabe_en_bodega=u <= ITEMS[it]['bodega'])

print('=== PLAN RECOMENDADO (red, sin Villa Nueva) ===')
tot_u = tot_q = tot_c = 0
for it, r in resumen.items():
    i = ITEMS[it]
    print(f"{i['corto']:<10} {r['cajas']:>4} cajas · {r['unidades']:>5} u · Q{r['q_costo']:>10,.2f} · {r['tiendas_beneficiadas']} tiendas · bodega {r['bodega']} {'OK' if r['cabe_en_bodega'] else 'INSUFICIENTE'}")
    tot_u += r['unidades']; tot_q += r['q_costo']; tot_c += r['cajas']
print(f"{'TOTAL':<10} {tot_c:>4} cajas · {tot_u:>5} u · Q{tot_q:>10,.2f}")

print()
print('=== DETALLE POR TIENDA (cajas por ítem) ===')
det = {}
for it, alloc in PLAN.items():
    for tn, a in alloc.items():
        if a['cajas'] > 0:
            det.setdefault(tn, {})[ITEMS[it]['corto']] = a['cajas']
for tn in sorted(det, key=lambda t: -tiendas.get(t, {}).get('prom3', 0)):
    tt = tiendas.get(tn, {})
    print(f"  {tn:<6} {tt.get('nombre','?'):<28} prom3sem Q{tt.get('prom3',0):>7,.0f}  → {det[tn]}  = {sum(det[tn].values())} cajas")

# ---------------- Villa Nueva: anexo de reconciliación ----------------
VN = [  # item, desc, fisico, sistema (reconteo 26/08 confirmado 28/08)
    ('75214930','Sapporo Silver', 0, 61), ('75437621','Trooper Original', 23, 184),
    ('75437610','Sapporo Black', 0, 20), ('75214932','Sapporo Gold', 0, 12),
    ('75437614','HB Original', 7, 26), ('75437616','HB Dunkel', 16, 27),
    ('75437623','Trooper FOTD', 0, 20), ('75437615','HB Helles', 0, 20),
    ('75437617','HB Session', 0, 20), ('75437613','NC Old Rasputin', 0, 13),
    ('75437612','NC Scrimshaw', 0, 20), ('75437618','Belhaven Best', 0, 20),
    ('75437619','Belhaven Black', 0, 20), ('75253328','StPet Suffolk', 8, 8),
]
fantasma_total = sum(s - f for _, _, f, s in VN)
print(f"\n=== VILLA NUEVA: {fantasma_total} unidades fantasma en {sum(1 for _,_,f,s in VN if s>f)} de {len(VN)} ítems ===")

# ---------------- export ----------------
export = dict(
    generado='2026-08-31', semana_datos='202630 (cerrada 28/08, bajada 30/08)',
    items={it: {**ITEMS[it], 'resumen': resumen.get(it, {})} for it in ITEMS},
    tiendas=tiendas, top20=TOP20,
    plan={it: PLAN[it] for it in PLAN},
    matriz=M.to_dict(orient='records'),
    villa_nueva=dict(filas=[dict(item=i, desc=d, fisico=f, sistema=s) for i, d, f, s in VN],
                     fantasma_total=fantasma_total),
    oc_automatica=dict(numero='2401060275', fecha='31/08/2026', cancelacion='04/09/2026',
                       destino='CD 5406 Barcenas Villa Nueva', total_u=576, total_q=16726.84,
                       lineas=[
                           dict(item='75214930', desc='Sapporo Silver', pedido=96, bodega=1450, ok=True),
                           dict(item='75437610', desc='Sapporo Black', pedido=40, bodega=951, ok=True),
                           dict(item='75437614', desc='HB Original', pedido=200, bodega=9, ok=False),
                           dict(item='75437616', desc='HB Dunkel', pedido=20, bodega=2912, ok=True),
                           dict(item='75437621', desc='Trooper Original', pedido=120, bodega=64, ok=False),
                           dict(item='75437623', desc='Trooper FOTD', pedido=100, bodega=5901, ok=True),
                       ]),
    totales=dict(cajas=tot_c, unidades=tot_u, q_costo=round(tot_q, 2)),
)
json.dump(export, open('datos.json', 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
print('\ndatos.json escrito ·', len(json.dumps(export)), 'bytes')
