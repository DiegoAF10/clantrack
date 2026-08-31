# -*- coding: utf-8 -*-
"""Capa de datos del tablero semanal: extrae y calcula todo lo que falta."""
import sys, io, json, warnings
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
warnings.filterwarnings('ignore')
import pandas as pd

HUB = r'C:\Users\Diego\projects\clan\coo\walmart\expediente-walmart-2026\datos\HUB_Walmart_Paiz_Clan_Cervecero_4.xlsx'

# ---------- 1. HUB SELL-OUT por item (628/629) ----------
so = pd.read_excel(HUB, sheet_name='4. SELL-OUT', header=None)
hdr_i = next(i for i in range(len(so)) if str(so.iloc[i, 0]).strip() in ('Ítem', 'Item', '�tem'))
items_hist = []
for i in range(hdr_i + 1, hdr_i + 30):
    row = so.iloc[i]
    v = str(row[0]).strip()
    if not v.replace('.0', '').isdigit():
        break
    items_hist.append(dict(item=str(int(float(v))), desc=str(row[1]), clase=str(row[2]),
                           u628=float(row[3] or 0), q628=float(row[4] or 0), u629=float(row[5] or 0), q629=float(row[6] or 0)))
print('items hist 628/629:', len(items_hist))

# ---------- 2. f3: venta por dia ----------
import re
xl = pd.read_excel('f3_113560909.xlsx', header=None)
hdr_row = 23
hdr = [str(h).strip() for h in xl.iloc[hdr_row]]
data = xl.iloc[hdr_row + 1:].copy()
data.columns = hdr
dias_cols = [c for c in hdr if re.match(r'(Lun|Mar|Mi|Jue|Vie|S.b|Dom)', str(c))]
print('columnas de dia:', dias_cols)
num = lambda s: pd.to_numeric(s, errors='coerce').fillna(0)
curva = {c: float(num(data[c]).sum()) for c in dias_cols}
print('curva diaria (u):', curva, '| total:', sum(curva.values()))

# ---------- 3. f1 dedupe: reclamos/devoluciones + instock ----------
f1 = pd.read_csv('t_formato_nuevo_202630.csv', dtype={'item': str, 'tienda_num': str})
print('reclamos u total:', f1['reclamo_u'].sum(), '| devoluciones u total:', f1['devolucion_u'].sum())
dd = f1.groupby(['tienda_num', 'tienda', 'item', 'desc'], as_index=False).agg(
    venta_u=('venta_u', 'sum'), venta_q=('venta_q', 'sum'), existencia=('existencia', 'max'),
    en_pedido=('en_pedido', 'max'), transito=('transito', 'max'))
print('combos deduplicados:', len(dd), '| tiendas:', dd['tienda_num'].nunique(), '| items:', dd['item'].nunique())

# instock sistema por item
inst_item = dd.groupby(['item', 'desc']).apply(
    lambda g: pd.Series(dict(tiendas=len(g), con_stock=int((g['existencia'] > 0).sum()),
                             exist_total=int(g['existencia'].sum()), venta_u=int(g['venta_u'].sum()),
                             venta_q=round(float(g['venta_q'].sum()), 2)))).reset_index()
inst_item['instock_pct'] = (inst_item['con_stock'] / inst_item['tiendas'] * 100).round(0)
print(inst_item.sort_values('venta_q', ascending=False).to_string(index=False))

# instock por tienda
inst_tienda = dd.groupby(['tienda_num', 'tienda']).apply(
    lambda g: pd.Series(dict(combos=len(g), con_stock=int((g['existencia'] > 0).sum()),
                             venta_q=round(float(g['venta_q'].sum()), 2)))).reset_index()
inst_tienda['instock_pct'] = (inst_tienda['con_stock'] / inst_tienda['combos'] * 100).round(0)
print()
print(inst_tienda.sort_values('venta_q', ascending=False).head(12).to_string(index=False))

dd.to_csv('t_combos_dedupe.csv', index=False, encoding='utf-8-sig')
json.dump(dict(items_hist=items_hist, curva=curva), open('t_hist.json', 'w', encoding='utf-8'), ensure_ascii=False)
print('t_combos_dedupe.csv + t_hist.json listos')
