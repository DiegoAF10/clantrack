#!/usr/bin/env python3
"""
BACKROOM / TRASTIENDA ANALYSIS - ClanTrack
Hypothesis: High inventory + Low/Zero sales = Product NOT on shelf
"""
import sqlite3
import sys
import io
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

conn = sqlite3.connect('C:/Users/Diego/projects/clan/coo/walmart/walmart.db')
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print("=" * 100)
print("ANALISIS DE TRASTIENDA - ClanTrack")
print("Hipotesis: Alto inventario + Baja/Cero venta = Producto NO esta en piso")
print("=" * 100)

# Step 1: Get latest inventory (week 202605) - MAX(inv_actual) per product/store
cur.execute("""
    SELECT producto, store_nbr, MAX(inv_actual) as inv
    FROM retail_link
    WHERE tipo_registro = 'INVENTARIO'
      AND semana_wm = '202605'
      AND inv_actual > 0
    GROUP BY producto, store_nbr
""")
inventory = {}
for r in cur.fetchall():
    inventory[(r['producto'], r['store_nbr'])] = r['inv']

print(f"\nProduct/store combos with inventory in week 202605: {len(inventory)}")

# Step 2: Get sales last 4 weeks (202601-202604)
cur.execute("""
    SELECT producto, store_nbr, SUM(venta_und) as sales_4w
    FROM retail_link
    WHERE tipo_registro = 'VENTA'
      AND semana_wm IN ('202601', '202602', '202603', '202604')
    GROUP BY producto, store_nbr
""")
sales_4w = {}
for r in cur.fetchall():
    sales_4w[(r['producto'], r['store_nbr'])] = r['sales_4w'] or 0

# Step 2b: Sales by period for trend analysis
cur.execute("""
    SELECT producto, store_nbr,
           SUM(CASE WHEN semana_wm IN ('202601','202602') THEN venta_und ELSE 0 END) as sales_early,
           SUM(CASE WHEN semana_wm IN ('202603','202604') THEN venta_und ELSE 0 END) as sales_late
    FROM retail_link
    WHERE tipo_registro = 'VENTA'
      AND semana_wm IN ('202601', '202602', '202603', '202604')
    GROUP BY producto, store_nbr
""")
sales_periods = {}
for r in cur.fetchall():
    sales_periods[(r['producto'], r['store_nbr'])] = (r['sales_early'] or 0, r['sales_late'] or 0)

# Step 3: Network average sales per product (only stores that sell > 0)
cur.execute("""
    SELECT producto,
           AVG(total_sales) as avg_sales,
           COUNT(*) as stores_selling,
           SUM(total_sales) as network_sales
    FROM (
        SELECT producto, store_nbr, SUM(venta_und) as total_sales
        FROM retail_link
        WHERE tipo_registro = 'VENTA'
          AND semana_wm IN ('202601', '202602', '202603', '202604')
        GROUP BY producto, store_nbr
        HAVING SUM(venta_und) > 0
    )
    GROUP BY producto
""")
network_avg = {}
for r in cur.fetchall():
    network_avg[r['producto']] = {
        'avg_sales_4w': r['avg_sales'],
        'stores_selling': r['stores_selling'],
        'network_sales': r['network_sales'],
        'avg_daily': r['avg_sales'] / 28.0
    }

# Step 4: Get product costs and store names
cur.execute("SELECT producto, ingreso_und_wm, costo_clan_und, estado FROM productos")
products = {}
for r in cur.fetchall():
    products[r['producto']] = {
        'cost_wm': r['ingreso_und_wm'] or 0,
        'cost_clan': r['costo_clan_und'] or 0,
        'estado': r['estado']
    }

cur.execute("SELECT no_tienda, nombre, formato, region FROM tiendas")
stores = {}
for r in cur.fetchall():
    stores[r['no_tienda']] = {
        'nombre': r['nombre'],
        'formato': r['formato'],
        'region': r['region']
    }

# Step 5: Build suspects list
suspects = []
for (prod, store), inv in inventory.items():
    if inv < 10:
        continue

    s4w = sales_4w.get((prod, store), 0)
    daily_sales = s4w / 28.0
    coverage = inv / daily_sales if daily_sales > 0 else 9999

    if coverage <= 60:
        continue

    # Check network average
    net = network_avg.get(prod)
    if not net or net['avg_daily'] <= 0.1:
        continue  # Product doesn't sell well anywhere

    # This store's sales vs network average
    store_ratio = s4w / net['avg_sales_4w'] if net['avg_sales_4w'] > 0 else 0
    if store_ratio >= 0.5:
        continue  # Store sells at >= 50% of network avg, probably fine

    # It's a suspect!
    early, late = sales_periods.get((prod, store), (0, 0))
    cost_wm = products.get(prod, {}).get('cost_wm', 0)
    cost_clan = products.get(prod, {}).get('cost_clan', 0)
    waste_wm = inv * cost_wm
    waste_clan = inv * cost_clan

    store_info = stores.get(store, {'nombre': f'Store {store}', 'formato': '?', 'region': '?'})

    # Trend: did sales drop?
    if early > 0 and late == 0:
        trend = "DROPPED"
    elif early > late and early > 0:
        trend = "DECLINING"
    elif early == 0 and late == 0:
        trend = "ZERO"
    else:
        trend = "STABLE_LOW"

    suspects.append({
        'producto': prod,
        'store_nbr': store,
        'store_name': store_info['nombre'],
        'formato': store_info['formato'],
        'region': store_info['region'],
        'inv': inv,
        'sales_4w': s4w,
        'daily_sales': daily_sales,
        'coverage': coverage,
        'net_avg_4w': net['avg_sales_4w'],
        'net_avg_daily': net['avg_daily'],
        'stores_selling': net['stores_selling'],
        'store_ratio': store_ratio,
        'sales_early': early,
        'sales_late': late,
        'trend': trend,
        'cost_wm': cost_wm,
        'cost_clan': cost_clan,
        'waste_wm': waste_wm,
        'waste_clan': waste_clan,
    })

# Sort by waste (WM cost * inventory)
suspects.sort(key=lambda x: -x['waste_wm'])

# ============================================================
# OUTPUT: FULL LIST
# ============================================================
print(f"\n{'=' * 100}")
print(f"BACKROOM SUSPECTS: {len(suspects)} product/store combos flagged")
print(f"{'=' * 100}")

print(f"\n{'PRODUCTO':<32} {'TIENDA':<28} {'INV':>5} {'V.4w':>5} {'COB':>6} {'NET.AVG':>7} {'RATIO':>6} {'TREND':<10} {'WASTE Q':>9}")
print("-" * 115)

for s in suspects:
    cov_str = f"{s['coverage']:.0f}d" if s['coverage'] < 9999 else "INF"
    ratio_str = f"{s['store_ratio']:.0%}"
    print(f"{s['producto']:<32} {s['store_name']:<28} {s['inv']:>5} {s['sales_4w']:>5} {cov_str:>6} {s['net_avg_4w']:>7.1f} {ratio_str:>6} {s['trend']:<10} Q{s['waste_wm']:>8,.0f}")

# ============================================================
# GROUP BY STORE
# ============================================================
print(f"\n\n{'=' * 100}")
print("AGRUPADO POR TIENDA (prioridad de ruta)")
print(f"{'=' * 100}")

by_store = defaultdict(list)
for s in suspects:
    by_store[s['store_name']].append(s)

store_summary = []
for store, items in by_store.items():
    total_waste = sum(i['waste_wm'] for i in items)
    total_units = sum(i['inv'] for i in items)
    store_summary.append((store, items[0]['store_nbr'], items[0]['formato'], items[0]['region'], len(items), total_units, total_waste))

store_summary.sort(key=lambda x: -x[6])

print(f"\n{'TIENDA':<30} {'#':>5} {'FMT':<8} {'REGION':<12} {'PRODS':>5} {'UNITS':>6} {'WASTE Q':>10}")
print("-" * 85)
for name, nbr, fmt, region, prods, units, waste in store_summary:
    print(f"{name:<30} {nbr:>5} {fmt:<8} {region:<12} {prods:>5} {units:>6} Q{waste:>9,.0f}")

# Detail per store
for name, nbr, fmt, region, prods, units, waste in store_summary:
    print(f"\n  >> {name} (#{nbr}) - {prods} productos, {units} und, Q{waste:,.0f} en riesgo")
    store_items = sorted(by_store[name], key=lambda x: -x['waste_wm'])
    for i in store_items:
        cov_str = f"{i['coverage']:.0f}d" if i['coverage'] < 9999 else "INF"
        print(f"     {i['producto']:<32} inv={i['inv']:>4}  v4w={i['sales_4w']:>3}  cob={cov_str:>5}  net={i['net_avg_4w']:.1f}  trend={i['trend']}")

# ============================================================
# GROUP BY PRODUCT
# ============================================================
print(f"\n\n{'=' * 100}")
print("AGRUPADO POR PRODUCTO (problemas sistematicos)")
print(f"{'=' * 100}")

by_product = defaultdict(list)
for s in suspects:
    by_product[s['producto']].append(s)

prod_summary = []
for prod, items in by_product.items():
    total_waste = sum(i['waste_wm'] for i in items)
    total_units = sum(i['inv'] for i in items)
    total_stores_inv = sum(1 for (p, st), inv in inventory.items() if p == prod)
    pct_flagged = len(items) / total_stores_inv * 100 if total_stores_inv > 0 else 0
    prod_summary.append((prod, len(items), total_stores_inv, pct_flagged, total_units, total_waste))

prod_summary.sort(key=lambda x: -x[5])

print(f"\n{'PRODUCTO':<35} {'FLAG':>4} {'TOTAL':>5} {'%FLAG':>6} {'UNITS':>6} {'WASTE Q':>10}")
print("-" * 75)
for prod, flagged, total, pct, units, waste in prod_summary:
    print(f"{prod:<35} {flagged:>4} {total:>5} {pct:>5.0f}% {units:>6} Q{waste:>9,.0f}")
    items = sorted(by_product[prod], key=lambda x: -x['inv'])
    for i in items:
        print(f"  -> {i['store_name']:<28} inv={i['inv']:>4}  v4w={i['sales_4w']:>3}  trend={i['trend']}")

# ============================================================
# SUMMARY STATS
# ============================================================
print(f"\n\n{'=' * 100}")
print("RESUMEN EJECUTIVO")
print(f"{'=' * 100}")

total_units = sum(s['inv'] for s in suspects)
total_waste_wm = sum(s['waste_wm'] for s in suspects)
total_waste_clan = sum(s['waste_clan'] for s in suspects)
zero_sales = sum(1 for s in suspects if s['sales_4w'] == 0)
dropped = sum(1 for s in suspects if s['trend'] == 'DROPPED')

print(f"\n  Total combos sospechosos:     {len(suspects)}")
print(f"  Total unidades atrapadas:     {total_units:,}")
print(f"  Valor a precio WM (ingreso):  Q{total_waste_wm:,.0f}")
print(f"  Valor a costo Clan:           Q{total_waste_clan:,.0f}")
print(f"  Tiendas afectadas:            {len(by_store)}")
print(f"  Productos afectados:          {len(by_product)}")
print(f"  Con CERO ventas en 4 semanas: {zero_sales} ({zero_sales/len(suspects)*100:.0f}% del total)")
print(f"  Ventas cayeron a cero:        {dropped} (vendian antes, ya no)")

# TOP 10 worst individual combos
print(f"\n\n  TOP 10 PEORES COMBOS (mayor capital atrapado):")
print(f"  {'#':>2} {'PRODUCTO':<32} {'TIENDA':<28} {'INV':>5} {'WASTE Q':>9}")
print(f"  " + "-" * 80)
for i, s in enumerate(suspects[:10], 1):
    print(f"  {i:>2} {s['producto']:<32} {s['store_name']:<28} {s['inv']:>5} Q{s['waste_wm']:>8,.0f}")

# Quick action items
print(f"\n\n  ACCIONES INMEDIATAS:")
print(f"  1. Las {min(5, len(store_summary))} tiendas con mas producto atrapado deben visitarse esta semana")
for i, (name, nbr, fmt, region, prods, units, waste) in enumerate(store_summary[:5], 1):
    print(f"     {i}. {name} - {prods} productos, Q{waste:,.0f}")
print(f"  2. Verificar fisicamente: producto en trastienda vs en gondola")
print(f"  3. Si esta en trastienda, mover a piso y negociar ubicacion con jefe de area")
print(f"  4. Para los {dropped} casos donde las ventas CAYERON, investigar si perdieron facings")

conn.close()
print("\n\nAnalisis completado.")
