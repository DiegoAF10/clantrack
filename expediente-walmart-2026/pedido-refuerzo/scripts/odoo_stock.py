# -*- coding: utf-8 -*-
"""Stock vivo en bodega Clan para los 13 items activos de Walmart."""
import sys, io, json, xmlrpc.client
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

cred = json.load(open(r'C:\Users\Diego\.clan-odoo\credentials.json'))
url, db, login, key = cred['url'], cred['db'], cred['login'], cred['api_key']
common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
uid = common.authenticate(db, login, key, {})
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')

CODES = ['CC-SAP-02', 'CC-SAP-03', 'CC-HB-01', 'CC-HB-02', 'CC-TROO-04', 'CC-TROOP-03',
         'CC-NC-15', 'CC-NC-14', 'CC-HB-09', 'CC-HB-10', 'CC-BEL-01', 'CC-BEL-02', 'CC-BEL-03']

# company_id 1 = Clan Cervecero (memoria: sin contexto de compania se leen ceros falsos)
ctx = {'context': {'allowed_company_ids': [1], 'company_id': 1}}
prods = models.execute_kw(db, uid, key, 'product.product', 'search_read',
    [[['default_code', 'in', CODES]]],
    {'fields': ['default_code', 'name', 'qty_available', 'free_qty', 'virtual_available', 'outgoing_qty', 'incoming_qty'], **ctx})
print(f"{'codigo':<14}{'qty':>8}{'libre':>8}{'saliente':>9}{'entrante':>9}  nombre")
for p in sorted(prods, key=lambda x: x['default_code'] or ''):
    print(f"{p['default_code']:<14}{p['qty_available']:>8.0f}{p['free_qty']:>8.0f}{p['outgoing_qty']:>9.0f}{p['incoming_qty']:>9.0f}  {p['name'][:44]}")
