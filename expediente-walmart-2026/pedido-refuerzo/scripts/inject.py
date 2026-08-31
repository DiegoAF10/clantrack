# -*- coding: utf-8 -*-
"""Inyecta datos_final.json en el HTML de Kimi entre /*__DATOS__*/ y /*__FIN_DATOS__*/."""
import sys, io, json, re
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

html = open('kimi_out.html', encoding='utf-8').read()
data = json.load(open('datos_final.json', encoding='utf-8'))
payload = json.dumps(data, ensure_ascii=False)

pat = re.compile(r'/\*__DATOS__\*/.*?/\*__FIN_DATOS__\*/', re.S)
if not pat.search(html):
    print('ERROR: no encontre los marcadores __DATOS__ en el HTML de Kimi')
    sys.exit(1)
out = pat.sub(lambda m: '/*__DATOS__*/' + payload + '/*__FIN_DATOS__*/', html, count=1)
open('PEDIDO-REFUERZO.html', 'w', encoding='utf-8').write(out)
print('inyectado ·', len(out), 'bytes · marcadores OK')
# sanity: el HTML no debe contener numeros de negocio fuera de DATA (chequeo rapido de fakes)
for fake in ['TIENDA DEMO', 'ITEM DEMO']:
    if fake in out:
        print(f'AVISO: quedo texto de muestra "{fake}" fuera del bloque DATA (revisar render)')
