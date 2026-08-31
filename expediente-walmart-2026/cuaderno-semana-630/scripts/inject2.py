# -*- coding: utf-8 -*-
"""Inyecta tablero_data.json en kimi_out2.html -> CUADERNO-SEMANA-630.html"""
import sys, io, json, re
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

html = open('kimi_out2.html', encoding='utf-8').read()
data = json.load(open('tablero_data.json', encoding='utf-8'))
payload = json.dumps(data, ensure_ascii=False)

pat = re.compile(r'/\*__DATOS__\*/.*?/\*__FIN_DATOS__\*/', re.S)
if not pat.search(html):
    print('ERROR: sin marcadores __DATOS__')
    sys.exit(1)
# La asignacion window.DATA se emite SIEMPRE, sin importar si el marcador original la incluia
bloque = '/*__DATOS__*/\nwindow.DATA = ' + payload + ';\n/*__FIN_DATOS__*/'
out = pat.sub(lambda m: bloque, html, count=1)
if 'window.DATA' not in out:
    print('ERROR: quedo sin asignacion window.DATA')
    sys.exit(1)
open('CUADERNO-SEMANA-630.html', 'w', encoding='utf-8').write(out)
print('inyectado ·', len(out), 'bytes')
for fake in ['TIENDA DEMO', 'ITEM DEMO']:
    if fake in out:
        print(f'AVISO: texto de muestra "{fake}" fuera del bloque DATA')
