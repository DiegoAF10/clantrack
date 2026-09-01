# -*- coding: utf-8 -*-
"""
Escaneo masivo de registros sanitarios — fotos de celular a PDF tipo escaneo.

Uso (lo corre Claude, no Diego):
  python escanear.py                  -> convierte todo lo de _entrada/ a PDF (nombre provisional)
  python escanear.py mapeo.json      -> convierte y renombra según el mapeo
                                         {"IMG_1234.jpg": "RS_A-104901_GUINNESS_2027-11-30", ...}

Mejora aplicada: rotación EXIF + escala de grises + autocontraste suave (2%) + nitidez leve.
El original NUNCA se toca; los PDF salen a esta carpeta.
"""
import sys, os, json
from PIL import Image, ImageOps, ImageEnhance

BASE = os.path.dirname(os.path.abspath(__file__))
ENTRADA = os.path.join(BASE, "_entrada")
EXT = (".jpg", ".jpeg", ".png", ".webp", ".heic")

mapeo = {}
if len(sys.argv) > 1:
    mapeo = json.load(open(sys.argv[1], encoding="utf-8"))

fotos = [f for f in sorted(os.listdir(ENTRADA)) if f.lower().endswith(EXT)]
if not fotos:
    print("No hay fotos en _entrada/")
    sys.exit(0)

for f in fotos:
    ruta = os.path.join(ENTRADA, f)
    try:
        img = Image.open(ruta)
        img = ImageOps.exif_transpose(img)          # respetar rotación del celular
        img = img.convert("L")                       # escala de grises (look escaneo)
        img = ImageOps.autocontrast(img, cutoff=2)   # contraste suave, sin quemar sellos
        img = ImageEnhance.Sharpness(img).enhance(1.4)
        if max(img.size) > 2600:                     # tamaño razonable de escaneo
            img.thumbnail((2600, 2600), Image.LANCZOS)
        nombre = mapeo.get(f, os.path.splitext(f)[0])
        salida = os.path.join(BASE, nombre + ".pdf")
        img.convert("RGB").save(salida, "PDF", resolution=200.0)
        print(f"OK  {f} -> {os.path.basename(salida)}")
    except Exception as e:
        print(f"ERROR {f}: {e}")
