# -*- coding: utf-8 -*-
"""
Ensambla los PDF finales de registros sanitarios (multipágina) desde los HEIC originales.

Uso:  python ensamblar_pdfs.py documentos.json
donde documentos.json = [
  {"nombre": "RS_A-104901_GUINNESS-DRAUGHT_2028-05-31",
   "paginas": ["20260827_200056434_iOS.heic", "20260827_200106347_iOS.heic"]},
  ...
]
Los originales no se tocan. Salida: PDFs en esta carpeta.
"""
import sys, os, json, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from PIL import Image, ImageOps, ImageEnhance
import pillow_heif
pillow_heif.register_heif_opener()

RAW = r"C:\Users\Diego\OneDrive - Clan Cervecero\CLAN CERVECERO\REGISTROS SANITARIOS\RAW"
BASE = os.path.dirname(os.path.abspath(__file__))

def escanear(ruta):
    img = Image.open(ruta)
    img = ImageOps.exif_transpose(img)
    img = img.convert("L")
    img = ImageOps.autocontrast(img, cutoff=2)
    img = ImageEnhance.Sharpness(img).enhance(1.35)
    if max(img.size) > 2600:
        img.thumbnail((2600, 2600), Image.LANCZOS)
    return img.convert("RGB")

docs = json.load(open(sys.argv[1], encoding="utf-8"))
for d in docs:
    paginas = [escanear(os.path.join(RAW, p)) for p in d["paginas"]]
    salida = os.path.join(BASE, d["nombre"] + ".pdf")
    paginas[0].save(salida, "PDF", resolution=200.0, save_all=True, append_images=paginas[1:])
    print(f"OK  {d['nombre']}.pdf ({len(paginas)} pág)")
print(f"Total: {len(docs)} documentos")
