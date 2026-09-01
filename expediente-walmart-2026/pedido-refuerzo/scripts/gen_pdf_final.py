# -*- coding: utf-8 -*-
import json, re, io, sys, csv
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
d = json.load(open("plan_final_pdf.json", encoding="utf-8"))
final = json.load(open("datos_final.json", encoding="utf-8"))
tiendas40 = {t["num"]: t for t in final["tiendas"]}
NOMBRE = {"75437615":"HB Helles Lager","75437613":"NC Old Rasputin","75437612":"NC Scrimshaw",
          "75437617":"HB Session Lager","75437618":"Belhaven Best","75437619":"Belhaven Black","75437620":"Belhaven McCallums"}
HOY = {"75437615":"18","75437613":"9","75437612":"0","75437617":"0","75437618":"0","75437619":"1","75437620":"1"}
res, lanz, tot = d["res"], d["lanz"], d["tot"]

filas_item = ""
for it, nom in NOMBRE.items():
    r = res.get(it, dict(cajas=0,tiendas=0,dos=0))
    filas_item += ('<tr><td><b>%s</b> <span class="it">(%s)</span></td><td class="r">%.1f u/sem</td>'
                   '<td class="r">%s u</td><td class="r"><b>%d</b></td><td class="r">%d</td><td class="r">%d</td></tr>'
                   % (nom, it, lanz[it], HOY[it], r["cajas"], r["tiendas"], r["dos"]))
mejores = ""
for it, nom in NOMBRE.items():
    ls = [l for l in d["lineas"] if l["it"]==it and l["cajas"]==2]
    if ls:
        tt = " · ".join(tiendas40[l["tn"]]["nombre"].title() for l in ls)
        mejores += "<tr><td><b>%s</b></td><td>%s</td></tr>" % (nom, tt)
sincombo = sum(1 for l in d["lineas"] if not l["combo"])

plantilla = open("pdf_final_template.html", encoding="utf-8").read()
html = (plantilla.replace("__FILAS__", filas_item).replace("__MEJORES__", mejores)
        .replace("__TC__", str(tot["c"])).replace("__TU__", format(tot["u"], ","))
        .replace("__TQ__", format(round(tot["q"]), ",")).replace("__SINCOMBO__", str(sincombo)))
open(r"C:\Users\Diego\projects\clan\coo\walmart\expediente-walmart-2026\pedido-refuerzo\PEDIDO-MEDICION-NO-MODULADOS.html", "w", encoding="utf-8").write(html)

with open(r"C:\Users\Diego\projects\clan\coo\walmart\expediente-walmart-2026\pedido-refuerzo\COMBOS-POR-HABILITAR.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.writer(f); w.writerow(["tienda","num_tienda","item","num_item"])
    for l in sorted(d["lineas"], key=lambda x:(x["it"], x["tn"])):
        if not l["combo"]:
            w.writerow([tiendas40[l["tn"]]["nombre"], l["tn"], NOMBRE[l["it"]], l["it"]])

mp = r"C:\Users\Diego\projects\clan\coo\walmart\expediente-walmart-2026\pedido-refuerzo\PEDIDO-REFUERZO.html"
html2 = open(mp, encoding="utf-8").read()
html2 = re.sub(r"/\*__DATOS__\*/.*?/\*__FIN_DATOS__\*/",
               lambda m: "/*__DATOS__*/" + json.dumps(final, ensure_ascii=False) + "/*__FIN_DATOS__*/",
               html2, count=1, flags=re.S)
open(mp, "w", encoding="utf-8").write(html2)

ap = r"C:\Users\Diego\projects\clan\coo\walmart\expediente-walmart-2026\pedido-refuerzo\ANALISIS-REFUERZO.html"
a = open(ap, encoding="utf-8").read()
a = a.replace("174 cajas · 3,480 u · Q 74,022 (OLA A · tiendas limpias)", "167 cajas · 3,340 u · Q 69,436 (FINAL conservador)")
a = re.sub(r"Resultado en olas:.*?requieren habilitar combo ítem-tienda\.",
 "Plan FINAL (regla conservadora de Diego, 31/08 noche): <b class=\"num\">167 cajas · 3,340 u · Q 69,436</b> — "
 "1 caja por tienda SIN existencias · 2 máx en las mejores tiendas históricas del ítem (rotación de lanzamiento ClanTrack) · "
 "11 tiendas con incidencias bloqueadas · evaluación diaria y reposición por comportamiento · 118 combos por habilitar (CSV). "
 "Entregables: Excel formato Cristian (SUGERIDO_WALMART_2026-08-31.xlsx) + PDF de justificación.", a, count=1, flags=re.S)
open(ap, "w", encoding="utf-8").write(a)
print("ok: html pdf-base, csv", sincombo, "lineas, manifiesto, memoria")
