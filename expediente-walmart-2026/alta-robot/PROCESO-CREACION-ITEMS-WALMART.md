# El robot de Walmart — proceso de creación de ítems, documentado

**Fuente oficial:** https://www.walmartcentroamerica.com/proveedores/actualizacion (revisada 27-ago-2026).
Instructivo canónico: `walmart-oficial/PreValidacionCreaciones.pdf` (33 págs, descargado de esa página).
**Fuente de la casa:** historial de correos de diego@clancervecero.com — envíos reales de 2024 (IDs 3700,
6140, 10740), jul-2025 (Oktoberfest) y oct-2025 (VENDOR_258977, el último aprobado).

---

## Los dos caminos

1. **Catálogo Electrónico / Sincronización de datos (GS1)** — el proceso que Walmart impulsa a futuro.
   Requiere estar sincronizado con el catálogo electrónico de GS1. Clan **no** lo usa hoy.
2. **Proceso alternativo (el «robot»)** — solicitud automatizada por correo con plantilla prevalidada.
   **Este es el que usa Clan** y el documentado abajo.

## Las piezas

| Pieza | Qué es | Copia local |
|---|---|---|
| `PlantillaF.xlsm` | El formulario: **New Item Input Form 1.0.4**, hoja «Simplificada» (155 col). La MISMA versión que Clan usó en oct-2025 — no ha cambiado. | `walmart-oficial/PlantillaF.xlsm` |
| `Prevalidador_CreacionArt.xlsm` | La macro validadora (v1.0.8). Se instala en `C:\DATA\` y crea carpetas `Por_Validar`, `Validados`, `Archivos`. Solo Windows. | `walmart-oficial/Prevalidador_CreacionArt.xlsm` |
| `AlcoholicBeverages.xlsx` | Plantilla de atributos E-Commerce para bebidas alcohólicas (la macro la exige según División/Atributos). | `walmart-oficial/AlcoholicBeverages-ecommerce.xlsx` |
| Buzón del robot | `CAMSOLCAT23@walmart.com` | — |

## El paso a paso

1. **Llenar la PlantillaF**, hoja «Simplificada», **desde la fila 6** (fila 4 = encabezados, fila 5 =
   tipos de dato — NO se borra ni se insertan/eliminan celdas: la hoja oculta «Tabla» tiene fórmulas que
   se rompen con #REF y el robot cancela la solicitud).
2. Guardar la plantilla llena en la carpeta `Por_Validar` y correr el **Prevalidador** → botón Validar.
   Errores salen en la hoja RESUMEN con fila, columna y mensaje; se corrige y se revalida hasta cero.
3. Sin errores, la macro pide el **correo del comprador** (dominio @walmart.com — Max Sosa) y las
   **plantillas E-Commerce** según el tipo de artículo (para cerveza: AlcoholicBeverages). Las descarga
   ella misma o se le entregan llenas.
4. **Adjuntar en la carpeta `Archivos`:**
   - 7–11 fotos por artículo (JPG, <5 MB c/u).
   - 5 fotos E-Commerce 300×300 JPG nombradas `{UPC}-01.jpg`, `{UPC}-02.jpg`… (sin esa nomenclatura
     exacta la herramienta no continúa).
   - **Registro sanitario** con vigencia mayor a 3 meses desde la fecha de creación.
   - Documento de **temporalidad** (PDF) si el Tipo de Producto es TEMPORAL (así fue Oktoberfest).
   - MRTM solo si canal Directo (Clan es CROSSDOCK/CENTRALIZADO — no aplica).
5. La macro genera en `Validados` el archivo final `PreCreacion_Vendor-…_OK.xlsx` y ZIPs de ≤5 MB en
   `Archivos`.
6. **Correo al robot:** a `CAMSOLCAT23@walmart.com`, asunto que **inicie con `VENDOR`** (ej.
   `VENDOR_258977 - Creación …`), **texto plano, sin firma, sin imágenes, sin cuerpo**. En el PRIMER
   correo van la plantilla OK.xlsx y las plantillas E-Commerce.
7. El robot responde con un **número de gestión**. Los archivos restantes (fotos pesadas) se mandan en
   correos con asunto `Gestion (####)` usando ese número (así fueron los «Gestion (12070) Consolidado
   1/2/3» de jul-2025).
8. Sigue la cadena interna: **Aprobación** del comprador en la app («Aprobación Exitosa ID …») →
   posible **Rechazo** con la plantilla marcada en color para corregir → **Finalizado** con los números
   de ítem asignados («Items creados 75253328 75253329»).

## Trampas conocidas (ya le pasaron a Clan)

- ⚠️ **El asunto jamás puede llegar como `Ext: VENDOR …`.** Algunos clientes de correo lo anteponen y
  el robot cancela la solicitud. Fue exactamente el error de jul-2025 (Oktoberfest, 3 intentos
  rechazados). El envío bueno de oct-2025 salió como `VENDOR_258977` limpio.
- ⚠️ **Firma o imágenes en el correo = cancelación.** Mandar en texto sin formato.
- ⚠️ **Plantilla incompleta = rechazo formal** (Rechazo N°1/N°2 de 2024: llenado incorrecto y falta de
  formulario de temporales). Cada rechazo cuesta días.
- ⚠️ Correos de Walmart no aceptan más de 13 MB; la macro ya parte en ZIPs de 5 MB — no renombrar
  ningún archivo generado.
- ⚠️ Registro sanitario con menos de 3 meses de vigencia no pasa.
- Soporte: mismo buzón, asunto `Vendor Soporte`, la duda en el cuerpo. (Sin cambiar el asunto de la
  respuesta automática.)

## Datos fijos de Clan para el formulario

- Vendor: **258977** (9 dígitos con depto: 10000790518 — así va en «Número del Proveedor»).
- Comprador precargado en la plantilla: ABCO IMPORTADOS · correo del comprador: Max.Sosa@walmart.com.
- Patrón aprobado de las 16 filas vivas (envío oct-2025): país GT · Marca 50042 (230715 para Sapporo) ·
  Marca Privada Y · Estante MULTI + ml · UOM EA/1/1/EA · tiempo de entrega 5 · RRP Y · Tipo Producto
  BÁSICO (TEMPORAL para Oktoberfest) · Canal CENTRALIZADO (en formatos: CROSSDOCK) · División ABARROTES.
- El «Costo de empaque de proveedor» es el costo por caja para Walmart (unitario × unidades/caja).
  Relación vigente del catálogo: costo ≈ PVP sin IVA × 0.771 (margen Walmart ~23%).
- UPC/EAN va **sin dígito verificador** (columna aparte). El EAN de la caja sigue la guía DUN-14/ITF-14
  (`walmart-oficial/guia-dun14-gs1.pdf`): indicador logístico (1–8) + GTIN-13 sin DV + DV recalculado.
- Los ítems ABCO de tipo TEMPORAL exigen además el **Formulario de Items Temporales**
  (`Downloads/Formulario Items Temporales.pdf` es el machote de jun-2026).
- Las medidas modulares (unidad/RRP/caja en cm + estibas/frentes) se piden aparte cuando hay gestión de
  modulares (`V_..._MODULARES.xlsx`, como en Gestion 12070).

## Estado del alta del 27-ago-2026

- `Alta_Robot_NewItemForm_borrador.xlsx` (esta carpeta): las **19 filas** de los ítems aprobados en la
  reunión, en el orden exacto de columnas de la PlantillaF, con semáforo de colores (verde=firme,
  gris=patrón aprobado, ámbar=propuesto, rojo=falta).
- Los huecos rojos (EAN de caja física, registros sanitarios, costos por firmar, PVP faltantes) se
  capturan en `Captura_Alta_Robot.html` y con ese export se escribe la PlantillaF final.
