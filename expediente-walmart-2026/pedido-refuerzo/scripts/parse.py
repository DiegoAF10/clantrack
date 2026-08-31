# -*- coding: utf-8 -*-
"""Parsea los 5 reportes Retail Link (semana 202630) a CSVs limpios + agregados."""
import sys, io, warnings
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
warnings.filterwarnings('ignore')
import pandas as pd
import numpy as np

def load(path, data_start):
    df = pd.read_excel(path, header=None)
    hdr = df.iloc[data_start].tolist()
    data = df.iloc[data_start + 1:].copy()
    data.columns = [str(h).strip() if pd.notna(h) else f'col{i}' for i, h in enumerate(hdr)]
    data = data.dropna(how='all')
    return data

def dedup_cols(df):
    # columnas duplicadas (versiones US$) -> sufijo
    seen = {}
    cols = []
    for c in df.columns:
        if c in seen:
            seen[c] += 1
            cols.append(f'{c}__{seen[c]}')
        else:
            seen[c] = 0
            cols.append(c)
    df.columns = cols
    return df

class P:
    """Acceso a columnas por prefijo: los headers reales son largos."""
    def __init__(self, df):
        self.df = df
    def __getitem__(self, prefix):
        matches = [c for c in self.df.columns if c.startswith(prefix) and '__' not in c]
        if not matches:
            matches = [c for c in self.df.columns if c.startswith(prefix)]
        if not matches:
            raise KeyError(prefix + ' | disponibles: ' + ' ;; '.join(self.df.columns))
        return self.df[matches[0]]

# ---------- f1: Formato Nuevo Semanal (202630, con reclamos) ----------
f1 = P(dedup_cols(load('f1_113562167.xlsx', 29)))
# ---------- f2: Reporte Sugerido ----------
f2 = P(dedup_cols(load('f2_113560790.xlsx', 22)))
# ---------- f3: Ventas Semanales Clan (por dia) ----------
f3 = P(dedup_cols(load('f3_113560909.xlsx', 23)))
# ---------- f4: K1 Reporte 1 Claude (matriz completa con ceros) ----------
f4 = P(dedup_cols(load('f4_113557852.xlsx', 25)))
# ---------- f5: Articulo Tienda Valido ----------
f5 = P(dedup_cols(load('f5_113557850.xlsx', 20)))

num = lambda s: pd.to_numeric(s, errors='coerce').fillna(0)

# Normalizar f4 (la matriz maestra)
f4r = pd.DataFrame({
    'semana': f4['Semana Walmart'],
    'item': f4['N�mero del Art�culo Primario'].astype(str).str.strip(),
    'desc': f4['Descripci�n Art�culo Primario'].astype(str).str.strip(),
    'tienda_num': f4['N�mero de Tienda'].astype(str).str.strip(),
    'tienda': f4['Nombre de la Tienda'].astype(str).str.strip(),
    'costo_u': num(f4['Costo Unitario']),
    'precio_u': num(f4['Precio Unitario']),
    'venta_u': num(f4['Venta en Unidades']),
    'venta_q': num(f4['Venta en Moneda']),
    'existencia': num(f4['Cantidad en Existencia en la Tienda Actu']),
    'semanas_abasto': num(f4['Semanas de Abasto de la Tienda']),
    'en_pedido': num(f4['Cantidad en Pedido de la Tienda Actual']),
    'tasa_venta': num(f4['Demanda Destemporalizada Promedio de Tie']),
    'instock_actual': num(f4['Instock Actual %']),
    'instock_prom': num(f4['Instock Promedio %']),
    'faltante_cant': num(f4['Cantidad de Faltantes  de la Tienda']),
    'faltante_cod': f4['C�digo de Faltante de la Tienda'],
    'proyeccion_52w': num(f4['Proyecci�n 52/53 Semanas de Pron�stico e']),
    'exist_historicas': num(f4['Cantidad en Existencias Hist�ricas']),
    'transito': num(f4['Cantidad Pedida por la Tienda en Transit']),
})
f4r.to_csv('t_matriz_202630.csv', index=False, encoding='utf-8-sig')

# f1: max shelf + reclamos/devoluciones
f1r = pd.DataFrame({
    'item': f1['N�mero del Art�culo Primario'].astype(str).str.strip(),
    'desc': f1['Descripci�n Art�culo Primario'].astype(str).str.strip(),
    'tienda_num': f1['N�mero de Tienda'].astype(str).str.strip(),
    'tienda': f1['Nombre de la Tienda'].astype(str).str.strip(),
    'venta_u': num(f1['Venta en Unidades']),
    'venta_q': num(f1['Venta en Moneda']),
    'max_shelf': num(f1['Cantidad M�xima del Estante']),
    'existencia': num(f1['Cantidad en Existencia en la Tienda Actu']),
    'transito': num(f1['Cantidad Pedida por la Tienda en Transit']),
    'en_pedido': num(f1['Cantidad en Pedido de la Tienda Actual']),
    'tasa_venta': num(f1['Demanda Destemporalizada Promedio de Tie']),
    'reclamo_u': num(f1['Cantidad de Reclamo de la Tienda al Prov']),
    'devolucion_u': num(f1['Art�culo de Devoluci�n del Cliente Total']),
    'venta_costo': num(f1['Ventas a Costo']),
})
f1r.to_csv('t_formato_nuevo_202630.csv', index=False, encoding='utf-8-sig')

# f2: sugerido (CD + semanas abasto)
f2r = pd.DataFrame({
    'item': f2['N�mero del Art�culo Primario'].astype(str).str.strip(),
    'desc': f2['Descripci�n Art�culo Primario'].astype(str).str.strip(),
    'cod_producto': f2['C�digo del Producto'].astype(str).str.strip(),
    'tienda_num': f2['N�mero de Tienda'].astype(str).str.strip(),
    'tienda': f2['Nombre de la Tienda'].astype(str).str.strip(),
    'tipo_articulo': f2['Tipo de Art�culo'].astype(str).str.strip(),
    'venta_um_4sem': num(f2['Cantidad a la Venta en Unidad de Medida']),
    'exist_historicas': num(f2['Cantidad en Existencias Hist�ricas']),
    'existencia': num(f2['Cantidad en Existencia en la Tienda Actu']),
    'transito': num(f2['Cantidad Pedida por la Tienda en Transit']),
    'exist_cd': num(f2['Cantidad en Existencia del Centro de Dis']),
    'en_pedido': num(f2['Cantidad en Pedido de la Tienda Actual']),
    'faltantes': num(f2['Cantidad de Faltantes  de la Tienda']),
    'semanas_abasto': num(f2['Semanas de Abasto de la Tienda']),
})
f2r.to_csv('t_sugerido.csv', index=False, encoding='utf-8-sig')

# f3: ventas por dia + config tarima + FECHA EXPIRACION (vigencias!)
f3r = pd.DataFrame({
    'item': f3['N�mero del Art�culo Primario'].astype(str).str.strip(),
    'desc': f3['Descripci�n Art�culo Primario'].astype(str).str.strip(),
    'tipo_articulo': f3['Tipo de Art�culo'].astype(str).str.strip(),
    'fecha_obsoleta': f3['Fecha Obsoleta'].astype(str),
    'fecha_expiracion': f3['Fecha de Expiraci�n'].astype(str),
    'cajas_por_cama': f3['Cantidad de Cajas por Camas/Capas por Ta'],
    'camas_por_tarima': f3['Cantidad de Camas/Capas por Tarima'],
    'tienda_num': f3['N�mero de Tienda'].astype(str).str.strip(),
    'tienda': f3['Nombre de la Tienda'].astype(str).str.strip(),
    'venta_u': num(f3['Venta en Unidades']),
    'venta_q': num(f3['Venta en Moneda']),
})
f3r.to_csv('t_ventas_dia.csv', index=False, encoding='utf-8-sig')

# f5: mapa de validez / modulado
f5r = pd.DataFrame({
    'item': f5['N�mero de Art�culo'].astype(str).str.strip(),
    'desc': f5['Descripci�n 1 del Art�culo'].astype(str).str.strip(),
    'estatus': f5['Estatus del Art�culo'].astype(str).str.strip(),
    'tipo_articulo': f5['Tipo de Art�culo'].astype(str).str.strip(),
    'fecha_efectiva': f5['Fecha Efectiva'].astype(str),
    'fecha_obsoleta': f5['Fecha Obsoleta'].astype(str),
    'nunca_agotado': f5['Bandera Nunca Agotado'].astype(str).str.strip(),
    'cod_modulado_mbm': f5['C�digo de Art�culo Modulado Actual MBM'].astype(str).str.strip(),
    'tienda_num': f5['N�mero de Tienda'].astype(str).str.strip(),
    'tienda': f5['Nombre de la Tienda'].astype(str).str.strip(),
    'formato': f5['Abreviatura del Formato'].astype(str).str.strip(),
    'ciudad': f5['Ciudad'].astype(str).str.strip(),
    'cd_regular': f5['Centro de Distribuci�n Regular'].astype(str).str.strip(),
})
f5r.to_csv('t_valido.csv', index=False, encoding='utf-8-sig')

# =================== RESUMENES ===================
print('### CATALOGO (f4, red completa 202630) ###')
cat = f4r.groupby(['item', 'desc']).agg(
    tiendas=('tienda_num', 'nunique'),
    venta_u=('venta_u', 'sum'),
    venta_q=('venta_q', 'sum'),
    existencia=('existencia', 'sum'),
    transito=('transito', 'sum'),
    en_pedido=('en_pedido', 'sum'),
    tasa_venta_sum=('tasa_venta', 'sum'),
    faltantes=('faltante_cant', 'sum'),
    proyeccion=('proyeccion_52w', 'sum'),
).reset_index().sort_values('venta_q', ascending=False)
print(cat.to_string(index=False))
print()
print('### VENTA TOTAL RED 202630: Q', round(f4r['venta_q'].sum(), 2), '· unidades', int(f4r['venta_u'].sum()))
print()
print('### f5: items y tiendas validas + codigo modulado ###')
mod = f5r.groupby(['item', 'desc', 'cod_modulado_mbm', 'nunca_agotado']).agg(tiendas_validas=('tienda_num', 'nunique')).reset_index()
print(mod.to_string(index=False))
print()
print('### f3: vigencias (fecha_expiracion por item) ###')
vig = f3r.groupby(['item', 'desc', 'fecha_expiracion', 'fecha_obsoleta']).size().reset_index(name='filas')
print(vig.to_string(index=False))
print()
print('### f2: stock en CD por item ###')
cd = f2r.groupby(['item', 'desc']).agg(exist_cd=('exist_cd', 'max'), tiendas=('tienda_num', 'nunique'),
                                        venta_um_4sem=('venta_um_4sem', 'sum')).reset_index()
print(cd.to_string(index=False))
print()
print('### Tiendas en f4:', f4r['tienda_num'].nunique(), '| en f5:', f5r['tienda_num'].nunique())
