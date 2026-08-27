-- WALMART Intelligence System — Clan Cervecero
-- SQLite schema v1.0
-- Replaces MASTER WALMART.xlsx (21 sheets, 242K+ rows)

PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- ═══════════════════════════════════════════════════════
-- REFERENCE TABLES
-- ═══════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS productos (
    producto        TEXT NOT NULL,
    ingreso_wm_date TEXT,
    precio_publico  REAL,
    und_x_caja      INTEGER,
    litros          REAL,
    ingreso_caja_wm REAL,
    ingreso_und_wm  REAL,
    marca           TEXT,
    codigo          TEXT PRIMARY KEY,
    costo_clan_caja REAL,
    costo_clan_und  REAL,
    item_wm         TEXT,
    item_wm_ant     TEXT,
    codigo_barras   TEXT,
    costo_und_usd   REAL,
    estado          TEXT DEFAULT 'Activo',
    temporada_ini   TEXT,
    temporada_fin   TEXT
);

CREATE TABLE IF NOT EXISTS tiendas (
    no_tienda   INTEGER PRIMARY KEY,
    nombre      TEXT NOT NULL,
    formato     TEXT,
    region      TEXT,
    estado      TEXT DEFAULT 'Activo'
);

-- ═══════════════════════════════════════════════════════
-- CORE DATA TABLES
-- ═══════════════════════════════════════════════════════

-- Unified Retail Link data (RL_Master + FormatoRepSemanal)
CREATE TABLE IF NOT EXISTS retail_link (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source          TEXT NOT NULL,  -- 'rl_master' or 'formato_semanal'
    tipo_registro   TEXT,           -- 'VENTA' or 'INVENTARIO'
    semana_wm       TEXT NOT NULL,
    diario          TEXT,           -- date string
    item_nbr        TEXT NOT NULL,
    producto        TEXT,
    store_nbr       INTEGER NOT NULL,
    tienda          TEXT,
    costo_q         REAL,
    costo_usd       REAL,
    precio_q        REAL,
    precio_usd      REAL,
    venta_und       INTEGER DEFAULT 0,
    venta_q         REAL DEFAULT 0,
    venta_usd       REAL DEFAULT 0,
    venta_comp_q    REAL DEFAULT 0,
    venta_comp_usd  REAL DEFAULT 0,
    venta_comp_und  INTEGER DEFAULT 0,
    inv_actual      INTEGER DEFAULT 0,
    sem_abasto      REAL,
    pedido_actual   INTEGER DEFAULT 0,
    tasa_venta      REAL,
    venta_costo_q   REAL DEFAULT 0,
    venta_costo_usd REAL DEFAULT 0,
    instock_act_pct REAL,
    instock_prom_pct REAL,
    tiendas_validas INTEGER,
    cod_faltante    TEXT,
    cant_faltante   INTEGER,
    fecha_faltante  TEXT,
    pronostico_52s  INTEGER,
    inv_historico   INTEGER,
    fecha_tc        TEXT,
    tipo_cambio     REAL,
    -- Extra from FormatoRepSemanal
    margen_pct      REAL,
    cod_barras      TEXT,
    max_estante     INTEGER,
    pedido_transito INTEGER,
    inv_liquidacion INTEGER,
    -- Calculated
    codigo_interno  TEXT,
    estado_producto TEXT,
    anio            INTEGER,
    mes             INTEGER,
    imported_at     TEXT DEFAULT (datetime('now'))
);

-- Dedup index: one record per semana+item+store+tipo
CREATE UNIQUE INDEX IF NOT EXISTS idx_rl_dedup
    ON retail_link(semana_wm, item_nbr, store_nbr, tipo_registro, diario);

CREATE INDEX IF NOT EXISTS idx_rl_semana ON retail_link(semana_wm);
CREATE INDEX IF NOT EXISTS idx_rl_producto ON retail_link(item_nbr);
CREATE INDEX IF NOT EXISTS idx_rl_tienda ON retail_link(store_nbr);
CREATE INDEX IF NOT EXISTS idx_rl_tipo ON retail_link(tipo_registro);
CREATE INDEX IF NOT EXISTS idx_rl_anio_mes ON retail_link(anio, mes);

-- Sell-In: What Clan invoices to Walmart
CREATE TABLE IF NOT EXISTS sell_in (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    codigo_producto     TEXT NOT NULL,
    nombre_producto     TEXT,
    cliente             TEXT,
    fecha               TEXT NOT NULL,
    cantidad_facturada  INTEGER NOT NULL,
    fecha_esperada      TEXT,
    producto_precios    TEXT,
    und_x_caja          INTEGER,
    cajas_facturadas    REAL,
    precio_caja_wm      REAL,
    costo_caja_clan     REAL,
    ingreso_bruto       REAL,
    costo_total         REAL,
    centralizacion      REAL,
    ingreso_neto        REAL,
    margen_bruto        REAL,
    margen_pct          REAL,
    marca               TEXT,
    anio_mes            TEXT,
    imported_at         TEXT DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_si_dedup
    ON sell_in(codigo_producto, fecha, cantidad_facturada);

-- Devoluciones / Claims
CREATE TABLE IF NOT EXISTS devoluciones (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    semana_wm               TEXT,
    diario                  TEXT,
    item_nbr                TEXT,
    descripcion             TEXT,
    pais                    TEXT,
    cant_reclamo_tienda     REAL DEFAULT 0,
    precio_reclamo_q        REAL DEFAULT 0,
    precio_reclamo_usd      REAL DEFAULT 0,
    costo_reclamo_q         REAL DEFAULT 0,
    costo_reclamo_usd       REAL DEFAULT 0,
    devolucion_cliente_und  REAL DEFAULT 0,
    costo_devolucion_q      REAL DEFAULT 0,
    costo_devolucion_usd    REAL DEFAULT 0,
    precio_devolucion_q     REAL DEFAULT 0,
    precio_devolucion_usd   REAL DEFAULT 0,
    fecha_tc                TEXT,
    tipo_cambio             REAL,
    estado                  TEXT,
    imported_at             TEXT DEFAULT (datetime('now'))
);

-- ═══════════════════════════════════════════════════════
-- METADATA
-- ═══════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS import_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source      TEXT NOT NULL,
    filename    TEXT,
    rows_added  INTEGER,
    rows_duped  INTEGER DEFAULT 0,
    imported_at TEXT DEFAULT (datetime('now'))
);

-- ═══════════════════════════════════════════════════════
-- VIEWS — Pre-computed queries for report generation
-- ═══════════════════════════════════════════════════════

-- Current inventory (latest week, INVENTARIO type only)
CREATE VIEW IF NOT EXISTS v_inventario_actual AS
SELECT
    rl.item_nbr,
    rl.producto,
    rl.store_nbr,
    rl.tienda,
    rl.inv_actual,
    rl.semana_wm
FROM retail_link rl
WHERE rl.tipo_registro = 'INVENTARIO'
  AND rl.semana_wm = (
      SELECT MAX(semana_wm) FROM retail_link WHERE tipo_registro = 'INVENTARIO'
  );

-- Sales by product (all time)
CREATE VIEW IF NOT EXISTS v_ventas_producto AS
SELECT
    item_nbr,
    producto,
    SUM(venta_und) as total_und,
    SUM(venta_q) as total_q,
    SUM(venta_costo_q) as total_costo_q,
    COUNT(DISTINCT store_nbr) as tiendas_activas,
    COUNT(DISTINCT semana_wm) as semanas_con_venta
FROM retail_link
WHERE tipo_registro IN ('VENTA', NULL)
  AND venta_und > 0
GROUP BY item_nbr, producto;

-- Sales by store (all time)
CREATE VIEW IF NOT EXISTS v_ventas_tienda AS
SELECT
    store_nbr,
    tienda,
    SUM(venta_und) as total_und,
    SUM(venta_q) as total_q,
    SUM(venta_costo_q) as total_costo_q,
    COUNT(DISTINCT item_nbr) as productos_vendidos,
    COUNT(DISTINCT semana_wm) as semanas_activas
FROM retail_link
WHERE tipo_registro IN ('VENTA', NULL)
  AND venta_und > 0
GROUP BY store_nbr, tienda;

-- Weekly sales trend
CREATE VIEW IF NOT EXISTS v_tendencia_semanal AS
SELECT
    semana_wm,
    MIN(diario) as fecha_inicio,
    SUM(venta_und) as total_und,
    SUM(venta_q) as total_q,
    SUM(venta_costo_q) as total_costo_q,
    COUNT(DISTINCT item_nbr) as productos_vendidos,
    COUNT(DISTINCT store_nbr) as tiendas_activas
FROM retail_link
WHERE tipo_registro IN ('VENTA', NULL)
GROUP BY semana_wm
ORDER BY semana_wm;

-- Sell-In summary by product
CREATE VIEW IF NOT EXISTS v_sellin_producto AS
SELECT
    codigo_producto,
    nombre_producto,
    marca,
    SUM(cantidad_facturada) as total_facturado,
    SUM(ingreso_bruto) as total_ingreso_bruto,
    SUM(costo_total) as total_costo,
    SUM(centralizacion) as total_centralizacion,
    SUM(ingreso_neto) as total_ingreso_neto,
    SUM(margen_bruto) as total_margen,
    CASE WHEN SUM(ingreso_bruto) > 0
        THEN SUM(margen_bruto) * 1.0 / SUM(ingreso_bruto)
        ELSE 0 END as margen_pct_promedio
FROM sell_in
GROUP BY codigo_producto, nombre_producto, marca;

-- Devoluciones summary
CREATE VIEW IF NOT EXISTS v_devoluciones_producto AS
SELECT
    item_nbr,
    descripcion,
    SUM(cant_reclamo_tienda) as total_reclamos_und,
    SUM(precio_reclamo_q) as total_reclamos_q,
    SUM(devolucion_cliente_und) as total_devoluciones_und,
    SUM(precio_devolucion_q) as total_devoluciones_q,
    COUNT(*) as registros
FROM devoluciones
GROUP BY item_nbr, descripcion;
