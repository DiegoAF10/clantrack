#!/usr/bin/env python3
"""
WALMART INTELLIGENCE SYSTEM — Clan Cervecero
Main entry point. Simple commands:

  python walmart.py report     → Generate fresh report from current data
  python walmart.py import     → Import new data (template + dropped files) + generate report
  python walmart.py status     → Quick database status
  python walmart.py template   → Open input template in Excel
  python walmart.py open       → Open latest report in Excel
"""

import sqlite3
import os
import sys
import subprocess

sys.stdout.reconfigure(encoding='utf-8')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "walmart.db")


def cmd_status():
    """Show database status."""
    conn = sqlite3.connect(DB_PATH)

    rl = conn.execute("SELECT COUNT(*) FROM retail_link").fetchone()[0]
    si = conn.execute("SELECT COUNT(*) FROM sell_in").fetchone()[0]
    dev = conn.execute("SELECT COUNT(*) FROM devoluciones").fetchone()[0]
    prod = conn.execute("SELECT COUNT(*) FROM productos").fetchone()[0]
    tiendas = conn.execute("SELECT COUNT(*) FROM tiendas").fetchone()[0]

    weeks = conn.execute("SELECT MIN(semana_wm), MAX(semana_wm) FROM retail_link").fetchone()
    last_import = conn.execute("SELECT imported_at, rows_added FROM import_log ORDER BY id DESC LIMIT 1").fetchone()

    total_venta = conn.execute("SELECT SUM(venta_und), SUM(venta_q) FROM retail_link WHERE tipo_registro='VENTA'").fetchone()
    inv = conn.execute("""
        SELECT SUM(inv_actual) FROM retail_link
        WHERE tipo_registro='INVENTARIO'
        AND semana_wm = (SELECT MAX(semana_wm) FROM retail_link WHERE tipo_registro='INVENTARIO')
    """).fetchone()[0] or 0

    db_size = os.path.getsize(DB_PATH) / (1024 * 1024)

    print("╔═══════════════════════════════════════════╗")
    print("║  WALMART DB — Status                      ║")
    print("╚═══════════════════════════════════════════╝")
    print(f"  Retail Link:    {rl:>10,} rows  ({weeks[0]} → {weeks[1]})")
    print(f"  Sell-In:        {si:>10,} rows")
    print(f"  Devoluciones:   {dev:>10,} rows")
    print(f"  Productos:      {prod:>10,} rows")
    print(f"  Tiendas:        {tiendas:>10,} rows")
    print(f"  ─────────────────────────────")
    print(f"  Ventas totales: {total_venta[0] or 0:>10,} und  Q{total_venta[1] or 0:>12,.2f}")
    print(f"  Inv. actual:    {inv:>10,} und")
    print(f"  DB size:        {db_size:>10.1f} MB")
    if last_import:
        print(f"  Last import:    {last_import[0]}  (+{last_import[1]:,} rows)")
    print("═══════════════════════════════════════════")

    conn.close()


def cmd_report():
    """Generate report."""
    import generate_report
    generate_report.main()


def cmd_import():
    """Import + report."""
    sys.argv = ['import_walmart.py', '--report']
    import import_walmart
    import_walmart.main()


def cmd_template():
    """Open template in Excel."""
    path = os.path.join(BASE_DIR, "templates", "WM-INPUT.xlsx")
    if not os.path.exists(path):
        import create_template
        create_template.main()
    subprocess.Popen(['start', '', path], shell=True)
    print(f"Opening: {path}")


def cmd_open():
    """Open latest report."""
    path = os.path.join(BASE_DIR, "reports", "REPORTE-WM_latest.xlsx")
    if not os.path.exists(path):
        print("No report found. Run: python walmart.py report")
        return
    subprocess.Popen(['start', '', path], shell=True)
    print(f"Opening: {path}")


def main():
    commands = {
        'status': cmd_status,
        'report': cmd_report,
        'import': cmd_import,
        'template': cmd_template,
        'open': cmd_open,
    }

    if len(sys.argv) < 2 or sys.argv[1] not in commands:
        print("WALMART INTELLIGENCE SYSTEM — Clan Cervecero")
        print()
        print("Commands:")
        print("  python walmart.py status     DB status")
        print("  python walmart.py report     Generate fresh report")
        print("  python walmart.py import     Import new data + report")
        print("  python walmart.py template   Open input template")
        print("  python walmart.py open       Open latest report")
        return

    commands[sys.argv[1]]()


if __name__ == '__main__':
    main()
