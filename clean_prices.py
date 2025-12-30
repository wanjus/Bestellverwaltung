"""
clean_prices.py

Sucht in der Tabelle `Produkte` nach nicht-numerischen Einträgen im Feld `Preis`
und konvertiert gängige Formate (z.B. "179,00", "€ 12.34", "12 345,67") in
numerische `REAL`-Werte. Vor jeder Änderung wird eine Sicherung der DB empfohlen.

Nutzung:
    python clean_prices.py       # führt Bereinigung durch
    python clean_prices.py --dry  # zeigt nur, was geändert würde

"""
import sqlite3
import re
import argparse
import shutil
import datetime
import os

DB = 'bestellverwaltung.db'


def backup_db():
    if not os.path.exists(DB):
        return None
    ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    name = f"{os.path.splitext(DB)[0]}_CLEANBACKUP_{ts}.db"
    shutil.copy2(DB, name)
    return name


def clean_prices(dry_run=False):
    conn = sqlite3.connect(DB)
    c = conn.cursor()

    rows = list(c.execute("SELECT ProduktID, Produktname, Preis FROM Produkte"))
    changes = []

    for pid, name, preis in rows:
        orig = preis
        parsed = None
        if isinstance(preis, (int, float)):
            continue
        s = str(preis).strip()
        if s == '':
            continue
        s = s.replace(',', '.')
        s = re.sub(r"[^0-9.\-]", "", s)
        # if multiple dots, keep last as decimal separator
        if s.count('.') > 1:
            parts = s.split('.')
            s = ''.join(parts[:-1]) + '.' + parts[-1]
        try:
            val = float(s)
            parsed = val
        except Exception:
            parsed = None
        if parsed is not None and parsed != orig:
            changes.append((pid, name, orig, parsed))

    if not changes:
        print("Keine zu bereinigenden Preise gefunden.")
        conn.close()
        return 0

    print(f"Gefundene fehlerhafte Preise: {len(changes)}")
    for ch in changes:
        print(f"ID {ch[0]} - {ch[1]}: {ch[2]} -> {ch[3]}")

    if dry_run:
        print("\nDry-run: keine Änderungen werden geschrieben.")
        conn.close()
        return len(changes)

    backup = backup_db()
    if backup:
        print(f"Datenbank gesichert als: {backup}")

    for pid, name, orig, parsed in changes:
        c.execute("UPDATE Produkte SET Preis = ? WHERE ProduktID = ?", (parsed, pid))

    conn.commit()
    conn.close()
    print(f"{len(changes)} Preise bereinigt.")
    return len(changes)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Bereinigt Preis-Spalte in Tabelle Produkte')
    parser.add_argument('--dry', action='store_true', help='Nur prüfen, nichts schreiben')
    args = parser.parse_args()

    try:
        changed = clean_prices(dry_run=args.dry)
    except Exception as e:
        print('Fehler:', e)
        raise SystemExit(1)
    raise SystemExit(0)
