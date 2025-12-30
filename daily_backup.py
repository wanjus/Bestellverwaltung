"""
daily_backup.py

Einfaches Backup-Skript für `bestellverwaltung.db`.
- Erstellt ein Backup mit Zeitstempel im selben Ordner.
- Optional: behält nur die letzten N Backups (Retention).

Usage: python daily_backup.py [--keep N]
"""
import shutil
import datetime
import argparse
import os

DB = 'bestellverwaltung.db'
DEFAULT_KEEP = 14


def backup_db(keep=DEFAULT_KEEP):
    if not os.path.exists(DB):
        print('Datenbankdatei nicht gefunden:', DB)
        return 1
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_name = f"bestellverwaltung_BACKUP_{timestamp}.db"
    shutil.copy2(DB, backup_name)
    print('Backup erstellt:', backup_name)

    # Retention: alte Backups löschen
    if keep is not None and keep > 0:
        files = sorted([f for f in os.listdir('.') if f.startswith('bestellverwaltung_BACKUP_') and f.endswith('.db')])
        if len(files) > keep:
            to_remove = files[:len(files)-keep]
            for f in to_remove:
                try:
                    os.remove(f)
                    print('Entferne altes Backup:', f)
                except Exception as e:
                    print('Fehler beim Entfernen:', f, e)
    return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--keep', type=int, default=DEFAULT_KEEP, help='Anzahl der Backups, die behalten werden sollen')
    args = parser.parse_args()
    raise SystemExit(backup_db(args.keep))
