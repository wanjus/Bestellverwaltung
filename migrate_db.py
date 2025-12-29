import sqlite3
import time

DB='bestellverwaltung.db'
print('Starte Migration: Bitte stelle sicher, dass kein anderes Programm die DB geöffnet hat.')
for attempt in range(6):
    try:
        conn=sqlite3.connect(DB, timeout=10)
        c=conn.cursor()
        break
    except Exception as e:
        print('connect attempt', attempt, 'failed:', e)
        time.sleep(1)
else:
    raise SystemExit('Konnte keine Verbindung zur DB herstellen.')

cols=[r[1] for r in c.execute("PRAGMA table_info(Bestellungen)")]
print('Aktuelle Spalten in Bestellungen:', cols)
changes=0
if 'Status' not in cols:
    try:
        c.execute("ALTER TABLE Bestellungen ADD COLUMN Status TEXT DEFAULT 'offen'")
        changes+=1
        print('Spalte Status hinzugefügt')
    except Exception as e:
        print('Fehler beim Hinzufügen von Status:', e)
if 'Rabatt' not in cols:
    try:
        c.execute("ALTER TABLE Bestellungen ADD COLUMN Rabatt REAL DEFAULT 0.0")
        changes+=1
        print('Spalte Rabatt hinzugefügt')
    except Exception as e:
        print('Fehler beim Hinzufügen von Rabatt:', e)
if 'Mwst_Satz' not in cols:
    try:
        c.execute("ALTER TABLE Bestellungen ADD COLUMN Mwst_Satz REAL DEFAULT 19.0")
        changes+=1
        print('Spalte Mwst_Satz hinzugefügt')
    except Exception as e:
        print('Fehler beim Hinzufügen von Mwst_Satz:', e)

conn.commit()
print('Anzahl Änderungen:', changes)
print('Spalten nach Migration:')
for r in c.execute("PRAGMA table_info(Bestellungen)"):
    print(r)

# Defaults setzen
try:
    c.execute("UPDATE Bestellungen SET Rabatt = 0.0 WHERE Rabatt IS NULL")
    c.execute("UPDATE Bestellungen SET Mwst_Satz = 19.0 WHERE Mwst_Satz IS NULL")
    c.execute("UPDATE Bestellungen SET Status = 'offen' WHERE Status IS NULL")
    conn.commit()
    print('Defaults gesetzt')
except Exception as e:
    print('Fehler beim Setzen der Defaults:', e)

conn.close()
print('Migration abgeschlossen.')
