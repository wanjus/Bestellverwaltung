Release v0.1 - Bestellverwaltung (Kurz-Release-Notes)
Datum: 2025-12-30

Wesentliche Änderungen / Verbesserungen
- Database migration: automatische Nachrüstung fehlender Spalten (`Status`, `Rabatt`, `Mwst_Satz`) beim Start.
- Datenbank-Adapter für `datetime.date` registriert (vermeidet DeprecationWarning unter Python 3.12+).
- Robustere Fehlerbehandlung und Logging (`bestellverwaltung.log`).
- Trigger korrigiert und erweitert zur Bestandsverwaltung (Einfügen/Änderung von Bestellpositionen).
- Bestandsprüfung vor Bestellannahme (Vermeidet Negativ-Bestand).
- Bestellstatus eingeführt (`offen`, `versendet`, `geliefert`).
- Funktionen: Bestellpositionen ändern/löschen, Bestellverlauf, Suchfunktionen für Kunden/Produkte.
- Rabatt- & MwSt-Support pro Bestellung, Rechnungsberechnung inkl. Rabatt/MwSt.
- Menü erweitert (Backup, Migration, Preis-Bereinigung u.v.m.).
- `migrate_db.py` hinzugefügt (sichere, wiederholbare Migration/Repair-Script).
- `clean_prices.py` hinzugefügt (Bereinigung fehlerhafter Preisformate).

Kurze Hinweise
- Backup: `bestellverwaltung.py` kann Backups erstellen (Menüpunkt).
- Migration: Falls die DB manuell vorliegt, `python migrate_db.py` ausführen.
- Preisbereinigung (Dry-run): `python clean_prices.py --dry` und zur Ausführung ohne `--dry`.

Empfehlungen
- Vor größeren Änderungen immer ein Backup anlegen.
- Tests/Unit-Tests in Folgeversion ergänzen (v0.2).

Dateien neu/aktualisiert
- bestellverwaltung.py (Hauptprogramm, viele Verbesserungen)
- migrate_db.py (DB-Migration)
- clean_prices.py (Preisbereinigung)
- RELEASE_NOTES.md (diese Datei)

Commit-Hinweis
- Commit enthält alle oben genannten Änderungen. Bitte ggf. Branch/Tag nach eurem Workflow anpassen.
