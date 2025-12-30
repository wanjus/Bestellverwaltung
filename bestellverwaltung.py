import sqlite3
import datetime
import sys
import os
import shutil
import logging

DB_NAME = 'bestellverwaltung.db'

# Logging konfigurieren
logging.basicConfig(
    filename='bestellverwaltung.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# sqlite3 date adapter/converter registrieren (vermeidet DeprecationWarning ab Py3.12)
def _adapt_date(d: datetime.date) -> str:
    return d.isoformat()

def _convert_date(s: bytes) -> datetime.date:
    return datetime.date.fromisoformat(s.decode())

sqlite3.register_adapter(datetime.date, _adapt_date)
sqlite3.register_converter("DATE", _convert_date)

def connect_db():
    try:
        conn = sqlite3.connect(DB_NAME, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
        logging.error(f"Datenbankverbindung fehlgeschlagen: {e}")
        print(f"Fehler: Datenbank konnte nicht geöffnet werden: {e}")
        sys.exit(1)

def datenbank_sichern():
    print("\n--- Datenbank-Sicherung ---")
    try:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"bestellverwaltung_BACKUP_{timestamp}.db"
        shutil.copy2(DB_NAME, backup_name)
        logging.info(f"Datenbankbackup erstellt: {backup_name}")
        print(f"Sicherung erfolgreich erstellt: {backup_name}")
    except FileNotFoundError:
        logging.error("Datenbank-Datei nicht gefunden")
        print("Fehler: Datenbankdatei nicht gefunden")
    except Exception as e:
        logging.error(f"Fehler bei der Sicherung: {e}")
        print(f"Fehler bei der Sicherung: {e}")

def init_db():
    """Erstellt die Tabellen und füllt sie mit korrigierten Beispieldaten, falls die DB nicht existiert."""
    if os.path.exists(DB_NAME):
        print(f"Datenbank '{DB_NAME}' existiert bereits. Prüfe Schema auf Migrationen...")
        # Verbindungsaufbau und automatische Migration fehlender Spalten
        conn = connect_db()
        c = conn.cursor()
        try:
            c.execute("PRAGMA table_info(Bestellungen)")
            cols = [r[1] for r in c.fetchall()]

            # Gewünschte Spalten und ihre ALTER-Befehle
            migrations = []
            if 'Status' not in cols:
                migrations.append("ALTER TABLE Bestellungen ADD COLUMN Status TEXT DEFAULT 'offen'")
            if 'Rabatt' not in cols:
                migrations.append("ALTER TABLE Bestellungen ADD COLUMN Rabatt REAL DEFAULT 0.0")
            if 'Mwst_Satz' not in cols:
                migrations.append("ALTER TABLE Bestellungen ADD COLUMN Mwst_Satz REAL DEFAULT 19.0")

            for sql in migrations:
                logging.info(f"Führe Migration aus: {sql}")
                try:
                    c.execute(sql)
                except Exception as e:
                    logging.error(f"Migration fehlgeschlagen: {e}")

            # Stelle sicher, dass bestehende Reihen sinnvolle Defaults haben
            if 'Status' in cols or any('Status' in s for s in migrations):
                try:
                    c.execute("UPDATE Bestellungen SET Status = 'offen' WHERE Status IS NULL")
                except Exception:
                    pass

            conn.commit()
        except sqlite3.Error as e:
            logging.error(f"Fehler bei Schema-Prüfung: {e}")
        finally:
            conn.close()
        print("Schema-Prüfung abgeschlossen.")
        return

    print(f"Erstelle neue Datenbank '{DB_NAME}' mit korrigierten Daten...")
    conn = connect_db()
    c = conn.cursor()

    # Tabellen erstellen (Schema aus deiner SQL-Datei)
    schema = """
    CREATE TABLE IF NOT EXISTS Kunden (
        KundeID INTEGER PRIMARY KEY AUTOINCREMENT,
        Name TEXT NOT NULL,
        Adresse TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS Lieferanten (
        LieferantID INTEGER PRIMARY KEY AUTOINCREMENT,
        Name TEXT NOT NULL,
        Kontakt TEXT,
        Lieferzeit INTEGER
    );

    CREATE TABLE IF NOT EXISTS Produkte (
        ProduktID INTEGER PRIMARY KEY AUTOINCREMENT,
        Produktname TEXT NOT NULL,
        Preis REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS Bestellungen (
        BestellID INTEGER PRIMARY KEY AUTOINCREMENT,
        KundeID INTEGER NOT NULL,
        Bestelldatum DATE NOT NULL,
        Status TEXT DEFAULT 'offen' CHECK (Status IN ('offen', 'versendet', 'geliefert')),
        Rabatt REAL DEFAULT 0.0,
        Mwst_Satz REAL DEFAULT 19.0,
        FOREIGN KEY (KundeID) REFERENCES Kunden(KundeID)
    );

    CREATE TABLE IF NOT EXISTS Bestellpositionen (
        PositionID INTEGER PRIMARY KEY AUTOINCREMENT,
        BestellID INTEGER NOT NULL,
        ProduktID INTEGER NOT NULL,
        Menge INTEGER NOT NULL,
        FOREIGN KEY (BestellID) REFERENCES Bestellungen(BestellID),
        FOREIGN KEY (ProduktID) REFERENCES Produkte(ProduktID)
    );

    CREATE TABLE IF NOT EXISTS Lagerbestand (
        LagerID INTEGER PRIMARY KEY AUTOINCREMENT,
        ProduktID INTEGER NOT NULL,
        Menge INTEGER NOT NULL,
        LieferantID INTEGER NOT NULL,
        Mindestbestand INTEGER DEFAULT 5,
        FOREIGN KEY (ProduktID) REFERENCES Produkte(ProduktID),
        FOREIGN KEY (LieferantID) REFERENCES Lieferanten(LieferantID)
    );
    """
    c.executescript(schema)

    # Trigger separat erstellen
    triggers = """
    CREATE TRIGGER IF NOT EXISTS nach_bestellung_abziehen
    AFTER INSERT ON Bestellpositionen
    FOR EACH ROW
    BEGIN
        UPDATE Lagerbestand
        SET Menge = Menge - NEW.Menge
        WHERE ProduktID = NEW.ProduktID;
    END;

    CREATE TRIGGER IF NOT EXISTS nach_menge_aenderung_gutschreiben
    AFTER UPDATE OF Menge ON Bestellpositionen
    FOR EACH ROW
    WHEN NEW.Menge > OLD.Menge
    BEGIN
        UPDATE Lagerbestand
        SET Menge = Menge - (NEW.Menge - OLD.Menge)
        WHERE ProduktID = NEW.ProduktID;
    END;

    CREATE TRIGGER IF NOT EXISTS nach_menge_reduktion_gutschreiben
    AFTER UPDATE OF Menge ON Bestellpositionen
    FOR EACH ROW
    WHEN NEW.Menge < OLD.Menge
    BEGIN
        UPDATE Lagerbestand
        SET Menge = Menge + (OLD.Menge - NEW.Menge)
        WHERE ProduktID = NEW.ProduktID;
    END;
    """
    c.executescript(triggers)

    # Korrigierte Beispieldaten einfügen
    # Kunden
    c.execute("INSERT INTO Kunden (Name, Adresse) VALUES ('Max Mustermann', 'Musterstraße 1')")
    c.execute("INSERT INTO Kunden (Name, Adresse) VALUES ('Anna Schmidt', 'Hauptstraße 10')")
    c.execute("INSERT INTO Kunden (Name, Adresse) VALUES ('Lisa Müller', 'Beispielweg 42')")
    
    # Lieferanten
    c.execute("INSERT INTO Lieferanten (Name, Kontakt, Lieferzeit) VALUES ('TechGroßhandel GmbH', 'max@techgross.de', 3)")
    c.execute("INSERT INTO Lieferanten (Name, Kontakt, Lieferzeit) VALUES ('ElektroPartner AG', 'vertrieb@elektropartner.de', 5)")

    # Produkte (IDs werden 1, 2, 3 sein)
    c.execute("INSERT INTO Produkte (Produktname, Preis) VALUES ('Laptop', 999.99)")
    c.execute("INSERT INTO Produkte (Produktname, Preis) VALUES ('Smartphone', 699.99)")
    c.execute("INSERT INTO Produkte (Produktname, Preis) VALUES ('Kopfhörer', 149.99)")

    # Lagerbestand (Bezug auf Produkt-IDs 1, 2, 3)
    c.execute("INSERT INTO Lagerbestand (ProduktID, Menge, LieferantID, Mindestbestand) VALUES (1, 10, 1, 3)")
    c.execute("INSERT INTO Lagerbestand (ProduktID, Menge, LieferantID, Mindestbestand) VALUES (2, 15, 2, 5)")
    c.execute("INSERT INTO Lagerbestand (ProduktID, Menge, LieferantID, Mindestbestand) VALUES (3, 20, 1, 10)")

    # Bestellungen
    c.execute("INSERT INTO Bestellungen (KundeID, Bestelldatum) VALUES (1, '2025-12-01')") # ID 1
    
    # Bestellpositionen (Bezug auf Bestellung 1 und Produkte 1, 3)
    c.execute("INSERT INTO Bestellpositionen (BestellID, ProduktID, Menge) VALUES (1, 1, 1)")
    c.execute("INSERT INTO Bestellpositionen (BestellID, ProduktID, Menge) VALUES (1, 3, 2)")

    conn.commit()
    conn.close()
    print("Datenbank erfolgreich initialisiert.")
    logging.info("Neue Datenbank initialisiert")

def print_table(headers, rows):
    """Hilfsfunktion für schöne Tabellenausgabe"""
    if not rows:
        print("Keine Daten vorhanden.")
        return
    
    # Spaltenbreiten berechnen
    widths = [len(h) for h in headers]
    for row in rows:
        for i, val in enumerate(row):
            widths[i] = max(widths[i], len(str(val)))
            
    # Formatstring erstellen
    fmt = " | ".join([f"{{:<{w}}}" for w in widths])
    
    print("-" * (sum(widths) + 3 * (len(headers) - 1)))
    print(fmt.format(*headers))
    print("-" * (sum(widths) + 3 * (len(headers) - 1)))
    for row in rows:
        print(fmt.format(*[str(v) for v in row]))
    print("-" * (sum(widths) + 3 * (len(headers) - 1)))

def list_kunden():
    try:
        conn = connect_db()
        c = conn.cursor()
        c.execute("SELECT KundeID, Name, Adresse FROM Kunden")
        rows = c.fetchall()
        conn.close()
        print("\n--- Kundenliste ---")
        print_table(["ID", "Name", "Adresse"], rows)
    except sqlite3.Error as e:
        logging.error(f"Fehler beim Abrufen der Kundenliste: {e}")
        print(f"Fehler beim Abrufen der Kunden: {e}")

def list_produkte():
    try:
        conn = connect_db()
        c = conn.cursor()
        c.execute("SELECT P.ProduktID, P.Produktname, P.Preis, COALESCE(L.Menge, 0) as Menge FROM Produkte P LEFT JOIN Lagerbestand L ON P.ProduktID = L.ProduktID")
        rows = c.fetchall()
        conn.close()
        print("\n--- Produktliste & Bestand ---")
        print_table(["ID", "Name", "Preis", "Lagerbestand"], rows)
    except sqlite3.Error as e:
        logging.error(f"Fehler beim Abrufen der Produktliste: {e}")
        print(f"Fehler beim Abrufen der Produkte: {e}")

def neuer_kunde():
    print("\n--- Neuer Kunde ---")
    try:
        name = input("Name: ").strip()
        if not name:
            print("Fehler: Name darf nicht leer sein.")
            return
        
        adresse = input("Adresse: ").strip()
        if not adresse:
            print("Fehler: Adresse darf nicht leer sein.")
            return
        
        conn = connect_db()
        c = conn.cursor()
        c.execute("INSERT INTO Kunden (Name, Adresse) VALUES (?, ?)", (name, adresse))
        conn.commit()
        kunde_id = c.lastrowid
        logging.info(f"Neuer Kunde hinzugefügt: {name} (ID: {kunde_id})")
        print(f"Kunde '{name}' hinzugefügt (ID: {kunde_id}).")
        conn.close()
    except sqlite3.Error as e:
        logging.error(f"Fehler beim Hinzufügen des Kunden: {e}")
        print(f"Datenbankfehler: {e}")
    except Exception as e:
        logging.error(f"Unerwarteter Fehler: {e}")
        print(f"Fehler: {e}")

def neues_produkt():
    print("\n--- Neues Produkt ---")
    try:
        name = input("Produktname: ").strip()
        if not name:
            print("Fehler: Produktname darf nicht leer sein.")
            return
        
        preis_input = input("Preis: ").replace(',', '.').strip()
        try:
            preis = float(preis_input)
            if preis < 0:
                print("Fehler: Preis kann nicht negativ sein.")
                return
        except ValueError:
            print("Fehler: Ungültiger Preis. Bitte eine Dezimalzahl eingeben.")
            return
        
        conn = connect_db()
        c = conn.cursor()
        c.execute("INSERT INTO Produkte (Produktname, Preis) VALUES (?, ?)", (name, preis))
        prod_id = c.lastrowid
        
        # Initialen Lagerbestand anlegen
        print("Initialer Lagerbestand:")
        try:
            menge = int(input("Menge: ").strip())
            if menge < 0:
                print("Fehler: Menge kann nicht negativ sein.")
                conn.close()
                return
            
            min_bestand = int(input("Mindestbestand: ").strip())
            if min_bestand < 0:
                print("Fehler: Mindestbestand kann nicht negativ sein.")
                conn.close()
                return
            
            # Lieferant wählen
            c.execute("SELECT LieferantID, Name FROM Lieferanten")
            lieferanten = c.fetchall()
            if not lieferanten:
                print("Fehler: Kein Lieferant vorhanden. Bitte erst einen Lieferanten anlegen.")
                conn.close()
                return
            
            print_table(["ID", "Name"], lieferanten)
            lief_id = int(input("Lieferant ID: ").strip())
            
            # Lieferant validieren
            c.execute("SELECT LieferantID FROM Lieferanten WHERE LieferantID = ?", (lief_id,))
            if not c.fetchone():
                print("Fehler: Lieferant-ID nicht gefunden.")
                conn.close()
                return
            
            c.execute("INSERT INTO Lagerbestand (ProduktID, Menge, LieferantID, Mindestbestand) VALUES (?, ?, ?, ?)",
                      (prod_id, menge, lief_id, min_bestand))
        except ValueError:
            print("Fehler bei der Eingabe, Produkt wurde ohne Lagerbestand angelegt.")
            logging.warning(f"Produkt {name} ohne Lagerbestand angelegt")
            
        conn.commit()
        logging.info(f"Neues Produkt hinzugefügt: {name} (ID: {prod_id})")
        print(f"Produkt '{name}' hinzugefügt.")
        conn.close()
    except sqlite3.Error as e:
        logging.error(f"Fehler beim Hinzufügen des Produkts: {e}")
        print(f"Datenbankfehler: {e}")
    except Exception as e:
        logging.error(f"Unerwarteter Fehler: {e}")
        print(f"Fehler: {e}")

def neue_bestellung():
    print("\n--- Neue Bestellung aufgeben ---")
    try:
        # 1. Kunde auswählen
        list_kunden()
        try:
            kunde_id = int(input("Kunden ID eingeben: ").strip())
        except ValueError:
            print("Fehler: Ungültige Kunden-ID.")
            return

        # Bestellung anlegen
        conn = connect_db()
        c = conn.cursor()
        
        # Prüfen ob Kunde existiert
        c.execute("SELECT Name FROM Kunden WHERE KundeID = ?", (kunde_id,))
        kunde = c.fetchone()
        if not kunde:
            print("Fehler: Kunde nicht gefunden.")
            conn.close()
            return

        datum = datetime.date.today()
        c.execute("INSERT INTO Bestellungen (KundeID, Bestelldatum) VALUES (?, ?)", (kunde_id, datum))
        bestell_id = c.lastrowid
        print(f"Bestellung {bestell_id} für {kunde[0]} am {datum} angelegt.")
        logging.info(f"Neue Bestellung erstellt: ID {bestell_id} für Kunde {kunde[0]}")

        # 2. Positionen hinzufügen (Loop)
        while True:
            list_produkte()
            try:
                prod_id_input = input("Produkt ID eingeben (oder 'f' für fertig): ").strip()
                if prod_id_input.lower() == 'f':
                    break
                
                prod_id = int(prod_id_input)
                
                # Preis und Name holen für Bestätigung
                c.execute("SELECT Produktname FROM Produkte WHERE ProduktID = ?", (prod_id,))
                prod = c.fetchone()
                if not prod:
                    print("Fehler: Produkt nicht gefunden.")
                    continue
                
                # Verfügbaren Bestand prüfen
                c.execute("SELECT Menge FROM Lagerbestand WHERE ProduktID = ?", (prod_id,))
                bestand = c.fetchone()
                verfuegbar = bestand[0] if bestand else 0
                
                menge_input = input(f"Menge für '{prod[0]}' (Verfügbar: {verfuegbar}): ").strip()
                menge = int(menge_input)
                
                if menge <= 0:
                    print("Fehler: Menge muss größer als 0 sein.")
                    continue
                
                # BESTANDS-PRÜFUNG
                if menge > verfuegbar:
                    print(f"Fehler: Nicht genug Bestand! Verfügbar: {verfuegbar}, angefordert: {menge}")
                    logging.warning(f"Bestellung abgelehnt: Nicht genug Bestand für Produkt {prod_id}")
                    continue
                
                c.execute("INSERT INTO Bestellpositionen (BestellID, ProduktID, Menge) VALUES (?, ?, ?)", 
                          (bestell_id, prod_id, menge))
                print(f"✓ {menge}x {prod[0]} zur Bestellung hinzugefügt.")
                logging.info(f"Bestellposition hinzugefügt: {menge}x {prod[0]} zu Bestellung {bestell_id}")
                
            except ValueError:
                print("Fehler: Ungültige Eingabe. Bitte eine Zahl eingeben.")
            except sqlite3.Error as e:
                logging.error(f"Datenbankfehler: {e}")
                print(f"Datenbankfehler: {e}")

        # 3. Rabatt eingeben
        try:
            rabatt_input = input("\nRabatt in % eingeben (Standard: 0): ").strip()
            if rabatt_input:
                rabatt = float(rabatt_input.replace(',', '.'))
                if rabatt < 0 or rabatt > 100:
                    print("Fehler: Rabatt muss zwischen 0 und 100% liegen. Setze auf 0.")
                    rabatt = 0.0
            else:
                rabatt = 0.0
        except ValueError:
            print("Fehler: Ungültiger Rabatt. Setze auf 0.")
            rabatt = 0.0
        
        # 4. MwSt-Satz eingeben
        try:
            mwst_input = input("MwSt-Satz in % eingeben (Standard: 19): ").strip()
            if mwst_input:
                mwst = float(mwst_input.replace(',', '.'))
                if mwst < 0 or mwst > 100:
                    print("Fehler: MwSt muss zwischen 0 und 100% liegen. Setze auf 19.")
                    mwst = 19.0
            else:
                mwst = 19.0
        except ValueError:
            print("Fehler: Ungültige MwSt. Setze auf 19.")
            mwst = 19.0

        # Rabatt und MwSt speichern
        c.execute("UPDATE Bestellungen SET Rabatt = ?, Mwst_Satz = ? WHERE BestellID = ?", 
                 (rabatt, mwst, bestell_id))

        conn.commit()
        print(f"✓ Bestellung {bestell_id} abgeschlossen.")
        print(f"  Rabatt: {rabatt:.2f}% | MwSt: {mwst:.2f}%")
        logging.info(f"Bestellung {bestell_id} abgeschlossen - Rabatt: {rabatt}%, MwSt: {mwst}%")
        conn.close()
    except Exception as e:
        logging.error(f"Fehler in neue_bestellung(): {e}")
        print(f"Fehler: {e}")

def zeige_rechnung():
    print("\n--- Rechnung anzeigen ---")
    try:
        bestell_id = int(input("Bestell-ID eingeben: ").strip())
    except ValueError:
        print("Fehler: Ungültige Bestell-ID.")
        return

    try:
        conn = connect_db()
        c = conn.cursor()

        # Bestelldaten und Kunde
        c.execute("""
            SELECT B.Bestelldatum, K.Name, K.Adresse, B.Status, B.Rabatt, B.Mwst_Satz
            FROM Bestellungen B 
            JOIN Kunden K ON B.KundeID = K.KundeID 
            WHERE B.BestellID = ?
        """, (bestell_id,))
        bestellung = c.fetchone()

        if not bestellung:
            print("Fehler: Bestellung nicht gefunden.")
            conn.close()
            return

        datum, kunde_name, adresse, status, rabatt, mwst_satz = bestellung

        print("\n" + "="*60)
        print(f"RECHNUNG für Bestellung Nr. {bestell_id}")
        print(f"Datum: {datum}")
        print(f"Status: {status.upper()}")
        print(f"Kunde: {kunde_name}")
        print(f"       {adresse}")
        print("="*60)

        # Positionen
        c.execute("""
            SELECT BP.PositionID, P.Produktname, P.Preis, BP.Menge
            FROM Bestellpositionen BP
            JOIN Produkte P ON BP.ProduktID = P.ProduktID
            WHERE BP.BestellID = ?
        """, (bestell_id,))
        
        positionen = c.fetchall()
        
        if not positionen:
            print("Keine Positionen in dieser Bestellung.")
            conn.close()
            return
        
        # Berechnung
        subtotal = 0.0
        
        print(f"{'ID':<4} | {'Produkt':<20} | {'Menge':<5} | {'Einzel':<8} | {'Gesamt':<10}")
        print("-" * 65)
        
        for pos in positionen:
            pos_id, name, preis, menge = pos
            zeilen_summe = preis * menge
            subtotal += zeilen_summe
            print(f"{pos_id:<4} | {name:<20} | {menge:<5} | {preis:>8.2f} | {zeilen_summe:>10.2f}")
            
        print("-" * 65)
        print(f"{'Subtotal:':<50} {subtotal:>10.2f} €")
        
        # Rabatt berechnen
        rabatt_betrag = subtotal * (rabatt / 100.0)
        netto = subtotal - rabatt_betrag
        
        if rabatt > 0:
            print(f"{'Rabatt (' + str(rabatt) + '%):':<50} -{rabatt_betrag:>9.2f} €")
        
        print(f"{'Netto:':<50} {netto:>10.2f} €")
        
        # MwSt berechnen
        mwst_betrag = netto * (mwst_satz / 100.0)
        gesamtsumme = netto + mwst_betrag
        
        print(f"{'MwSt (' + str(mwst_satz) + '%):':<50} {mwst_betrag:>10.2f} €")
        print("="*60)
        print(f"{'GESAMTSUMME:':<50} {gesamtsumme:>10.2f} €")
        print("="*60 + "\n")
        
        logging.info(f"Rechnung angezeigt: Bestellung {bestell_id} - Summe: {gesamtsumme:.2f}€")
        conn.close()
    except sqlite3.Error as e:
        logging.error(f"Datenbankfehler beim Abrufen der Rechnung: {e}")
        print(f"Datenbankfehler: {e}")
    except Exception as e:
        logging.error(f"Fehler in zeige_rechnung(): {e}")
        print(f"Fehler: {e}")

def lagerbestand_korrigieren():
    print("\n--- Lagerbestand korrigieren (Inventur) ---")
    try:
        list_produkte()
        prod_id = int(input("Produkt ID für Korrektur eingeben: ").strip())
    except ValueError:
        print("Fehler: Ungültige Eingabe.")
        return

    try:
        conn = connect_db()
        c = conn.cursor()

        # Prüfen, ob Eintrag existiert
        c.execute("SELECT P.Produktname, L.Menge FROM Produkte P JOIN Lagerbestand L ON P.ProduktID = L.ProduktID WHERE P.ProduktID = ?", (prod_id,))
        result = c.fetchone()

        if not result:
            print("Fehler: Kein Lagerbestand für dieses Produkt gefunden.")
            conn.close()
            return

        name, aktuelle_menge = result
        print(f"Produkt: {name}")
        print(f"Aktueller Bestand im System: {aktuelle_menge}")

        try:
            neue_menge = int(input("Neuer tatsächlicher Bestand: ").strip())
            if neue_menge < 0:
                print("Fehler: Bestand kann nicht negativ sein.")
                conn.close()
                return
        except ValueError:
            print("Fehler: Ungültige Eingabe.")
            conn.close()
            return

        c.execute("UPDATE Lagerbestand SET Menge = ? WHERE ProduktID = ?", (neue_menge, prod_id))
        conn.commit()
        logging.info(f"Lagerbestand korrigiert für Produkt {name}: {aktuelle_menge} → {neue_menge}")
        print(f"✓ Bestand für '{name}' auf {neue_menge} korrigiert.")
        conn.close()
    except sqlite3.Error as e:
        logging.error(f"Datenbankfehler beim Korrigieren des Lagerbestands: {e}")
        print(f"Datenbankfehler: {e}")
    except Exception as e:
        logging.error(f"Fehler in lagerbestand_korrigieren(): {e}")
        print(f"Fehler: {e}")

def list_lieferanten():
    try:
        conn = connect_db()
        c = conn.cursor()
        c.execute("SELECT LieferantID, Name, Kontakt, Lieferzeit FROM Lieferanten")
        rows = c.fetchall()
        conn.close()
        print("\n--- Lieferantenliste ---")
        print_table(["ID", "Name", "Kontakt", "Lieferzeit (Tage)"], rows)
    except sqlite3.Error as e:
        logging.error(f"Fehler beim Abrufen der Lieferantenliste: {e}")
        print(f"Fehler beim Abrufen der Lieferanten: {e}")

def neuer_lieferant():
    print("\n--- Neuer Lieferant ---")
    try:
        name = input("Firmenname: ").strip()
        if not name:
            print("Fehler: Firmenname darf nicht leer sein.")
            return
        
        kontakt = input("Kontakt (Email/Tel): ").strip()
        if not kontakt:
            print("Fehler: Kontakt darf nicht leer sein.")
            return
        
        try:
            zeit = int(input("Lieferzeit in Tagen: ").strip())
            if zeit < 0:
                print("Fehler: Lieferzeit kann nicht negativ sein.")
                return
        except ValueError:
            print("Fehler: Ungültige Lieferzeit, setze Standardwert 0.")
            zeit = 0
        
        conn = connect_db()
        c = conn.cursor()
        c.execute("INSERT INTO Lieferanten (Name, Kontakt, Lieferzeit) VALUES (?, ?, ?)", (name, kontakt, zeit))
        conn.commit()
        lieferant_id = c.lastrowid
        logging.info(f"Neuer Lieferant hinzugefügt: {name} (ID: {lieferant_id})")
        print(f"Lieferant '{name}' hinzugefügt.")
        conn.close()
    except sqlite3.Error as e:
        logging.error(f"Fehler beim Hinzufügen des Lieferanten: {e}")
        print(f"Datenbankfehler: {e}")
    except Exception as e:
        logging.error(f"Unerwarteter Fehler: {e}")
        print(f"Fehler: {e}")

def bestellposition_aendern():
    """Ändert die Menge einer Bestellposition oder löscht sie."""
    print("\n--- Bestellposition ändern/löschen ---")
    try:
        bestell_id = int(input("Bestell-ID eingeben: ").strip())
        conn = connect_db()
        c = conn.cursor()
        
        # Zeige alle Positionen der Bestellung
        c.execute("""
            SELECT BP.PositionID, P.Produktname, BP.Menge, L.Menge as Bestand
            FROM Bestellpositionen BP
            JOIN Produkte P ON BP.ProduktID = P.ProduktID
            LEFT JOIN Lagerbestand L ON P.ProduktID = L.ProduktID
            WHERE BP.BestellID = ?
        """, (bestell_id,))
        
        positionen = c.fetchall()
        if not positionen:
            print("Keine Positionen für diese Bestellung gefunden.")
            conn.close()
            return
        
        print("\n--- Positionen in Bestellung ---")
        print_table(["Pos-ID", "Produkt", "Menge", "Lagerbestand"], positionen)
        
        try:
            pos_id = int(input("\nPosition ID zum Ändern eingeben: ").strip())
        except ValueError:
            print("Fehler: Ungültige Position ID.")
            conn.close()
            return
        
        # Prüfe, ob Position existiert
        c.execute("""
            SELECT BP.PositionID, BP.BestellID, BP.ProduktID, BP.Menge, P.Produktname
            FROM Bestellpositionen BP
            JOIN Produkte P ON BP.ProduktID = P.ProduktID
            WHERE BP.PositionID = ?
        """, (pos_id,))
        
        position = c.fetchone()
        if not position:
            print("Fehler: Position nicht gefunden.")
            conn.close()
            return
        
        pos_id_val, bestell_id_val, prod_id, alte_menge, prod_name = position
        
        print(f"\nAktuelle Position: {prod_name}, Menge: {alte_menge}")
        print("Optionen:")
        print("1. Menge ändern")
        print("2. Position löschen")
        
        option = input("Auswahl (1/2): ").strip()
        
        if option == '1':
            try:
                neue_menge = int(input("Neue Menge eingeben: ").strip())
                if neue_menge < 0:
                    print("Fehler: Menge kann nicht negativ sein.")
                    conn.close()
                    return
                
                if neue_menge == 0:
                    print("Menge 0 bedeutet: Position wird gelöscht!")
                    bestaetigung = input("Bestätigen? (j/n): ").strip().lower()
                    if bestaetigung != 'j':
                        conn.close()
                        return
                    c.execute("DELETE FROM Bestellpositionen WHERE PositionID = ?", (pos_id,))
                    # Bestand zurückbuchen
                    c.execute("UPDATE Lagerbestand SET Menge = Menge + ? WHERE ProduktID = ?", 
                             (alte_menge, prod_id))
                    logging.info(f"Bestellposition {pos_id} gelöscht. Bestand +{alte_menge}")
                    print(f"✓ Position gelöscht. Bestand von '{prod_name}' +{alte_menge}")
                else:
                    # Bestandsprüfung bei Erhöhung
                    if neue_menge > alte_menge:
                        c.execute("SELECT Menge FROM Lagerbestand WHERE ProduktID = ?", (prod_id,))
                        bestand = c.fetchone()
                        verfuegbar = bestand[0] if bestand else 0
                        menge_erhoehung = neue_menge - alte_menge
                        
                        if menge_erhoehung > verfuegbar:
                            print(f"Fehler: Nicht genug Bestand! Verfügbar: {verfuegbar}, angefordert: {menge_erhoehung}")
                            conn.close()
                            return
                    
                    c.execute("UPDATE Bestellpositionen SET Menge = ? WHERE PositionID = ?", 
                             (neue_menge, pos_id))
                    logging.info(f"Bestellposition {pos_id} geändert: {alte_menge} → {neue_menge}")
                    print(f"✓ Menge geändert: {alte_menge} → {neue_menge}")
                    
            except ValueError:
                print("Fehler: Ungültige Eingabe.")
                conn.close()
                return
                
        elif option == '2':
            bestaetigung = input(f"Position '{prod_name}' ({alte_menge}x) wirklich löschen? (j/n): ").strip().lower()
            if bestaetigung == 'j':
                c.execute("DELETE FROM Bestellpositionen WHERE PositionID = ?", (pos_id,))
                # Bestand zurückbuchen
                c.execute("UPDATE Lagerbestand SET Menge = Menge + ? WHERE ProduktID = ?", 
                         (alte_menge, prod_id))
                logging.info(f"Bestellposition {pos_id} gelöscht. Bestand +{alte_menge}")
                print(f"✓ Position gelöscht. Bestand von '{prod_name}' +{alte_menge}")
            else:
                print("Abgebrochen.")
                conn.close()
                return
        else:
            print("Ungültige Auswahl.")
            conn.close()
            return
        
        conn.commit()
        conn.close()
    except sqlite3.Error as e:
        logging.error(f"Datenbankfehler: {e}")
        print(f"Datenbankfehler: {e}")
    except Exception as e:
        logging.error(f"Fehler in bestellposition_aendern(): {e}")
        print(f"Fehler: {e}")

def bestellung_status_aendern():
    """Ändert den Status einer Bestellung."""
    print("\n--- Bestellstatus ändern ---")
    try:
        bestell_id = int(input("Bestell-ID eingeben: ").strip())
        
        conn = connect_db()
        c = conn.cursor()
        
        # Zeige aktuelle Status
        c.execute("SELECT Status FROM Bestellungen WHERE BestellID = ?", (bestell_id,))
        result = c.fetchone()
        
        if not result:
            print("Fehler: Bestellung nicht gefunden.")
            conn.close()
            return
        
        aktueller_status = result[0]
        print(f"\nAktueller Status: {aktueller_status.upper()}")
        print("\nVerfügbare Status:")
        print("1. offen")
        print("2. versendet")
        print("3. geliefert")
        
        option = input("Neuer Status (1/2/3): ").strip()
        
        status_map = {'1': 'offen', '2': 'versendet', '3': 'geliefert'}
        neuer_status = status_map.get(option)
        
        if not neuer_status:
            print("Ungültige Auswahl.")
            conn.close()
            return
        
        if neuer_status == aktueller_status:
            print("Status ist bereits auf diesem Wert.")
            conn.close()
            return
        
        c.execute("UPDATE Bestellungen SET Status = ? WHERE BestellID = ?", 
                 (neuer_status, bestell_id))
        conn.commit()
        logging.info(f"Bestellung {bestell_id}: Status geändert {aktueller_status} → {neuer_status}")
        print(f"✓ Bestellstatus geändert: {aktueller_status} → {neuer_status}")
        conn.close()
        
    except ValueError:
        print("Fehler: Ungültige Eingabe.")
    except sqlite3.Error as e:
        logging.error(f"Datenbankfehler: {e}")
        print(f"Datenbankfehler: {e}")
    except Exception as e:
        logging.error(f"Fehler in bestellung_status_aendern(): {e}")
        print(f"Fehler: {e}")

def rabatt_mwst_aendern():
    """Ändert Rabatt und MwSt-Satz einer existierenden Bestellung."""
    print("\n--- Rabatt & MwSt ändern ---")
    try:
        bestell_id = int(input("Bestell-ID eingeben: ").strip())
        
        conn = connect_db()
        c = conn.cursor()
        
        # Zeige aktuelle Werte
        c.execute("SELECT Rabatt, Mwst_Satz FROM Bestellungen WHERE BestellID = ?", (bestell_id,))
        result = c.fetchone()
        
        if not result:
            print("Fehler: Bestellung nicht gefunden.")
            conn.close()
            return
        
        alter_rabatt, alter_mwst = result
        print(f"\nAktuelle Werte:")
        print(f"  Rabatt: {alter_rabatt:.2f}%")
        print(f"  MwSt-Satz: {alter_mwst:.2f}%")
        
        # Neuen Rabatt eingeben
        try:
            rabatt_input = input("\nNeuen Rabatt eingeben (%) - (Enter für unverändert): ").strip()
            if rabatt_input:
                neuer_rabatt = float(rabatt_input.replace(',', '.'))
                if neuer_rabatt < 0 or neuer_rabatt > 100:
                    print("Fehler: Rabatt muss zwischen 0 und 100% liegen.")
                    conn.close()
                    return
            else:
                neuer_rabatt = alter_rabatt
        except ValueError:
            print("Fehler: Ungültiger Rabatt.")
            conn.close()
            return
        
        # Neuen MwSt-Satz eingeben
        try:
            mwst_input = input("Neuen MwSt-Satz eingeben (%) - (Enter für unverändert): ").strip()
            if mwst_input:
                neuer_mwst = float(mwst_input.replace(',', '.'))
                if neuer_mwst < 0 or neuer_mwst > 100:
                    print("Fehler: MwSt muss zwischen 0 und 100% liegen.")
                    conn.close()
                    return
            else:
                neuer_mwst = alter_mwst
        except ValueError:
            print("Fehler: Ungültiger MwSt-Satz.")
            conn.close()
            return
        
        # Bestätigung
        print(f"\nÄnderungen:")
        if neuer_rabatt != alter_rabatt:
            print(f"  Rabatt: {alter_rabatt:.2f}% → {neuer_rabatt:.2f}%")
        if neuer_mwst != alter_mwst:
            print(f"  MwSt: {alter_mwst:.2f}% → {neuer_mwst:.2f}%")
        
        bestaetigung = input("Bestätigen? (j/n): ").strip().lower()
        if bestaetigung != 'j':
            print("Abgebrochen.")
            conn.close()
            return
        
        c.execute("UPDATE Bestellungen SET Rabatt = ?, Mwst_Satz = ? WHERE BestellID = ?", 
                 (neuer_rabatt, neuer_mwst, bestell_id))
        conn.commit()
        logging.info(f"Bestellung {bestell_id}: Rabatt {alter_rabatt}% → {neuer_rabatt}%, MwSt {alter_mwst}% → {neuer_mwst}%")
        print(f"✓ Rabatt und MwSt-Satz aktualisiert.")
        conn.close()
        
    except ValueError:
        print("Fehler: Ungültige Eingabe.")
    except sqlite3.Error as e:
        logging.error(f"Datenbankfehler: {e}")
        print(f"Datenbankfehler: {e}")
    except Exception as e:
        logging.error(f"Fehler in rabatt_mwst_aendern(): {e}")
        print(f"Fehler: {e}")

def suche_kunde():
    """Sucht einen Kunden nach Name."""
    print("\n--- Kundensuche ---")
    try:
        suchbegriff = input("Kundennamen eingeben (Teilsuche): ").strip()
        if not suchbegriff:
            print("Fehler: Suchbegriff darf nicht leer sein.")
            return
        
        conn = connect_db()
        c = conn.cursor()
        c.execute("SELECT KundeID, Name, Adresse FROM Kunden WHERE Name LIKE ?", 
                 (f"%{suchbegriff}%",))
        kunden = c.fetchall()
        conn.close()
        
        if not kunden:
            print(f"Keine Kunden gefunden, die '{suchbegriff}' enthalten.")
            return
        
        print(f"\n--- Suchergebnisse ({len(kunden)} gefunden) ---")
        print_table(["ID", "Name", "Adresse"], kunden)
        
    except sqlite3.Error as e:
        logging.error(f"Datenbankfehler: {e}")
        print(f"Datenbankfehler: {e}")
    except Exception as e:
        logging.error(f"Fehler in suche_kunde(): {e}")
        print(f"Fehler: {e}")

def suche_produkt():
    """Sucht ein Produkt nach Name."""
    print("\n--- Produktsuche ---")
    try:
        suchbegriff = input("Produktnamen eingeben (Teilsuche): ").strip()
        if not suchbegriff:
            print("Fehler: Suchbegriff darf nicht leer sein.")
            return
        
        conn = connect_db()
        c = conn.cursor()
        c.execute("""
            SELECT P.ProduktID, P.Produktname, P.Preis, COALESCE(L.Menge, 0) as Bestand
            FROM Produkte P
            LEFT JOIN Lagerbestand L ON P.ProduktID = L.ProduktID
            WHERE P.Produktname LIKE ?
        """, (f"%{suchbegriff}%",))
        produkte = c.fetchall()
        conn.close()
        
        if not produkte:
            print(f"Keine Produkte gefunden, die '{suchbegriff}' enthalten.")
            return
        
        print(f"\n--- Suchergebnisse ({len(produkte)} gefunden) ---")
        print_table(["ID", "Name", "Preis", "Bestand"], produkte)
        
    except sqlite3.Error as e:
        logging.error(f"Datenbankfehler: {e}")
        print(f"Datenbankfehler: {e}")
    except Exception as e:
        logging.error(f"Fehler in suche_produkt(): {e}")
        print(f"Fehler: {e}")

def bestellverlauf_kunde():
    """Zeigt den Bestellverlauf eines Kunden."""
    print("\n--- Bestellverlauf anzeigen ---")
    try:
        kunde_id = int(input("Kunden-ID eingeben: ").strip())
        
        conn = connect_db()
        c = conn.cursor()
        
        # Kunde existieren?
        c.execute("SELECT Name FROM Kunden WHERE KundeID = ?", (kunde_id,))
        kunde = c.fetchone()
        if not kunde:
            print("Fehler: Kunde nicht gefunden.")
            conn.close()
            return
        
        print(f"\n--- Bestellverlauf für: {kunde[0]} ---")
        
        # Alle Bestellungen des Kunden
        c.execute("""
            SELECT BestellID, Bestelldatum, Status
            FROM Bestellungen
            WHERE KundeID = ?
            ORDER BY Bestelldatum DESC
        """, (kunde_id,))
        
        bestellungen = c.fetchall()
        if not bestellungen:
            print("Keine Bestellungen vorhanden.")
            conn.close()
            return
        
        print_table(["Bestellung-ID", "Datum", "Status"], bestellungen)
        
        # Optional: Details einer Bestellung
        try:
            detail_id = int(input("\nBestellung-ID für Details eingeben (oder 0 zum Abbrechen): ").strip())
            if detail_id > 0:
                c.execute("""
                    SELECT BP.PositionID, P.Produktname, BP.Menge, P.Preis
                    FROM Bestellpositionen BP
                    JOIN Produkte P ON BP.ProduktID = P.ProduktID
                    WHERE BP.BestellID = ?
                """, (detail_id,))
                
                positionen = c.fetchall()
                if positionen:
                    gesamtsumme = sum(pos[2] * pos[3] for pos in positionen)
                    print(f"\n--- Details Bestellung {detail_id} ---")
                    print_table(["Pos-ID", "Produkt", "Menge", "Preis"], positionen)
                    print(f"Gesamtsumme: {gesamtsumme:.2f} €")
        except ValueError:
            pass
        
        conn.close()
        logging.info(f"Bestellverlauf angezeigt für Kunde {kunde_id}")
        
    except ValueError:
        print("Fehler: Ungültige Eingabe.")
    except sqlite3.Error as e:
        logging.error(f"Datenbankfehler: {e}")
        print(f"Datenbankfehler: {e}")
    except Exception as e:
        logging.error(f"Fehler in bestellverlauf_kunde(): {e}")
        print(f"Fehler: {e}")


def preise_bereinigen_menu():
    """Wrapper, um das Bereinigungsskript für Preise auszuführen."""
    print("\n--- Preise bereinigen ---")
    try:
        auswahl = input("Dry-run? (j = nur anzeigen, n = ausführen) [j/n]: ").strip().lower()
        dry = True if auswahl == 'j' or auswahl == '' else False
        print("Starte Bereinigung (Dry-run=" + str(dry) + ")...")
        try:
            import importlib
            cp = importlib.import_module('clean_prices')
            importlib.reload(cp)
            changed = cp.clean_prices(dry_run=dry)
            print(f"Bereinigung abgeschlossen. Geänderte Einträge: {changed}")
            logging.info(f"Preise bereinigt (dry={dry}) - geänderte Einträge: {changed}")
        except ModuleNotFoundError:
            print("Fehler: Modul 'clean_prices.py' nicht gefunden. Stelle sicher, dass die Datei im Projektordner liegt.")
            logging.error("clean_prices.py nicht gefunden")
        except Exception as e:
            print(f"Fehler beim Ausführen der Bereinigung: {e}")
            logging.error(f"Fehler in preise_bereinigen_menu(): {e}")
    except Exception as e:
        logging.error(f"Fehler in preise_bereinigen_menu(): {e}")
        print(f"Fehler: {e}")

def pruefe_mindestbestaende():
    """Prüft Mindestbestände und meldet Nachbestellungen."""
    print("\n--- Mindestbestand Prüfung ---")
    try:
        conn = connect_db()
        c = conn.cursor()
        
        c.execute("""
            SELECT L.LagerID, P.ProduktID, P.Produktname, L.Menge, L.Mindestbestand, 
                   Li.Name as Lieferant, Li.Lieferzeit
            FROM Lagerbestand L
            JOIN Produkte P ON L.ProduktID = P.ProduktID
            JOIN Lieferanten Li ON L.LieferantID = Li.LieferantID
            WHERE L.Menge <= L.Mindestbestand
            ORDER BY L.Menge ASC
        """)
        
        nachbestellungen = c.fetchall()
        conn.close()
        
        if not nachbestellungen:
            print("✓ Alle Bestände sind im grünen Bereich!")
            return
        
        print(f"\n⚠ {len(nachbestellungen)} Produkt(e) unter Mindestbestand:\n")
        print_table(
            ["ProduktID", "Produkt", "Ist", "Min", "Lieferant", "Lieferzeit"],
            [(n[1], n[2], n[3], n[4], n[5], f"{n[6]}d") for n in nachbestellungen]
        )
        
        print("\n--- Vorgeschlagene Nachbestellungen ---")
        for nachbestellung in nachbestellungen:
            lager_id, prod_id, prod_name, ist_menge, min_menge, lief_name, lief_zeit = nachbestellung
            # Standardmenge: Mindestbestand * 3
            empf_menge = min_menge * 3
            differenz = empf_menge - ist_menge
            
            print(f"• {prod_name}: {differenz} Stück @ {lief_name} (Lieferzeit: {lief_zeit} Tage)")
        
        logging.info(f"Mindestbestand-Prüfung: {len(nachbestellungen)} Produkte unter Minimum")
        
    except sqlite3.Error as e:
        logging.error(f"Datenbankfehler: {e}")
        print(f"Datenbankfehler: {e}")
    except Exception as e:
        logging.error(f"Fehler in pruefe_mindestbestaende(): {e}")
        print(f"Fehler: {e}")

def main_menu():
    def print_menu():
        print("\n=== BESTELLVERWALTUNG ===")
        print("--- ANZEIGEN ---")
        print("1. Kunden anzeigen")
        print("2. Produkte & Bestand anzeigen")
        print("3. Lieferanten anzeigen")
        print("--- SUCHEN ---")
        print("4. Kundensuche")
        print("5. Produktsuche")
        print("--- STAMMDATEN PFLEGE ---")
        print("6. Neuer Kunde")
        print("7. Neuer Lieferant")
        print("8. Neues Produkt")
        print("--- TAGESGESCHÄFT ---")
        print("9. Neue Bestellung")
        print("10. Rechnung anzeigen")
        print("11. Bestellposition ändern/löschen")
        print("12. Bestellstatus ändern")
        print("13. Rabatt & MwSt ändern")
        print("--- REPORTING ---")
        print("14. Bestellverlauf eines Kunden")
        print("15. Mindestbestand Prüfung")
        print("16. Lagerbestand korrigieren (Inventur)")
        print("17. Datenbank sichern (Backup)")
        print("18. Preise bereinigen (clean_prices.py)")
        print("0. Beenden")

    init_db()
    def clear_screen():
        try:
            os.system('cls' if os.name == 'nt' else 'clear')
        except Exception:
            pass

    while True:
        # Bildschirm säubern, Menü oben anzeigen
        clear_screen()
        print_menu()
        wahl = input("Auswahl: ").strip()

        if wahl == '1':
            list_kunden()
        elif wahl == '2':
            list_produkte()
        elif wahl == '3':
            list_lieferanten()
        elif wahl == '4':
            suche_kunde()
        elif wahl == '5':
            suche_produkt()
        elif wahl == '6':
            neuer_kunde()
        elif wahl == '7':
            neuer_lieferant()
        elif wahl == '8':
            neues_produkt()
        elif wahl == '9':
            neue_bestellung()
        elif wahl == '10':
            zeige_rechnung()
        elif wahl == '11':
            bestellposition_aendern()
        elif wahl == '12':
            bestellung_status_aendern()
        elif wahl == '13':
            rabatt_mwst_aendern()
        elif wahl == '14':
            bestellverlauf_kunde()
        elif wahl == '15':
            pruefe_mindestbestaende()
        elif wahl == '16':
            lagerbestand_korrigieren()
        elif wahl == '17':
            datenbank_sichern()
        elif wahl == '18':
            preise_bereinigen_menu()
        elif wahl == '0':
            print("Auf Wiedersehen!")
            break
        else:
            print("Ungültige Auswahl.")

        # Kurze Pause, damit Benutzer Ausgabe lesen kann
        try:
            input("\nDrücke Enter, um zum Menü zurückzukehren...")
        except Exception:
            pass

if __name__ == "__main__":
    main_menu()