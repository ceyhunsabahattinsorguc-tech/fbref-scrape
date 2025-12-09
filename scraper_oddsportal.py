# -*- coding: utf-8 -*-
"""
OddsPortal Scraper
Maç oranlarını (1X2, O/U 2.5) çeker
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')

import pyodbc
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright
import time
import re

CONNECTION_STRING = (
    "DRIVER={SQL Server};"
    "SERVER=195.201.146.224,1433;"
    "DATABASE=FBREF;"
    "UID=sa;"
    "PWD=FbRef2024Str0ng;"
)

# OddsPortal lig URL'leri
ODDSPORTAL_LEAGUES = [
    {"lig_id": 6, "name": "Premier League", "url": "england/premier-league"},
    {"lig_id": 7, "name": "La Liga", "url": "spain/laliga"},
    {"lig_id": 8, "name": "Serie A", "url": "italy/serie-a"},
    {"lig_id": 9, "name": "Ligue 1", "url": "france/ligue-1"},
    {"lig_id": 10, "name": "Bundesliga", "url": "germany/bundesliga"},
    {"lig_id": 4, "name": "Super Lig", "url": "turkey/super-lig"},
    {"lig_id": 11, "name": "Eredivisie", "url": "netherlands/eredivisie"},
    {"lig_id": 12, "name": "Primeira Liga", "url": "portugal/liga-portugal"},
    {"lig_id": 19, "name": "Champions League", "url": "europe/champions-league"},
    {"lig_id": 20, "name": "Europa League", "url": "europe/europa-league"},
    {"lig_id": 21, "name": "Conference League", "url": "europe/europa-conference-league"},
]


def get_db_connection():
    return pyodbc.connect(CONNECTION_STRING)


def get_html(url, timeout=60000):
    """Playwright ile HTML al"""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        page = context.new_page()

        try:
            page.goto(url, timeout=timeout, wait_until='networkidle')
            # OddsPortal dinamik yükleme için bekle
            time.sleep(5)

            # Sayfayı scroll et (lazy loading için)
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(2)

            html = page.content()
        finally:
            browser.close()

        return html


def calculate_probability(odds):
    """Oranı olasılığa çevir"""
    if odds and odds > 0:
        return round(100 / odds, 2)
    return None


def calculate_overround(probs):
    """Overround (bahisçi marjı) hesapla"""
    if all(p for p in probs):
        return round(sum(probs) - 100, 2)
    return None


def parse_odds_page(html, league_name):
    """OddsPortal sayfasından oranları parse et"""
    soup = BeautifulSoup(html, 'html.parser')
    matches = []

    # OddsPortal'ın yeni yapısı - eventRow class'ları
    event_rows = soup.find_all('div', class_=re.compile(r'eventRow'))

    if not event_rows:
        # Alternatif: table yapısını dene
        event_rows = soup.find_all('tr', class_=re.compile(r'deactivate'))

    print(f"  Bulunan maç satırı: {len(event_rows)}")

    for row in event_rows:
        try:
            # Takım isimleri
            teams = row.find_all('a', class_=re.compile(r'participant'))
            if len(teams) < 2:
                teams = row.find_all('span', class_=re.compile(r'participant'))

            if len(teams) >= 2:
                home_team = teams[0].get_text(strip=True)
                away_team = teams[1].get_text(strip=True)
            else:
                continue

            # Tarih
            date_elem = row.find('span', class_=re.compile(r'date'))
            match_date = None
            if date_elem:
                date_text = date_elem.get_text(strip=True)
                # Format: "09 Dec" veya "Today" veya "Tomorrow"
                try:
                    if 'Today' in date_text:
                        match_date = datetime.now().date()
                    elif 'Tomorrow' in date_text:
                        match_date = (datetime.now() + timedelta(days=1)).date()
                    else:
                        # "09 Dec 2025" formatı
                        match_date = datetime.strptime(date_text + ' 2025', '%d %b %Y').date()
                except:
                    match_date = datetime.now().date()

            # Oranlar (1X2)
            odds_elements = row.find_all('span', class_=re.compile(r'odds-value'))
            if not odds_elements:
                odds_elements = row.find_all('td', class_=re.compile(r'odds'))

            odds_1 = None
            odds_x = None
            odds_2 = None

            if len(odds_elements) >= 3:
                try:
                    odds_1 = float(odds_elements[0].get_text(strip=True))
                    odds_x = float(odds_elements[1].get_text(strip=True))
                    odds_2 = float(odds_elements[2].get_text(strip=True))
                except:
                    pass

            if home_team and away_team:
                matches.append({
                    'home_team': home_team,
                    'away_team': away_team,
                    'date': match_date,
                    'odds_1': odds_1,
                    'odds_x': odds_x,
                    'odds_2': odds_2,
                    'prob_1': calculate_probability(odds_1),
                    'prob_x': calculate_probability(odds_x),
                    'prob_2': calculate_probability(odds_2),
                })

        except Exception as e:
            continue

    return matches


def save_odds(conn, match, lig_id):
    """Oranları veritabanına kaydet"""
    cursor = conn.cursor()

    # Olasılıkları hesapla
    prob_1 = calculate_probability(match['odds_1'])
    prob_x = calculate_probability(match['odds_x'])
    prob_2 = calculate_probability(match['odds_2'])
    overround = calculate_overround([prob_1, prob_x, prob_2]) if all([prob_1, prob_x, prob_2]) else None

    # Mevcut kayıt kontrolü
    cursor.execute("""
        SELECT ORAN_ID FROM BAHIS.MAC_ORANLARI
        WHERE LIG_ID = ? AND EV_SAHIBI = ? AND MISAFIR = ? AND MAC_TARIHI = ?
    """, lig_id, match['home_team'], match['away_team'], match['date'])

    row = cursor.fetchone()

    if row:
        # Güncelle
        cursor.execute("""
            UPDATE BAHIS.MAC_ORANLARI
            SET ORAN_1 = ?, ORAN_X = ?, ORAN_2 = ?,
                OLASILIK_1 = ?, OLASILIK_X = ?, OLASILIK_2 = ?,
                OVERROUND_1X2 = ?,
                GUNCELLEME_TARIHI = GETDATE()
            WHERE ORAN_ID = ?
        """, match['odds_1'], match['odds_x'], match['odds_2'],
            prob_1, prob_x, prob_2, overround, row[0])
        conn.commit()
        return row[0], False
    else:
        # Yeni kayıt
        cursor.execute("""
            INSERT INTO BAHIS.MAC_ORANLARI (
                LIG_ID, EV_SAHIBI, MISAFIR, MAC_TARIHI,
                ORAN_1, ORAN_X, ORAN_2,
                OLASILIK_1, OLASILIK_X, OLASILIK_2,
                OVERROUND_1X2, KAYNAK
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OddsPortal')
        """, lig_id, match['home_team'], match['away_team'], match['date'],
            match['odds_1'], match['odds_x'], match['odds_2'],
            prob_1, prob_x, prob_2, overround)
        conn.commit()
        cursor.execute("SELECT @@IDENTITY")
        return cursor.fetchone()[0], True


def match_with_fixture(conn, oran_id, lig_id, home_team, away_team, match_date):
    """FIKSTUR tablosuyla eşleştir"""
    cursor = conn.cursor()

    # Tam eşleşme dene
    cursor.execute("""
        SELECT FIKSTURID FROM FIKSTUR.FIKSTUR
        WHERE LIG_ID = ? AND EVSAHIBI = ? AND MISAFIR = ?
        AND CAST(TARIH AS DATE) = ?
    """, lig_id, home_team, away_team, match_date)

    row = cursor.fetchone()
    if row:
        cursor.execute("""
            UPDATE BAHIS.MAC_ORANLARI SET FIKSTURID = ? WHERE ORAN_ID = ?
        """, row[0], oran_id)
        conn.commit()
        return True

    # Benzer isim araması (LIKE)
    cursor.execute("""
        SELECT FIKSTURID FROM FIKSTUR.FIKSTUR
        WHERE LIG_ID = ? AND EVSAHIBI LIKE ? AND MISAFIR LIKE ?
        AND CAST(TARIH AS DATE) = ?
    """, lig_id, f'%{home_team[:5]}%', f'%{away_team[:5]}%', match_date)

    row = cursor.fetchone()
    if row:
        cursor.execute("""
            UPDATE BAHIS.MAC_ORANLARI SET FIKSTURID = ? WHERE ORAN_ID = ?
        """, row[0], oran_id)
        conn.commit()
        return True

    return False


def main(leagues=None):
    """Ana fonksiyon"""
    if leagues is None:
        leagues = ODDSPORTAL_LEAGUES

    conn = get_db_connection()

    # Schema ve tablo oluştur
    print("BAHIS schema kontrol ediliyor...")
    cursor = conn.cursor()
    try:
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'BAHIS')
                EXEC('CREATE SCHEMA BAHIS')
        """)
        conn.commit()

        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'MAC_ORANLARI' AND schema_id = SCHEMA_ID('BAHIS'))
            CREATE TABLE BAHIS.MAC_ORANLARI (
                ORAN_ID INT IDENTITY(1,1) PRIMARY KEY,
                FIKSTURID INT NULL,
                LIG_ID INT NOT NULL,
                EV_SAHIBI NVARCHAR(100) NOT NULL,
                MISAFIR NVARCHAR(100) NOT NULL,
                MAC_TARIHI DATE NOT NULL,
                ORAN_1 DECIMAL(5,2) NULL,
                ORAN_X DECIMAL(5,2) NULL,
                ORAN_2 DECIMAL(5,2) NULL,
                OLASILIK_1 DECIMAL(5,2) NULL,
                OLASILIK_X DECIMAL(5,2) NULL,
                OLASILIK_2 DECIMAL(5,2) NULL,
                ORAN_UST_25 DECIMAL(5,2) NULL,
                ORAN_ALT_25 DECIMAL(5,2) NULL,
                OLASILIK_UST_25 DECIMAL(5,2) NULL,
                OLASILIK_ALT_25 DECIMAL(5,2) NULL,
                OVERROUND_1X2 DECIMAL(5,2) NULL,
                OVERROUND_OU DECIMAL(5,2) NULL,
                KAYNAK NVARCHAR(50) DEFAULT 'OddsPortal',
                KAYIT_TARIHI DATETIME DEFAULT GETDATE(),
                GUNCELLEME_TARIHI DATETIME NULL
            )
        """)
        conn.commit()
        print("BAHIS.MAC_ORANLARI tablosu hazır.")
    except Exception as e:
        print(f"Tablo oluşturma hatası: {e}")

    print("=" * 70)
    print("ODDSPORTAL SCRAPER")
    print(f"Lig sayısı: {len(leagues)}")
    print("=" * 70)

    total_new = 0
    total_updated = 0
    total_matched = 0

    for league in leagues:
        lig_id = league['lig_id']
        league_name = league['name']
        url_path = league['url']

        url = f"https://www.oddsportal.com/football/{url_path}/"

        print(f"\n[{league_name}] {url}")

        try:
            html = get_html(url)
            matches = parse_odds_page(html, league_name)

            new_count = 0
            update_count = 0
            match_count = 0

            for match in matches:
                if match['odds_1']:  # Oran varsa kaydet
                    oran_id, is_new = save_odds(conn, match, lig_id)

                    if is_new:
                        new_count += 1
                    else:
                        update_count += 1

                    # Fikstur ile eşleştir
                    if match_with_fixture(conn, oran_id, lig_id,
                                         match['home_team'], match['away_team'],
                                         match['date']):
                        match_count += 1

            print(f"  Maç: {len(matches)} | Yeni: {new_count} | Güncellendi: {update_count} | Eşleşti: {match_count}")

            total_new += new_count
            total_updated += update_count
            total_matched += match_count

            # Rate limiting
            time.sleep(3)

        except Exception as e:
            print(f"  HATA: {e}")
            continue

    print("\n" + "=" * 70)
    print(f"TOPLAM: {total_new} yeni, {total_updated} güncellendi, {total_matched} eşleşti")
    print("=" * 70)

    conn.close()


def get_upcoming_odds(conn, days_ahead=7):
    """Önümüzdeki X gün içindeki maçların oranlarını getir"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            f.TARIH, f.EVSAHIBI, f.MISAFIR, l.LIG_ADI,
            o.ORAN_1, o.ORAN_X, o.ORAN_2,
            o.OLASILIK_1, o.OLASILIK_X, o.OLASILIK_2
        FROM FIKSTUR.FIKSTUR f
        LEFT JOIN BAHIS.MAC_ORANLARI o ON f.FIKSTURID = o.FIKSTURID
        JOIN TANIM.LIG l ON f.LIG_ID = l.LIG_ID
        WHERE f.DURUM = 0  -- Oynanmamış
        AND f.TARIH >= GETDATE()
        AND f.TARIH <= DATEADD(day, ?, GETDATE())
        ORDER BY f.TARIH
    """, days_ahead)

    return cursor.fetchall()


if __name__ == "__main__":
    main()
