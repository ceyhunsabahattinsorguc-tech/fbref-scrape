# -*- coding: utf-8 -*-
"""
Golcü Oranları Scraper
Tipico, Bet365 ve diğer kaynaklardan golcü oranlarını çeker
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')

import pyodbc
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright
import time
import re
import json

CONNECTION_STRING = (
    "DRIVER={SQL Server};"
    "SERVER=195.201.146.224,1433;"
    "DATABASE=FBREF;"
    "UID=sa;"
    "PWD=FbRef2024Str0ng;"
)

# Tipico maç sayfası URL pattern
# https://sports.tipico.de/en/event/football/xxx
TIPICO_LEAGUES = [
    {"lig_id": 6, "name": "Premier League", "tipico_id": "england-premier-league"},
    {"lig_id": 7, "name": "La Liga", "tipico_id": "spain-la-liga"},
    {"lig_id": 8, "name": "Serie A", "tipico_id": "italy-serie-a"},
    {"lig_id": 9, "name": "Ligue 1", "tipico_id": "france-ligue-1"},
    {"lig_id": 10, "name": "Bundesliga", "tipico_id": "germany-bundesliga"},
    {"lig_id": 4, "name": "Super Lig", "tipico_id": "turkey-super-lig"},
]


def get_db_connection():
    return pyodbc.connect(CONNECTION_STRING)


def get_html(url, timeout=60000):
    """Playwright ile HTML al"""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            locale='en-US',
            timezone_id='Europe/Berlin'
        )
        page = context.new_page()

        try:
            page.goto(url, timeout=timeout, wait_until='networkidle')
            time.sleep(5)

            # "Anytime Goalscorer" veya "Torschütze" sekmesine tıkla
            try:
                scorer_tab = page.query_selector('text="Anytime Goalscorer"')
                if scorer_tab:
                    scorer_tab.click()
                    time.sleep(2)
                else:
                    scorer_tab = page.query_selector('text="Goalscorer"')
                    if scorer_tab:
                        scorer_tab.click()
                        time.sleep(2)
            except:
                pass

            html = page.content()
        finally:
            browser.close()

        return html


def calculate_probability(odds):
    """Oranı olasılığa çevir"""
    if odds and odds > 0:
        return round(100 / odds, 2)
    return None


def parse_goalscorer_odds(html, home_team, away_team):
    """Golcü oranlarını parse et"""
    soup = BeautifulSoup(html, 'html.parser')
    players = []

    # Tipico yapısı: market-group class'ları içinde oyuncu oranları
    goalscorer_section = None

    # "Anytime Goalscorer" bölümünü bul
    for section in soup.find_all(['div', 'section'], class_=re.compile(r'market|betting')):
        header = section.find(['h2', 'h3', 'span'], text=re.compile(r'Goalscorer|Torschütz', re.I))
        if header:
            goalscorer_section = section
            break

    if goalscorer_section:
        # Oyuncu satırlarını bul
        player_rows = goalscorer_section.find_all(['div', 'tr'], class_=re.compile(r'outcome|selection|row'))

        for row in player_rows:
            try:
                # Oyuncu adı
                name_elem = row.find(['span', 'td', 'div'], class_=re.compile(r'name|participant|label'))
                if not name_elem:
                    name_elem = row.find(['span', 'td'])

                if name_elem:
                    player_name = name_elem.get_text(strip=True)

                    # Oran
                    odds_elem = row.find(['span', 'td', 'button'], class_=re.compile(r'odds|price|value'))
                    if odds_elem:
                        odds_text = odds_elem.get_text(strip=True)
                        try:
                            odds = float(odds_text.replace(',', '.'))
                            if odds > 1:  # Geçerli oran
                                # Takım belirleme (basit yaklaşım)
                                team = home_team  # Default
                                if any(away_word in player_name.lower() for away_word in away_team.lower().split()):
                                    team = away_team

                                players.append({
                                    'player_name': player_name,
                                    'team': team,
                                    'odds_anytime': odds,
                                    'probability': calculate_probability(odds)
                                })
                        except:
                            pass
            except:
                continue

    return players


def save_goalscorer_odds(conn, player, fikstur_id, match_date):
    """Golcü oranını veritabanına kaydet"""
    cursor = conn.cursor()

    # Tablonun varlığını kontrol et
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'GOLCU_ORANLARI' AND schema_id = SCHEMA_ID('BAHIS'))
        CREATE TABLE BAHIS.GOLCU_ORANLARI (
            GOLCU_ORAN_ID INT IDENTITY(1,1) PRIMARY KEY,
            FIKSTURID INT NULL,
            OYUNCU_ID INT NULL,
            OYUNCU_ADI NVARCHAR(100) NOT NULL,
            TAKIM_ADI NVARCHAR(100) NOT NULL,
            MAC_TARIHI DATE NOT NULL,
            ORAN_HER_AN DECIMAL(5,2) NULL,
            ORAN_ILK_GOL DECIMAL(5,2) NULL,
            ORAN_SON_GOL DECIMAL(5,2) NULL,
            ORAN_2_GOL DECIMAL(5,2) NULL,
            ORAN_HAT_TRICK DECIMAL(5,2) NULL,
            OLASILIK_GOL DECIMAL(5,2) NULL,
            KAYNAK NVARCHAR(50) DEFAULT 'Tipico',
            KAYIT_TARIHI DATETIME DEFAULT GETDATE(),
            GUNCELLEME_TARIHI DATETIME NULL
        )
    """)
    conn.commit()

    # Mevcut kayıt kontrolü
    cursor.execute("""
        SELECT GOLCU_ORAN_ID FROM BAHIS.GOLCU_ORANLARI
        WHERE FIKSTURID = ? AND OYUNCU_ADI = ?
    """, fikstur_id, player['player_name'])

    row = cursor.fetchone()

    if row:
        # Güncelle
        cursor.execute("""
            UPDATE BAHIS.GOLCU_ORANLARI
            SET ORAN_HER_AN = ?, OLASILIK_GOL = ?, GUNCELLEME_TARIHI = GETDATE()
            WHERE GOLCU_ORAN_ID = ?
        """, player['odds_anytime'], player['probability'], row[0])
        conn.commit()
        return row[0], False
    else:
        # Yeni kayıt
        cursor.execute("""
            INSERT INTO BAHIS.GOLCU_ORANLARI (
                FIKSTURID, OYUNCU_ADI, TAKIM_ADI, MAC_TARIHI,
                ORAN_HER_AN, OLASILIK_GOL, KAYNAK
            )
            VALUES (?, ?, ?, ?, ?, ?, 'Tipico')
        """, fikstur_id, player['player_name'], player['team'],
            match_date, player['odds_anytime'], player['probability'])
        conn.commit()
        cursor.execute("SELECT @@IDENTITY")
        return cursor.fetchone()[0], True


def match_player_to_db(conn, player_name, team_name):
    """Oyuncuyu veritabanındaki kayıtla eşleştir"""
    cursor = conn.cursor()

    # Tam eşleşme
    cursor.execute("""
        SELECT OYUNCU_ID FROM TANIM.OYUNCU
        WHERE OYUNCU_ADI = ?
    """, player_name)

    row = cursor.fetchone()
    if row:
        return row[0]

    # Benzer isim araması
    name_parts = player_name.split()
    if len(name_parts) >= 2:
        surname = name_parts[-1]
        cursor.execute("""
            SELECT TOP 1 OYUNCU_ID FROM TANIM.OYUNCU
            WHERE OYUNCU_ADI LIKE ?
        """, f'%{surname}%')

        row = cursor.fetchone()
        if row:
            return row[0]

    return None


def get_upcoming_matches(conn, lig_id, days_ahead=3):
    """Önümüzdeki X gün içindeki maçları getir"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT FIKSTURID, TARIH, EVSAHIBI, MISAFIR
        FROM FIKSTUR.FIKSTUR
        WHERE LIG_ID = ?
        AND DURUM = 0  -- Oynanmamış
        AND TARIH >= CAST(GETDATE() AS DATE)
        AND TARIH <= DATEADD(day, ?, GETDATE())
        ORDER BY TARIH
    """, lig_id, days_ahead)

    return cursor.fetchall()


def main(leagues=None, days_ahead=3):
    """Ana fonksiyon"""
    if leagues is None:
        leagues = TIPICO_LEAGUES

    conn = get_db_connection()

    print("=" * 70)
    print("GOLCÜ ORANLARI SCRAPER")
    print(f"Lig sayısı: {len(leagues)}")
    print(f"Önümüzdeki {days_ahead} gün")
    print("=" * 70)

    total_new = 0
    total_updated = 0
    total_players = 0

    for league in leagues:
        lig_id = league['lig_id']
        league_name = league['name']
        tipico_id = league['tipico_id']

        print(f"\n[{league_name}]")

        # Önümüzdeki maçları al
        matches = get_upcoming_matches(conn, lig_id, days_ahead)
        print(f"  Maç sayısı: {len(matches)}")

        for match in matches:
            fikstur_id, match_date, home_team, away_team = match

            # Tipico maç URL'si oluştur
            # Not: Gerçek URL yapısı değişkenlik gösterebilir
            url = f"https://sports.tipico.de/en/sports/football/{tipico_id}"

            print(f"    {home_team} vs {away_team} ({match_date})")

            try:
                html = get_html(url)
                players = parse_goalscorer_odds(html, home_team, away_team)

                new_count = 0
                update_count = 0

                for player in players:
                    _, is_new = save_goalscorer_odds(conn, player, fikstur_id, match_date)

                    if is_new:
                        new_count += 1
                    else:
                        update_count += 1

                print(f"      Oyuncu: {len(players)} | Yeni: {new_count} | Güncellendi: {update_count}")

                total_new += new_count
                total_updated += update_count
                total_players += len(players)

                # Rate limiting
                time.sleep(5)

            except Exception as e:
                print(f"      HATA: {e}")
                continue

    print("\n" + "=" * 70)
    print(f"TOPLAM: {total_players} oyuncu, {total_new} yeni, {total_updated} güncellendi")
    print("=" * 70)

    conn.close()


# =============================================
# ALTERNATİF: API TABANLI YAKLAŞIM
# =============================================

def fetch_odds_from_api(api_key, match_id):
    """
    RapidAPI veya The Odds API üzerinden golcü oranları çek

    Kullanım:
    1. https://the-odds-api.com/ adresinden ücretsiz API key al
    2. api_key parametresini ayarla

    Örnek endpoint:
    GET https://api.the-odds-api.com/v4/sports/soccer_epl/odds/
        ?apiKey=YOUR_API_KEY
        &regions=eu
        &markets=player_goal_scorer_anytime
    """
    import requests

    url = "https://api.the-odds-api.com/v4/sports/soccer_epl/odds/"
    params = {
        "apiKey": api_key,
        "regions": "eu",
        "markets": "player_goal_scorer_anytime",
        "oddsFormat": "decimal"
    }

    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"API Hatası: {response.status_code}")
            return None
    except Exception as e:
        print(f"API bağlantı hatası: {e}")
        return None


def get_goalscorer_rankings(conn, fikstur_id):
    """
    Belirli bir maç için golcü oranlarını sıralı getir
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            g.OYUNCU_ADI,
            g.TAKIM_ADI,
            g.ORAN_HER_AN,
            g.OLASILIK_GOL,
            s.TOPLAM_SKOR AS ALGORITMA_SKORU
        FROM BAHIS.GOLCU_ORANLARI g
        LEFT JOIN TAHMIN.v_Oyuncu_Gol_Skoru s
            ON g.OYUNCU_ADI = s.OYUNCU_ADI
        WHERE g.FIKSTURID = ?
        ORDER BY g.OLASILIK_GOL DESC
    """, fikstur_id)

    return cursor.fetchall()


def compare_odds_vs_algorithm(conn, fikstur_id):
    """
    Bahis oranları ile kendi algoritmamızı karşılaştır
    Value bet fırsatlarını bul
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            g.OYUNCU_ADI,
            g.ORAN_HER_AN,
            g.OLASILIK_GOL AS BAHIS_OLASILIK,
            s.TOPLAM_SKOR AS ALGORITMA_SKOR,
            -- Basit normalizasyon (0-100 -> olasılık)
            s.TOPLAM_SKOR AS ALGORITMA_OLASILIK,
            -- Fark: Pozitif = Bizim tahminimiz daha yüksek (value bet?)
            (s.TOPLAM_SKOR - g.OLASILIK_GOL) AS FARK
        FROM BAHIS.GOLCU_ORANLARI g
        JOIN TAHMIN.v_Oyuncu_Gol_Skoru s ON g.OYUNCU_ADI = s.OYUNCU_ADI
        WHERE g.FIKSTURID = ?
        ORDER BY (s.TOPLAM_SKOR - g.OLASILIK_GOL) DESC
    """, fikstur_id)

    results = cursor.fetchall()

    print("\n=== VALUE BET ANALİZİ ===")
    print(f"{'Oyuncu':<25} {'Oran':<8} {'Bahis %':<10} {'Algo %':<10} {'Fark':<8}")
    print("-" * 65)

    for row in results:
        oyuncu, oran, bahis_olas, algo_skor, algo_olas, fark = row
        marker = "🔥" if fark > 10 else ""
        print(f"{oyuncu:<25} {oran:<8.2f} {bahis_olas:<10.1f} {algo_olas:<10.1f} {fark:+.1f} {marker}")

    return results


if __name__ == "__main__":
    # Test: Sadece Premier League, önümüzdeki 1 gün
    test_leagues = [l for l in TIPICO_LEAGUES if l['lig_id'] == 6]
    main(test_leagues, days_ahead=1)
