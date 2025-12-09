# -*- coding: utf-8 -*-
"""
FBRef Fixture Scraper
Sadece fikstür bilgisi çeker (oynanmamış maçlar dahil)
Hızlı çalışır - sadece schedule sayfalarını tarar
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')

import pyodbc
from bs4 import BeautifulSoup
from datetime import datetime
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

# Tüm ligler (Full Stats + Summary)
ALL_LEAGUES = [
    # Full Stats
    {"lig_id": 6, "name": "Premier League", "comp_id": 9, "url_name": "Premier-League"},
    {"lig_id": 7, "name": "La Liga", "comp_id": 12, "url_name": "La-Liga"},
    {"lig_id": 8, "name": "Serie A", "comp_id": 11, "url_name": "Serie-A"},
    {"lig_id": 9, "name": "Ligue 1", "comp_id": 13, "url_name": "Ligue-1"},
    {"lig_id": 10, "name": "Bundesliga", "comp_id": 20, "url_name": "Bundesliga"},
    {"lig_id": 11, "name": "Eredivisie", "comp_id": 23, "url_name": "Eredivisie"},
    {"lig_id": 12, "name": "Primeira Liga", "comp_id": 32, "url_name": "Primeira-Liga"},
    {"lig_id": 14, "name": "Brazilian Serie A", "comp_id": 24, "url_name": "Serie-A"},
    {"lig_id": 15, "name": "Championship", "comp_id": 10, "url_name": "Championship"},
    {"lig_id": 17, "name": "First Division A", "comp_id": 37, "url_name": "Belgian-Pro-League"},
    # Summary
    {"lig_id": 4, "name": "Süper Lig", "comp_id": 26, "url_name": "Super-Lig"},
    {"lig_id": 13, "name": "Scottish Premiership", "comp_id": 40, "url_name": "Scottish-Premiership"},
    {"lig_id": 16, "name": "Austrian Bundesliga", "comp_id": 56, "url_name": "Austrian-Bundesliga"},
    {"lig_id": 18, "name": "Superliga", "comp_id": 50, "url_name": "Danish-Superliga"},
    {"lig_id": 19, "name": "Champions League", "comp_id": 8, "url_name": "Champions-League"},
    {"lig_id": 20, "name": "Europa League", "comp_id": 19, "url_name": "Europa-League"},
    {"lig_id": 21, "name": "Europa Conference League", "comp_id": 882, "url_name": "Europa-Conference-League"},
    {"lig_id": 22, "name": "Serbian SuperLiga", "comp_id": 54, "url_name": "Serbian-SuperLiga"},
    {"lig_id": 23, "name": "Swiss Super League", "comp_id": 57, "url_name": "Swiss-Super-League"},
    {"lig_id": 24, "name": "Ekstraklasa", "comp_id": 36, "url_name": "Ekstraklasa"},
    {"lig_id": 25, "name": "Super League Greece", "comp_id": 27, "url_name": "Super-League-Greece"},
    {"lig_id": 26, "name": "Czech First League", "comp_id": 66, "url_name": "Czech-First-League"},
    {"lig_id": 28, "name": "Veikkausliiga", "comp_id": 51, "url_name": "Veikkausliiga"},
    {"lig_id": 29, "name": "Eliteserien", "comp_id": 28, "url_name": "Eliteserien"},
    {"lig_id": 30, "name": "Allsvenskan", "comp_id": 29, "url_name": "Allsvenskan"},
    # Alman Alt Ligleri
    {"lig_id": 31, "name": "2. Bundesliga", "comp_id": 33, "url_name": "2-Bundesliga"},
    {"lig_id": 32, "name": "3. Liga", "comp_id": 59, "url_name": "3-Liga"},
]

# Yerel Kupalar (2024-2025 sezonu)
DOMESTIC_CUPS = [
    {"lig_id": 33, "name": "FA Cup", "comp_id": 514, "url_name": "FA-Cup"},
    {"lig_id": 34, "name": "EFL Cup", "comp_id": 515, "url_name": "EFL-Cup"},
    {"lig_id": 35, "name": "DFB-Pokal", "comp_id": 516, "url_name": "DFB-Pokal"},
    {"lig_id": 36, "name": "Copa del Rey", "comp_id": 569, "url_name": "Copa-del-Rey"},
    {"lig_id": 37, "name": "Coppa Italia", "comp_id": 521, "url_name": "Coppa-Italia"},
    {"lig_id": 38, "name": "Coupe de France", "comp_id": 526, "url_name": "Coupe-de-France"},
    {"lig_id": 39, "name": "Türkiye Kupası", "comp_id": 573, "url_name": "Turkish-Cup"},
    {"lig_id": 40, "name": "KNVB Cup", "comp_id": 520, "url_name": "KNVB-Beker"},
    {"lig_id": 41, "name": "Taça de Portugal", "comp_id": 518, "url_name": "Taca-de-Portugal"},
    {"lig_id": 42, "name": "Beker van België", "comp_id": 540, "url_name": "Belgian-Cup"},
    {"lig_id": 43, "name": "Scottish Cup", "comp_id": 519, "url_name": "Scottish-Cup"},
    {"lig_id": 44, "name": "Scottish League Cup", "comp_id": 538, "url_name": "Scottish-League-Cup"},
]

CUP_SEASON = "2024-2025"

SEASON = "2025-2026"


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
            time.sleep(3)
            html = page.content()
        finally:
            browser.close()

        return html


def parse_fixtures(html, league_name):
    """Schedule sayfasından tüm fikstürleri parse et"""
    soup = BeautifulSoup(html, 'html.parser')
    fixtures = []

    # Fikstür tablosunu bul
    table = soup.find('table', {'id': lambda x: x and 'sched' in x.lower()})
    if not table:
        table = soup.find('table', class_=lambda x: x and 'stats_table' in str(x))

    if not table:
        print(f"  UYARI: Tablo bulunamadı - {league_name}")
        return fixtures

    tbody = table.find('tbody')
    if not tbody:
        return fixtures

    for row in tbody.find_all('tr'):
        if 'thead' in row.get('class', []) or 'spacer' in row.get('class', []):
            continue

        try:
            # Tarih
            date_cell = row.find('td', {'data-stat': 'date'})
            match_date = None
            if date_cell:
                date_text = date_cell.get_text(strip=True)
                if date_text:
                    try:
                        match_date = datetime.strptime(date_text, '%Y-%m-%d')
                    except:
                        pass

            # Saat
            time_cell = row.find('td', {'data-stat': 'time'})
            match_time = time_cell.get_text(strip=True) if time_cell else None

            # Ev sahibi
            home_cell = row.find('td', {'data-stat': 'home_team'})
            home_team = home_cell.get_text(strip=True) if home_cell else None

            # Misafir
            away_cell = row.find('td', {'data-stat': 'away_team'})
            away_team = away_cell.get_text(strip=True) if away_cell else None

            # Skor
            score_cell = row.find('td', {'data-stat': 'score'})
            score = None
            home_score = None
            away_score = None
            if score_cell:
                score_text = score_cell.get_text(strip=True)
                # Skor formatı: "2–1" veya "2-1"
                match = re.match(r'(\d+)[–-](\d+)', score_text)
                if match:
                    home_score = int(match.group(1))
                    away_score = int(match.group(2))
                    score = f"{home_score}-{away_score}"

            # URL
            match_url = None
            if score_cell:
                link = score_cell.find('a')
                if link and link.get('href'):
                    match_url = 'https://fbref.com' + link['href']

            # Hafta/Round
            round_cell = row.find('td', {'data-stat': 'round'}) or row.find('th', {'data-stat': 'round'})
            round_name = round_cell.get_text(strip=True) if round_cell else None

            if home_team and away_team:
                fixtures.append({
                    'date': match_date,
                    'time': match_time,
                    'home_team': home_team,
                    'away_team': away_team,
                    'score': score,
                    'home_score': home_score,
                    'away_score': away_score,
                    'url': match_url,
                    'round': round_name,
                    'played': score is not None
                })
        except Exception as e:
            continue

    return fixtures


def save_fixture(conn, fixture, lig_id):
    """Fikstürü veritabanına kaydet veya güncelle"""
    cursor = conn.cursor()

    # URL veya tarih+takım kombinasyonu ile kontrol
    if fixture['url']:
        cursor.execute("SELECT FIKSTURID FROM FIKSTUR.FIKSTUR WHERE URL = ?", fixture['url'])
    else:
        cursor.execute("""
            SELECT FIKSTURID FROM FIKSTUR.FIKSTUR
            WHERE LIG_ID = ? AND EVSAHIBI = ? AND MISAFIR = ? AND TARIH = ?
        """, lig_id, fixture['home_team'], fixture['away_team'], fixture['date'])

    row = cursor.fetchone()

    if row:
        # Güncelle (eğer skor değişmişse)
        fikstur_id = row[0]
        if fixture['score']:
            cursor.execute("""
                UPDATE FIKSTUR.FIKSTUR
                SET SKOR = ?, DURUM = 1
                WHERE FIKSTURID = ? AND (SKOR IS NULL OR SKOR != ?)
            """, fixture['score'], fikstur_id, fixture['score'])
            conn.commit()
        return fikstur_id, False  # Güncellendi
    else:
        # Yeni kayıt
        cursor.execute("""
            INSERT INTO FIKSTUR.FIKSTUR (
                LIG_ID, EVSAHIBI, MISAFIR, SKOR, TARIH, URL, KAYIT_TARIHI, DURUM
            )
            VALUES (?, ?, ?, ?, ?, ?, GETDATE(), ?)
        """,
            lig_id, fixture['home_team'], fixture['away_team'],
            fixture['score'], fixture['date'], fixture['url'],
            1 if fixture['played'] else 0
        )
        conn.commit()
        cursor.execute("SELECT @@IDENTITY")
        return cursor.fetchone()[0], True  # Yeni eklendi


def main(leagues=None):
    """Ana fonksiyon"""
    if leagues is None:
        leagues = ALL_LEAGUES

    conn = get_db_connection()

    print("=" * 70)
    print("FBREF FIXTURE SCRAPER")
    print(f"Sezon: {SEASON}")
    print(f"Lig sayısı: {len(leagues)}")
    print("=" * 70)

    total_new = 0
    total_updated = 0

    for league in leagues:
        lig_id = league['lig_id']
        comp_id = league['comp_id']
        url_name = league['url_name']
        league_name = league['name']

        url = f"https://fbref.com/en/comps/{comp_id}/{SEASON}/schedule/{SEASON}-{url_name}-Scores-and-Fixtures"

        print(f"\n[{league_name}] {url}")

        try:
            html = get_html(url)
            fixtures = parse_fixtures(html, league_name)

            new_count = 0
            update_count = 0

            for fixture in fixtures:
                _, is_new = save_fixture(conn, fixture, lig_id)
                if is_new:
                    new_count += 1
                else:
                    update_count += 1

            played = len([f for f in fixtures if f['played']])
            not_played = len([f for f in fixtures if not f['played']])

            print(f"  Toplam: {len(fixtures)} | Oynandı: {played} | Oynanmadı: {not_played}")
            print(f"  Yeni: {new_count} | Güncellendi: {update_count}")

            total_new += new_count
            total_updated += update_count

        except Exception as e:
            print(f"  HATA: {e}")
            continue

    print("\n" + "=" * 70)
    print(f"TOPLAM: {total_new} yeni, {total_updated} güncellendi")
    print("=" * 70)

    conn.close()


def main_cups(cups=None):
    """Kupa fikstürlerini çek"""
    if cups is None:
        cups = DOMESTIC_CUPS

    conn = get_db_connection()

    print("=" * 70)
    print("FBREF CUP FIXTURE SCRAPER")
    print(f"Sezon: {CUP_SEASON}")
    print(f"Kupa sayısı: {len(cups)}")
    print("=" * 70)

    total_new = 0
    total_updated = 0

    for cup in cups:
        lig_id = cup['lig_id']
        comp_id = cup['comp_id']
        url_name = cup['url_name']
        cup_name = cup['name']

        url = f"https://fbref.com/en/comps/{comp_id}/{CUP_SEASON}/schedule/{CUP_SEASON}-{url_name}-Scores-and-Fixtures"

        print(f"\n[{cup_name}] {url}")

        try:
            html = get_html(url)
            fixtures = parse_fixtures(html, cup_name)

            new_count = 0
            update_count = 0

            for fixture in fixtures:
                _, is_new = save_fixture(conn, fixture, lig_id)
                if is_new:
                    new_count += 1
                else:
                    update_count += 1

            played = len([f for f in fixtures if f['played']])
            not_played = len([f for f in fixtures if not f['played']])

            print(f"  Toplam: {len(fixtures)} | Oynandı: {played} | Oynanmadı: {not_played}")
            print(f"  Yeni: {new_count} | Güncellendi: {update_count}")

            total_new += new_count
            total_updated += update_count

        except Exception as e:
            print(f"  HATA: {e}")
            continue

    print("\n" + "=" * 70)
    print(f"KUPALAR TOPLAM: {total_new} yeni, {total_updated} güncellendi")
    print("=" * 70)

    conn.close()


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--cups":
        main_cups()
    elif len(sys.argv) > 1 and sys.argv[1] == "--all":
        main()
        main_cups()
    else:
        main()
