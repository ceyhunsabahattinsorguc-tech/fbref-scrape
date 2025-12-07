# -*- coding: utf-8 -*-
"""
FBRef Güncelleme Scraper
Sadece yeni maçları çeker - mevcut veriler korunur
"""

import pyodbc
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import re
import sys

sys.stdout.reconfigure(encoding='utf-8')

CONNECTION_STRING = (
    "DRIVER={SQL Server};"
    "SERVER=195.201.146.224,1433;"
    "DATABASE=FBREF;"
    "UID=sa;"
    "PWD=FbRef2024Str0ng;"
)

# Tüm ligler
ALL_LEAGUES = [
    # Full Stats (6 sekme)
    {"lig_id": 6, "name": "Premier League", "comp_id": 9, "url_name": "Premier-League", "type": "full"},
    {"lig_id": 7, "name": "La Liga", "comp_id": 12, "url_name": "La-Liga", "type": "full"},
    {"lig_id": 8, "name": "Serie A", "comp_id": 11, "url_name": "Serie-A", "type": "full"},
    {"lig_id": 9, "name": "Ligue 1", "comp_id": 13, "url_name": "Ligue-1", "type": "full"},
    {"lig_id": 10, "name": "Bundesliga", "comp_id": 20, "url_name": "Bundesliga", "type": "full"},
    {"lig_id": 11, "name": "Eredivisie", "comp_id": 23, "url_name": "Eredivisie", "type": "full"},
    {"lig_id": 12, "name": "Primeira Liga", "comp_id": 32, "url_name": "Primeira-Liga", "type": "full"},
    {"lig_id": 14, "name": "Brazilian Serie A", "comp_id": 24, "url_name": "Serie-A", "type": "full"},
    {"lig_id": 15, "name": "Championship", "comp_id": 10, "url_name": "Championship", "type": "full"},
    {"lig_id": 17, "name": "First Division A", "comp_id": 37, "url_name": "Belgian-Pro-League", "type": "full"},
    # Summary Only
    {"lig_id": 4, "name": "Süper Lig", "comp_id": 26, "url_name": "Super-Lig", "type": "summary"},
    {"lig_id": 13, "name": "Scottish Premiership", "comp_id": 40, "url_name": "Scottish-Premiership", "type": "summary"},
    {"lig_id": 16, "name": "Austrian Bundesliga", "comp_id": 56, "url_name": "Austrian-Bundesliga", "type": "summary"},
    {"lig_id": 18, "name": "Superliga", "comp_id": 50, "url_name": "Danish-Superliga", "type": "summary"},
    {"lig_id": 19, "name": "Champions League", "comp_id": 8, "url_name": "Champions-League", "type": "summary"},
    {"lig_id": 20, "name": "Europa League", "comp_id": 19, "url_name": "Europa-League", "type": "summary"},
    {"lig_id": 21, "name": "Europa Conference League", "comp_id": 882, "url_name": "Europa-Conference-League", "type": "summary"},
    {"lig_id": 22, "name": "Serbian SuperLiga", "comp_id": 54, "url_name": "Serbian-SuperLiga", "type": "summary"},
    {"lig_id": 23, "name": "Swiss Super League", "comp_id": 57, "url_name": "Swiss-Super-League", "type": "summary"},
    {"lig_id": 24, "name": "Ekstraklasa", "comp_id": 36, "url_name": "Ekstraklasa", "type": "summary"},
    {"lig_id": 25, "name": "Super League Greece", "comp_id": 27, "url_name": "Super-League-Greece", "type": "summary"},
    {"lig_id": 26, "name": "Czech First League", "comp_id": 66, "url_name": "Czech-First-League", "type": "summary"},
    {"lig_id": 28, "name": "Veikkausliiga", "comp_id": 61, "url_name": "Veikkausliiga", "type": "summary"},
    {"lig_id": 29, "name": "Eliteserien", "comp_id": 28, "url_name": "Eliteserien", "type": "summary"},
    {"lig_id": 30, "name": "Allsvenskan", "comp_id": 29, "url_name": "Allsvenskan", "type": "summary"},
]

SEASON = "2025-2026"
SEZON_ID = 4


def get_db_connection():
    return pyodbc.connect(CONNECTION_STRING)


def get_existing_match_urls(conn, lig_id):
    """Bu lig için mevcut maç URL'lerini getir"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT URL FROM FIKSTUR.FIKSTUR
        WHERE LIG_ID = ?
    """, lig_id)
    return set(row[0] for row in cursor.fetchall())


def get_html(url, timeout=90000):
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            page = context.new_page()
            page.goto(url, timeout=timeout, wait_until="networkidle")
            page.wait_for_timeout(3000)
            html = page.content()
            browser.close()
        return html
    except Exception as e:
        print(f"    HATA: {e}")
        return None


def get_new_matches(fixture_html, table_id, existing_urls):
    """Sadece yeni maçları getir (mevcut olmayanlar)"""
    soup = BeautifulSoup(fixture_html, 'html.parser')
    table = soup.find('table', id=table_id)
    if not table:
        table = soup.find('table', id=re.compile(r'sched_'))

    if not table:
        return []

    new_matches = []
    tbody = table.find('tbody')
    if not tbody:
        return []

    for row in tbody.find_all('tr'):
        score_cell = row.find('td', {'data-stat': 'score'})
        if not score_cell:
            continue

        score_link = score_cell.find('a')
        if not score_link or not score_link.get_text(strip=True):
            continue  # Oynanmamış maç

        score = score_link.get_text(strip=True)
        match_url = "https://fbref.com" + score_link.get('href', '')

        # Zaten varsa atla
        if match_url in existing_urls:
            continue

        home_cell = row.find('td', {'data-stat': 'home_team'})
        away_cell = row.find('td', {'data-stat': 'away_team'})
        date_cell = row.find('td', {'data-stat': 'date'})

        new_matches.append({
            'url': match_url,
            'home_team': home_cell.get_text(strip=True) if home_cell else "?",
            'away_team': away_cell.get_text(strip=True) if away_cell else "?",
            'score': score,
            'date': date_cell.get_text(strip=True) if date_cell else None
        })

    return new_matches


def check_for_updates():
    """Tüm liglerde kaç yeni maç var kontrol et"""
    print("=" * 70)
    print("YENİ MAÇ KONTROLÜ")
    print("=" * 70)

    conn = get_db_connection()
    results = []

    for league in ALL_LEAGUES:
        fixture_url = f"https://fbref.com/en/comps/{league['comp_id']}/{SEASON}/schedule/{SEASON}-{league['url_name']}-Scores-and-Fixtures"
        table_id = f"sched_{SEASON}_{league['comp_id']}_1"

        print(f"\n{league['name']}...", end=" ")

        # Mevcut maç URL'lerini al
        existing_urls = get_existing_match_urls(conn, league['lig_id'])

        # Fikstur sayfasını yükle
        fixture_html = get_html(fixture_url)
        if not fixture_html:
            print("HATA")
            continue

        # Yeni maçları bul
        new_matches = get_new_matches(fixture_html, table_id, existing_urls)

        print(f"Mevcut: {len(existing_urls)}, Yeni: {len(new_matches)}")

        results.append({
            'league': league['name'],
            'lig_id': league['lig_id'],
            'type': league['type'],
            'existing': len(existing_urls),
            'new': len(new_matches),
            'new_matches': new_matches
        })

    conn.close()

    # Özet
    print("\n" + "=" * 70)
    print("ÖZET")
    print("=" * 70)

    total_new = sum(r['new'] for r in results)
    print(f"\nToplam yeni maç: {total_new}")

    if total_new > 0:
        print("\nYeni maçları olan ligler:")
        for r in results:
            if r['new'] > 0:
                print(f"  - {r['league']}: {r['new']} yeni maç")

    return results


def update_league(conn, league, new_matches):
    """Bir lig için yeni maçları işle"""
    # Scraper modüllerini import et
    if league['type'] == 'full':
        from scraper_full import process_match
    else:
        from scraper_summary import process_match

    success = 0
    for i, match in enumerate(new_matches, 1):
        print(f"  [{i}/{len(new_matches)}]", end=" ")
        try:
            if process_match(conn, match, league['lig_id'], None):
                success += 1
        except Exception as e:
            print(f"HATA: {e}")

    return success


def run_update(selected_leagues=None):
    """Güncelleme çalıştır"""
    print("=" * 70)
    print("GÜNCELLEME BAŞLIYOR")
    print("=" * 70)

    conn = get_db_connection()

    leagues = selected_leagues if selected_leagues else ALL_LEAGUES
    total_new = 0
    total_success = 0

    for league in leagues:
        fixture_url = f"https://fbref.com/en/comps/{league['comp_id']}/{SEASON}/schedule/{SEASON}-{league['url_name']}-Scores-and-Fixtures"
        table_id = f"sched_{SEASON}_{league['comp_id']}_1"

        print(f"\n{'='*60}")
        print(f"{league['name']}")
        print(f"{'='*60}")

        # Mevcut maç URL'lerini al
        existing_urls = get_existing_match_urls(conn, league['lig_id'])
        print(f"Mevcut maç sayısı: {len(existing_urls)}")

        # Fikstur sayfasını yükle
        print("Fikstur sayfası yükleniyor...")
        fixture_html = get_html(fixture_url)
        if not fixture_html:
            print("HATA: Fikstur yüklenemedi")
            continue

        # Yeni maçları bul
        new_matches = get_new_matches(fixture_html, table_id, existing_urls)
        print(f"Yeni maç sayısı: {len(new_matches)}")

        if not new_matches:
            print("Güncellenecek maç yok.")
            continue

        # Yeni maçları işle
        total_new += len(new_matches)
        success = update_league(conn, league, new_matches)
        total_success += success

    conn.close()

    # Özet
    print("\n" + "=" * 70)
    print("GÜNCELLEME TAMAMLANDI")
    print("=" * 70)
    print(f"Toplam yeni maç: {total_new}")
    print(f"Başarılı: {total_success}")
    print(f"Hatalı: {total_new - total_success}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description='FBRef Güncelleme Scraper')
    parser.add_argument('--check', action='store_true', help='Sadece yeni maçları kontrol et, çekme')
    parser.add_argument('--league', type=str, help='Sadece belirli ligi güncelle (lig adı)')

    args = parser.parse_args()

    if args.check:
        check_for_updates()
    elif args.league:
        # Belirli lig
        selected = [l for l in ALL_LEAGUES if args.league.lower() in l['name'].lower()]
        if selected:
            run_update(selected)
        else:
            print(f"Lig bulunamadı: {args.league}")
    else:
        run_update()


if __name__ == "__main__":
    main()
