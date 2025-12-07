# -*- coding: utf-8 -*-
"""
Tüm Ligleri Ekle ve Analiz Et
1. TANIM.LIG tablosuna 2025-2026 kayıtlarını ekle
2. Her lig için maç sayfası analizi yap (hangi sekmeler var?)
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

# Tüm ligler - FBRef comp_id'leri ile
LEAGUES = [
    {"name": "Premier League", "country": "İNGİLTERE", "comp_id": 9, "url_name": "Premier-League"},
    {"name": "La Liga", "country": "İSPANYA", "comp_id": 12, "url_name": "La-Liga"},
    {"name": "Serie A", "country": "İTALYA", "comp_id": 11, "url_name": "Serie-A"},
    {"name": "Ligue 1", "country": "FRANSA", "comp_id": 13, "url_name": "Ligue-1"},
    {"name": "Bundesliga", "country": "ALMANYA", "comp_id": 20, "url_name": "Bundesliga"},
    {"name": "Eredivisie", "country": "HOLLANDA", "comp_id": 23, "url_name": "Eredivisie"},
    {"name": "Primeira Liga", "country": "PORTEKİZ", "comp_id": 32, "url_name": "Primeira-Liga"},
    {"name": "Süper Lig", "country": "TÜRKİYE", "comp_id": 26, "url_name": "Super-Lig"},
    {"name": "Scottish Premiership", "country": "İSKOÇYA", "comp_id": 40, "url_name": "Scottish-Premiership"},
    {"name": "Brazilian Serie A", "country": "BREZİLYA", "comp_id": 24, "url_name": "Serie-A"},
    {"name": "Championship", "country": "İNGİLTERE", "comp_id": 10, "url_name": "Championship"},
    {"name": "Austrian Bundesliga", "country": "AVUSTURYA", "comp_id": 56, "url_name": "Austrian-Bundesliga"},
    {"name": "First Division A", "country": "BELÇİKA", "comp_id": 37, "url_name": "Belgian-Pro-League"},
    {"name": "Superliga", "country": "DANİMARKA", "comp_id": 50, "url_name": "Danish-Superliga"},
    {"name": "Champions League", "country": "AVRUPA", "comp_id": 8, "url_name": "Champions-League"},
    {"name": "Europa League", "country": "AVRUPA", "comp_id": 19, "url_name": "Europa-League"},
    {"name": "Europa Conference League", "country": "AVRUPA", "comp_id": 882, "url_name": "Europa-Conference-League"},
    {"name": "Serbian SuperLiga", "country": "SIRBİSTAN", "comp_id": 54, "url_name": "Serbian-SuperLiga"},
    {"name": "Swiss Super League", "country": "İSVİÇRE", "comp_id": 57, "url_name": "Swiss-Super-League"},
    {"name": "Ekstraklasa", "country": "POLONYA", "comp_id": 36, "url_name": "Ekstraklasa"},
    {"name": "Super League Greece", "country": "YUNANİSTAN", "comp_id": 27, "url_name": "Super-League-Greece"},
    {"name": "Czech First League", "country": "ÇEKYA", "comp_id": 66, "url_name": "Czech-First-League"},
    {"name": "1. HNL", "country": "HIRVATİSTAN", "comp_id": 68, "url_name": "Hrvatska-NL"},
    {"name": "Veikkausliiga", "country": "FİNLANDİYA", "comp_id": 61, "url_name": "Veikkausliiga"},
    {"name": "Eliteserien", "country": "NORVEÇ", "comp_id": 28, "url_name": "Eliteserien"},
    {"name": "Allsvenskan", "country": "İSVEÇ", "comp_id": 29, "url_name": "Allsvenskan"},
]

SEASON = "2025-2026"
SEZON_ID = 4  # 2025-2026 sezon ID


def get_db_connection():
    return pyodbc.connect(CONNECTION_STRING)


def get_html(url, timeout=60000):
    """Playwright ile sayfa HTML'ini al"""
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            page = context.new_page()
            page.goto(url, timeout=timeout, wait_until="networkidle")
            page.wait_for_timeout(2000)
            html = page.content()
            browser.close()
        return html
    except Exception as e:
        print(f"    HATA: {e}")
        return None


def add_league_to_db(conn, league):
    """Ligi veritabanına ekle"""
    cursor = conn.cursor()

    # URL oluştur
    url = f"https://fbref.com/en/comps/{league['comp_id']}/{SEASON}/schedule/{SEASON}-{league['url_name']}-Scores-and-Fixtures"
    table_id = f"sched_{SEASON}_{league['comp_id']}_1"

    # Zaten var mı kontrol et
    cursor.execute("""
        SELECT LIG_ID FROM TANIM.LIG
        WHERE LIG_ADI = ? AND SEZON = ?
    """, league['name'], SEASON)

    row = cursor.fetchone()
    if row:
        print(f"  [MEVCUT] {league['name']} - LIG_ID: {row[0]}")
        return row[0], url, table_id

    # Yeni kayıt ekle
    cursor.execute("""
        INSERT INTO TANIM.LIG (LIG_ADI, URL, ULKE, SEZON, FIKSTUR_TABLO_ID, SEZON_ID, DURUM, SON_ISLEM_ZAMANI)
        VALUES (?, ?, ?, ?, ?, ?, 1, GETDATE())
    """, league['name'], url, league['country'], SEASON, table_id, SEZON_ID)
    conn.commit()

    cursor.execute("SELECT @@IDENTITY")
    lig_id = cursor.fetchone()[0]
    print(f"  [YENİ] {league['name']} - LIG_ID: {lig_id}")

    return lig_id, url, table_id


def find_played_match(html, table_id):
    """Oynanmış bir maç bul"""
    soup = BeautifulSoup(html, 'html.parser')

    # Tablo ID'sini dene
    table = soup.find('table', id=table_id)
    if not table:
        # Alternatif: herhangi bir sched tablosu
        table = soup.find('table', id=re.compile(r'sched_'))

    if not table:
        return None

    tbody = table.find('tbody')
    if not tbody:
        return None

    for row in tbody.find_all('tr'):
        score_cell = row.find('td', {'data-stat': 'score'})
        if score_cell:
            link = score_cell.find('a')
            if link and link.get_text(strip=True):
                return "https://fbref.com" + link.get('href', '')

    return None


def analyze_match_page(html):
    """Maç sayfasındaki tabloları analiz et"""
    soup = BeautifulSoup(html, 'html.parser')

    # Tüm stats tablolarını bul
    all_tables = soup.find_all('table', id=re.compile(r'stats_.*'))
    keeper_tables = soup.find_all('table', id=re.compile(r'keeper_stats_'))

    # Tablo tiplerini çıkar
    table_types = set()
    for table in all_tables:
        table_id = table.get('id', '')
        parts = table_id.split('_')
        if len(parts) >= 3:
            table_type = parts[-1]
            table_types.add(table_type)

    return {
        'total_tables': len(all_tables),
        'keeper_tables': len(keeper_tables),
        'table_types': sorted(table_types)
    }


def main():
    print("=" * 80)
    print("LİG EKLEYİCİ VE ANALİZCİ")
    print("=" * 80)

    conn = get_db_connection()
    print("Veritabani baglantisi basarili\n")

    results = []

    for i, league in enumerate(LEAGUES, 1):
        print(f"\n[{i}/{len(LEAGUES)}] {league['name']} ({league['country']})")
        print("-" * 50)

        # 1. Ligi veritabanına ekle
        lig_id, fixture_url, table_id = add_league_to_db(conn, league)

        # 2. Fikstur sayfasını yükle
        print(f"  Fikstur sayfasi yukleniyor...")
        fixture_html = get_html(fixture_url)

        if not fixture_html:
            results.append({
                'league': league['name'],
                'country': league['country'],
                'lig_id': lig_id,
                'status': 'HATA - Sayfa yuklenemedi',
                'tables': []
            })
            continue

        # 3. Oynanmış maç bul
        match_url = find_played_match(fixture_html, table_id)

        if not match_url:
            results.append({
                'league': league['name'],
                'country': league['country'],
                'lig_id': lig_id,
                'status': 'Oynanmis mac yok',
                'tables': []
            })
            print(f"  Oynanmis mac bulunamadi")
            continue

        # 4. Maç sayfasını analiz et
        print(f"  Mac sayfasi yukleniyor...")
        match_html = get_html(match_url)

        if not match_html:
            results.append({
                'league': league['name'],
                'country': league['country'],
                'lig_id': lig_id,
                'status': 'HATA - Mac sayfasi yuklenemedi',
                'tables': []
            })
            continue

        analysis = analyze_match_page(match_html)

        results.append({
            'league': league['name'],
            'country': league['country'],
            'lig_id': lig_id,
            'status': 'OK',
            'total_tables': analysis['total_tables'],
            'keeper_tables': analysis['keeper_tables'],
            'tables': analysis['table_types']
        })

        print(f"  Tablolar: {', '.join(analysis['table_types'])}")
        print(f"  Toplam: {analysis['total_tables']} stats + {analysis['keeper_tables']} keeper")

    conn.close()

    # ÖZET RAPOR
    print("\n" + "=" * 80)
    print("ÖZET RAPOR")
    print("=" * 80)

    print(f"\n{'LİG':<30} {'ÜLKE':<15} {'DURUM':<20} {'TABLOLAR'}")
    print("-" * 100)

    for r in results:
        tables_str = ', '.join(r.get('tables', [])) if r.get('tables') else '-'
        print(f"{r['league']:<30} {r['country']:<15} {r['status']:<20} {tables_str}")

    # İstatistik grupları
    print("\n" + "=" * 80)
    print("LİG GRUPLARI (Mevcut Tablolara Göre)")
    print("=" * 80)

    groups = {}
    for r in results:
        if r['status'] == 'OK':
            key = tuple(r['tables'])
            if key not in groups:
                groups[key] = []
            groups[key].append(r['league'])

    for tables, leagues in groups.items():
        print(f"\n[{', '.join(tables)}]")
        for league in leagues:
            print(f"  - {league}")


if __name__ == "__main__":
    main()
