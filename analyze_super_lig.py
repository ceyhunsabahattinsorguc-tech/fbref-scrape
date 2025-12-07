# -*- coding: utf-8 -*-
"""
Süper Lig Maç Sayfası Analizi
Hangi sekmeler ve data-stat değerleri mevcut?
"""

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import sys
import re

sys.stdout.reconfigure(encoding='utf-8')

def get_html(url):
    """Playwright ile sayfa HTML'ini al"""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = context.new_page()
        print(f"Sayfa yukleniyor: {url}")
        page.goto(url, timeout=90000, wait_until="networkidle")
        page.wait_for_timeout(3000)
        html = page.content()
        browser.close()
    return html

def find_super_lig_match():
    """Süper Lig 2025-26 fiksturünden oynanmış bir maç bul"""
    fixture_url = "https://fbref.com/en/comps/26/2025-2026/schedule/2025-2026-Super-Lig-Scores-and-Fixtures"

    print("=" * 70)
    print("SUPER LIG 2025-26 FIKSTUR SAYFASI")
    print("=" * 70)

    html = get_html(fixture_url)
    soup = BeautifulSoup(html, 'html.parser')

    table = soup.find('table', id='sched_2025-2026_26_1')
    if not table:
        print("HATA: Fikstur tablosu bulunamadi!")
        return None

    # Oynanmış maç bul (skoru olan)
    rows = table.find('tbody').find_all('tr') if table.find('tbody') else []
    match_url = None

    for row in rows:
        score_cell = row.find('td', {'data-stat': 'score'})
        if score_cell:
            link = score_cell.find('a')
            if link and link.get_text(strip=True):
                # Ev ve deplasman takımlarını al
                home = row.find('td', {'data-stat': 'home_team'})
                away = row.find('td', {'data-stat': 'away_team'})
                score = link.get_text(strip=True)

                home_name = home.get_text(strip=True) if home else "?"
                away_name = away.get_text(strip=True) if away else "?"

                match_url = "https://fbref.com" + link.get('href')
                print(f"\nBulunan mac: {home_name} {score} {away_name}")
                print(f"URL: {match_url}")
                break

    return match_url

def analyze_match_page(html):
    """Maç sayfasını analiz et - hangi tablolar var?"""
    soup = BeautifulSoup(html, 'html.parser')

    print("\n" + "=" * 70)
    print("MAC SAYFASI ANALIZI")
    print("=" * 70)

    # Tüm stats tablolarını bul
    all_tables = soup.find_all('table', id=re.compile(r'stats_.*'))

    print(f"\nToplam stats tablosu: {len(all_tables)}")
    print("\nBulunan tablolar:")
    print("-" * 50)

    table_types = {}
    for table in all_tables:
        table_id = table.get('id', '')
        print(f"  - {table_id}")

        # Tablo tipini çıkar (summary, passing, defense vs.)
        # Format: stats_<team_id>_<type>
        parts = table_id.split('_')
        if len(parts) >= 3:
            table_type = parts[-1]  # Son kısım tip
            if table_type not in table_types:
                table_types[table_type] = []
            table_types[table_type].append(table_id)

    print("\n" + "=" * 70)
    print("TABLO TIPLERI (SEKMELER)")
    print("=" * 70)

    for ttype, tables in sorted(table_types.items()):
        print(f"\n[{ttype.upper()}] - {len(tables)} tablo")

        # Bu tipteki ilk tablodan sütunları al
        if tables:
            sample_table = soup.find('table', id=tables[0])
            if sample_table:
                thead = sample_table.find('thead')
                if thead:
                    # Son header row'daki sütunları al
                    header_rows = thead.find_all('tr')
                    if header_rows:
                        last_row = header_rows[-1]
                        columns = []
                        for th in last_row.find_all('th'):
                            data_stat = th.get('data-stat', '')
                            if data_stat and data_stat != 'player':
                                columns.append(data_stat)

                        print(f"  Sutunlar ({len(columns)}): {', '.join(columns[:15])}")
                        if len(columns) > 15:
                            print(f"  ... ve {len(columns) - 15} sutun daha")

def main():
    # 1. Süper Lig maçı bul
    match_url = find_super_lig_match()

    if not match_url:
        print("Oynanmis mac bulunamadi!")
        return

    # 2. Maç sayfasını yükle
    print("\n" + "=" * 70)
    print("MAC SAYFASI YUKLENIYOR")
    print("=" * 70)

    html = get_html(match_url)

    # 3. Analiz et
    analyze_match_page(html)

    print("\n" + "=" * 70)
    print("ANALIZ TAMAMLANDI")
    print("=" * 70)

if __name__ == "__main__":
    main()
