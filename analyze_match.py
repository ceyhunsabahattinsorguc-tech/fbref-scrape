"""
Fbref Mac Sayfasi HTML Analizi
Premier Lig 2025-26 ornek mac
"""

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import os

def get_html(url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(url, timeout=90000, wait_until="networkidle")
        page.wait_for_timeout(3000)
        html = page.content()
        browser.close()
    return html

def analyze_match_page(html):
    soup = BeautifulSoup(html, 'html.parser')

    print("=" * 70)
    print("FBREF MAC SAYFASI ANALIZI")
    print("=" * 70)

    # 1. Scorebox analizi
    print("\n[1] SCOREBOX (Skor, Takim, Menajer)")
    scorebox = soup.find('div', class_='scorebox')
    if scorebox:
        # Manager linkleri
        managers = scorebox.find_all('a', href=lambda x: x and '/managers/' in x)
        for i, m in enumerate(managers):
            print(f"    Menajer {i+1}: {m.get_text(strip=True)}")

    # 2. Team Stats
    print("\n[2] TEAM STATS (Possession, Shots vs)")
    team_stats = soup.find('div', id='team_stats')
    if team_stats:
        print("    team_stats BULUNDU")
        # Possession
        text = team_stats.get_text()
        if 'Possession' in text:
            print(f"    Possession bilgisi var")

    team_stats_extra = soup.find('div', id='team_stats_extra')
    if team_stats_extra:
        print("    team_stats_extra BULUNDU")

    # 3. Lineup / Formation
    print("\n[3] LINEUP (Dizilis)")
    lineups = soup.find_all('div', class_='lineup')
    for i, lineup in enumerate(lineups):
        th = lineup.find('th')
        if th:
            print(f"    Lineup {i+1}: {th.get_text(strip=True)}")

    # 4. Player Stats Tables - EN ONEMLI
    print("\n[4] OYUNCU ISTATISTIK TABLOLARI")

    # Tum tablolari listele
    tables = soup.find_all('table')
    stats_tables = []
    for table in tables:
        table_id = table.get('id', '')
        if 'stats_' in table_id:
            stats_tables.append(table_id)
            print(f"    Tablo: {table_id}")

    # Ilk summary tablosunu detayli incele
    print("\n[5] SUMMARY TABLOSU DETAYI")
    for table_id in stats_tables:
        if 'summary' in table_id:
            table = soup.find('table', id=table_id)
            if table:
                print(f"\n    >>> {table_id}")

                # Header'lari al
                thead = table.find('thead')
                if thead:
                    headers = []
                    # Son tr'deki th'leri al (asil basliklar)
                    header_rows = thead.find_all('tr')
                    if header_rows:
                        last_header = header_rows[-1]
                        for th in last_header.find_all('th'):
                            header_text = th.get_text(strip=True)
                            data_stat = th.get('data-stat', '')
                            headers.append((header_text, data_stat))

                        print("    SUTUNLAR:")
                        for i, (text, stat) in enumerate(headers):
                            print(f"      [{i}] {stat:25} -> {text}")

                # Ilk oyuncu satirini ornek goster
                tbody = table.find('tbody')
                if tbody:
                    first_row = tbody.find('tr')
                    if first_row:
                        print("\n    ORNEK SATIR (Ilk oyuncu):")
                        cells = first_row.find_all(['th', 'td'])
                        for i, cell in enumerate(cells[:15]):  # Ilk 15 sutun
                            data_stat = cell.get('data-stat', '')
                            value = cell.get_text(strip=True)
                            print(f"      [{i}] {data_stat:25} = {value}")

                break  # Sadece ilk summary tablosu

    # 6. Tum data-stat degerlerini topla
    print("\n[6] TUM DATA-STAT DEGERLERI (Summary tablolarindan)")
    all_stats = set()
    for table_id in stats_tables:
        if 'summary' in table_id:
            table = soup.find('table', id=table_id)
            if table:
                for cell in table.find_all(['th', 'td']):
                    ds = cell.get('data-stat')
                    if ds:
                        all_stats.add(ds)

    print("    " + ", ".join(sorted(all_stats)))


def main():
    # Onca Premier Lig 2025-26 fikstur sayfasindan bir mac URL'si bul
    fixture_url = "https://fbref.com/en/comps/9/2025-2026/schedule/2025-2026-Premier-League-Scores-and-Fixtures"

    print("Premier Lig 2025-26 fikstur sayfasi yukleniyor...")
    fixture_html = get_html(fixture_url)

    # Oynanmis bir mac bul (skoru olan)
    soup = BeautifulSoup(fixture_html, 'html.parser')
    table = soup.find('table', id='sched_2025-2026_9_1')
    if table:
        rows = table.find_all('tr')
        for row in rows:
            score_cell = row.find('td', {'data-stat': 'score'})
            if score_cell:
                link = score_cell.find('a')
                if link and link.get_text(strip=True):  # Skor varsa
                    url = "https://fbref.com" + link.get('href')
                    print(f"Bulunan mac: {url}")
                    break
    else:
        print("Tablo bulunamadi, manuel URL kullaniliyor")
        url = "https://fbref.com/en/matches/d5b8ae05/Manchester-United-Fulham-August-16-2025-Premier-League"

    print(f"\nMac sayfasi yukleniyor: {url}")
    html = get_html(url)

    # HTML'i kaydet
    output_path = os.path.join(os.path.dirname(__file__), "match_sample.html")
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"HTML kaydedildi: {output_path}")

    # Analiz
    analyze_match_page(html)


if __name__ == "__main__":
    main()
