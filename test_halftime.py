"""
Fbref maç detay sayfasından ilk yarı skorunu çekme testi
"""

import pyodbc
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import re

CONNECTION_STRING = (
    "DRIVER={SQL Server};"
    "SERVER=195.201.146.224,1433;"
    "DATABASE=FBREF;"
    "UID=sa;"
    "PWD=FbRef2024Str0ng;"
)


def get_html_with_playwright(url):
    """Playwright ile sayfa HTML'ini al"""
    print(f"  Sayfa yukleniyor: {url}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = context.new_page()

        try:
            page.goto(url, timeout=90000, wait_until="networkidle")
            page.wait_for_timeout(3000)
            html = page.content()
        finally:
            browser.close()

    return html


def extract_halftime_score(html):
    """HTML'den ilk yari skorunu cek"""
    soup = BeautifulSoup(html, 'html.parser')

    # Scorebox'u bul
    scorebox = soup.find('div', class_='scorebox')
    if not scorebox:
        return None, "Scorebox bulunamadi"

    # Ilk yari skoru genellikle scorebox_meta icinde veya event bilgilerinde
    # "HT" veya "(X-X)" formatinda olabilir

    # Yontem 1: scorebox_meta icinde ara
    meta = scorebox.find('div', class_='scorebox_meta')
    if meta:
        meta_text = meta.get_text()
        # HT: 1-0 veya (1-0) formatini ara
        ht_match = re.search(r'HT[:\s]*(\d+)[–-](\d+)', meta_text)
        if ht_match:
            return f"{ht_match.group(1)}-{ht_match.group(2)}", "scorebox_meta"

    # Yontem 2: Event'lerde halftime isaretini ara
    events = soup.find_all('div', class_='event')
    for event in events:
        event_text = event.get_text()
        if 'Half' in event_text or 'HT' in event_text:
            # Skor bilgisini event oncesinden al
            ht_match = re.search(r'(\d+)[–-](\d+)', event_text)
            if ht_match:
                return f"{ht_match.group(1)}-{ht_match.group(2)}", "event"

    # Yontem 3: Tum sayfada HT ara
    page_text = soup.get_text()
    ht_patterns = [
        r'Half\s*Time[:\s]*(\d+)[–-](\d+)',
        r'HT[:\s]*(\d+)[–-](\d+)',
        r'\((\d+)[–-](\d+)\s*HT\)',
    ]
    for pattern in ht_patterns:
        match = re.search(pattern, page_text, re.IGNORECASE)
        if match:
            return f"{match.group(1)}-{match.group(2)}", "page_text"

    # Yontem 4: Gol timeline'dan hesapla
    # 45. dakika ve oncesindeki golleri say
    home_goals_ht = 0
    away_goals_ht = 0

    # Sol taraf (ev sahibi) golleri
    home_events = scorebox.find_all('div', class_='event a')
    for event in home_events:
        event_text = event.get_text()
        # Dakika bilgisini bul
        minute_match = re.search(r"(\d+)'", event_text)
        if minute_match:
            minute = int(minute_match.group(1))
            if minute <= 45:
                # Gol mu kontrol et (kirmizi kart degil)
                if 'goal' in event.get('class', []) or not any(x in event_text.lower() for x in ['red', 'yellow']):
                    home_goals_ht += 1

    # Sag taraf (misafir) golleri
    away_events = scorebox.find_all('div', class_='event b')
    for event in away_events:
        event_text = event.get_text()
        minute_match = re.search(r"(\d+)'", event_text)
        if minute_match:
            minute = int(minute_match.group(1))
            if minute <= 45:
                if 'goal' in event.get('class', []) or not any(x in event_text.lower() for x in ['red', 'yellow']):
                    away_goals_ht += 1

    if home_goals_ht > 0 or away_goals_ht > 0:
        return f"{home_goals_ht}-{away_goals_ht}", "calculated_from_events"

    return None, "Ilk yari skoru bulunamadi"


def main():
    print("=" * 60)
    print("Fbref Ilk Yari Skoru Test")
    print("=" * 60)

    # Veritabanindan 2025-2026 sezonunun ilk 5 macini al
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT TOP 5 f.FIKSTURID, f.EVSAHIBI, f.MISAFIR, f.SKOR, f.URL
        FROM FIKSTUR.FIKSTUR f
        INNER JOIN TANIM.LIG l ON f.LIG_ID = l.LIG_ID
        INNER JOIN FIKSTUR.SEZON s ON l.SEZON_ID = s.SEZON_ID
        WHERE s.SEZON = '2025-2026' AND f.URL IS NOT NULL AND f.SKOR IS NOT NULL AND f.SKOR != ''
        ORDER BY f.TARIH
    """)

    matches = cursor.fetchall()

    if not matches:
        print("[HATA] 2025-2026 sezonunda URL'li mac bulunamadi")
        conn.close()
        return

    print(f"\n{len(matches)} mac test edilecek...\n")

    results = []

    for match in matches:
        fixture_id, home, away, score, url = match
        print(f"\n{'-' * 40}")
        print(f"Mac: {home} vs {away} ({score})")
        print(f"URL: {url}")

        try:
            html = get_html_with_playwright(url)
            ht_score, method = extract_halftime_score(html)

            if ht_score:
                print(f"  [OK] Ilk Yari: {ht_score} (yontem: {method})")
                results.append({
                    'id': fixture_id,
                    'match': f"{home} vs {away}",
                    'final': score,
                    'halftime': ht_score,
                    'method': method,
                    'success': True
                })
            else:
                print(f"  [UYARI] {method}")
                results.append({
                    'id': fixture_id,
                    'match': f"{home} vs {away}",
                    'final': score,
                    'halftime': None,
                    'method': method,
                    'success': False
                })
        except Exception as e:
            print(f"  [HATA] {e}")
            results.append({
                'id': fixture_id,
                'match': f"{home} vs {away}",
                'final': score,
                'halftime': None,
                'method': str(e),
                'success': False
            })

    conn.close()

    # Ozet
    print(f"\n{'=' * 60}")
    print("SONUC OZETI")
    print("=" * 60)

    success_count = sum(1 for r in results if r['success'])
    print(f"Basarili: {success_count}/{len(results)}")

    print("\nDetaylar:")
    for r in results:
        status = "[OK]" if r['success'] else "[X]"
        ht = r['halftime'] if r['halftime'] else "YOK"
        print(f"  {status} {r['match']}: Final={r['final']}, HT={ht}")


if __name__ == "__main__":
    main()
