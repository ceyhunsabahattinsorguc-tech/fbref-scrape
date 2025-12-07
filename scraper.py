"""
FbrefSolution - Türkiye Süper Lig Son 3 Sezon Scraper
Tablo kurallarına uygun olarak verileri çeker ve HTML olarak gösterir.
"""

import pyodbc
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import webbrowser
import os

# Veritabanı bağlantı bilgileri
CONNECTION_STRING = (
    "DRIVER={SQL Server};"
    "SERVER=195.201.146.224,1433;"
    "DATABASE=FBREF;"
    "UID=sa;"
    "PWD=FbRef2024Str0ng;"
)

# Son 4 Türkiye Süper Ligi sezonu
SEASONS = [
    {
        "name": "2025-2026",
        "url": "https://fbref.com/en/comps/26/2025-2026/schedule/2025-2026-Super-Lig-Scores-and-Fixtures",
        "table_id": "sched_2025-2026_26_1"
    },
    {
        "name": "2024-2025",
        "url": "https://fbref.com/en/comps/26/2024-2025/schedule/2024-2025-Super-Lig-Scores-and-Fixtures",
        "table_id": "sched_2024-2025_26_1"
    },
    {
        "name": "2023-2024",
        "url": "https://fbref.com/en/comps/26/2023-2024/schedule/2023-2024-Super-Lig-Scores-and-Fixtures",
        "table_id": "sched_2023-2024_26_1"
    },
    {
        "name": "2022-2023",
        "url": "https://fbref.com/en/comps/26/2022-2023/schedule/2022-2023-Super-Lig-Scores-and-Fixtures",
        "table_id": "sched_2022-2023_26_1"
    }
]


def get_db_connection():
    """Veritabanı bağlantısı oluştur"""
    return pyodbc.connect(CONNECTION_STRING)


def ensure_season_exists(conn, season_name):
    """Sezon kaydını kontrol et veya oluştur"""
    cursor = conn.cursor()

    # Sezon var mı kontrol et
    cursor.execute("SELECT SEZON_ID FROM FIKSTUR.SEZON WHERE SEZON = ?", season_name)
    row = cursor.fetchone()

    if row:
        return row[0]

    # Sezon yoksa oluştur
    cursor.execute("""
        INSERT INTO FIKSTUR.SEZON (SEZON, DURUM, SON_ISLEM_ZAMANI)
        VALUES (?, 1, GETDATE())
    """, season_name)
    conn.commit()

    cursor.execute("SELECT SEZON_ID FROM FIKSTUR.SEZON WHERE SEZON = ?", season_name)
    return cursor.fetchone()[0]


def ensure_league_exists(conn, season_id, season_name, url, table_id):
    """Lig kaydını kontrol et veya oluştur"""
    cursor = conn.cursor()

    # Lig var mı kontrol et
    cursor.execute("""
        SELECT LIG_ID FROM TANIM.LIG
        WHERE SEZON_ID = ? AND (LIG_ADI = 'Süper Lig' OR LIG_ADI = 'Super Lig')
    """, season_id)
    row = cursor.fetchone()

    if row:
        # URL ve table_id güncelle
        cursor.execute("""
            UPDATE TANIM.LIG
            SET URL = ?, FIKSTUR_TABLO_ID = ?, SON_ISLEM_ZAMANI = GETDATE()
            WHERE LIG_ID = ?
        """, url, table_id, row[0])
        conn.commit()
        return row[0]

    # Lig yoksa oluştur
    cursor.execute("""
        INSERT INTO TANIM.LIG (LIG_ADI, URL, ULKE, SEZON, FIKSTUR_TABLO_ID, DURUM, SEZON_ID, SON_ISLEM_ZAMANI)
        VALUES (?, ?, ?, ?, ?, 1, ?, GETDATE())
    """, "Süper Lig", url, "TÜRKİYE", season_name, table_id, season_id)
    conn.commit()

    cursor.execute("""
        SELECT LIG_ID FROM TANIM.LIG
        WHERE SEZON_ID = ? AND LIG_ADI = 'Süper Lig'
    """, season_id)
    return cursor.fetchone()[0]


def get_html_with_playwright(url):
    """Playwright ile sayfa HTML'ini al"""
    print(f"  Sayfa yükleniyor: {url}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        try:
            page.goto(url, timeout=90000, wait_until="networkidle")
            page.wait_for_timeout(3000)  # Ekstra bekleme
            html = page.content()
        finally:
            browser.close()

    return html


def parse_fixtures(html, table_id, league_id):
    """HTML'den fixture verilerini parse et"""
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table', {'id': table_id})

    if not table:
        print(f"  UYARI: Tablo bulunamadı: {table_id}")
        return []

    fixtures = []
    tbody = table.find('tbody')
    if not tbody:
        return []

    rows = tbody.find_all('tr')
    for row in rows:
        cells = row.find_all(['td', 'th'])
        if len(cells) < 7:
            continue

        try:
            week_text = cells[0].get_text(strip=True)
            date_text = cells[1].get_text(strip=True)
            day_text = cells[2].get_text(strip=True)
            home_team = cells[4].get_text(strip=True)
            score_text = cells[5].get_text(strip=True)
            away_team = cells[6].get_text(strip=True)

            # Maç URL'si
            match_url = None
            score_link = cells[5].find('a')
            if score_link and score_link.get('href'):
                match_url = "https://fbref.com" + score_link.get('href')

            # Hafta numarası
            week = None
            if week_text.isdigit():
                week = int(week_text)

            # Tarih parse
            match_date = None
            if date_text:
                try:
                    match_date = datetime.strptime(date_text, "%Y-%m-%d")
                except:
                    pass

            if home_team and away_team:
                fixtures.append({
                    'league_id': league_id,
                    'week': week,
                    'day': day_text,
                    'date': match_date,
                    'home': home_team,
                    'away': away_team,
                    'score': score_text,
                    'url': match_url
                })
        except Exception as e:
            continue

    return fixtures


def save_fixtures(conn, fixtures):
    """Fixture'ları veritabanına kaydet"""
    cursor = conn.cursor()
    inserted = 0
    updated = 0

    for f in fixtures:
        # Mevcut kayıt kontrolü
        cursor.execute("""
            SELECT FIKSTURID FROM FIKSTUR.FIKSTUR
            WHERE LIG_ID = ? AND TARIH = ? AND EVSAHIBI = ? AND MISAFIR = ?
        """, f['league_id'], f['date'], f['home'], f['away'])

        row = cursor.fetchone()

        if row:
            # Güncelle
            cursor.execute("""
                UPDATE FIKSTUR.FIKSTUR
                SET HAFTA = ?, GUN = ?, SKOR = ?, URL = ?, DEGISIKLIK_TARIHI = GETDATE()
                WHERE FIKSTURID = ?
            """, f['week'], f['day'], f['score'], f['url'], row[0])
            updated += 1
        else:
            # Yeni kayıt
            cursor.execute("""
                INSERT INTO FIKSTUR.FIKSTUR
                (LIG_ID, HAFTA, GUN, TARIH, EVSAHIBI, SKOR, MISAFIR, URL, DURUM, KAYIT_TARIHI, DEGISIKLIK_TARIHI)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, GETDATE(), GETDATE())
            """, f['league_id'], f['week'], f['day'], f['date'], f['home'], f['score'], f['away'], f['url'])
            inserted += 1

    conn.commit()
    return inserted, updated


def get_all_fixtures(conn):
    """Tüm fixture'ları sezonlara göre getir"""
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            s.SEZON,
            f.HAFTA,
            f.GUN,
            f.TARIH,
            f.EVSAHIBI,
            f.SKOR,
            f.MISAFIR,
            f.URL
        FROM FIKSTUR.FIKSTUR f
        INNER JOIN TANIM.LIG l ON f.LIG_ID = l.LIG_ID
        INNER JOIN FIKSTUR.SEZON s ON l.SEZON_ID = s.SEZON_ID
        WHERE s.SEZON IN ('2025-2026', '2024-2025', '2023-2024', '2022-2023')
        ORDER BY s.SEZON DESC, f.TARIH, f.HAFTA
    """)

    fixtures = cursor.fetchall()
    return fixtures


def generate_html_report(fixtures):
    """Fixture'ları HTML tablo olarak oluştur"""

    # Sezonlara göre grupla
    seasons_data = {}
    for row in fixtures:
        season = row[0]
        if season not in seasons_data:
            seasons_data[season] = []
        seasons_data[season].append(row)

    html = """
<!DOCTYPE html>
<html lang="tr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Türkiye Süper Ligi - Son 3 Sezon Fikstür</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            color: #eee;
            margin: 0;
            padding: 20px;
        }
        h1 {
            text-align: center;
            color: #e94560;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
            margin-bottom: 30px;
        }
        h2 {
            color: #0f3460;
            background: linear-gradient(90deg, #e94560, #0f3460);
            padding: 15px 20px;
            border-radius: 10px;
            margin-top: 40px;
            text-align: center;
            color: white;
        }
        .stats {
            display: flex;
            justify-content: center;
            gap: 30px;
            margin-bottom: 30px;
            flex-wrap: wrap;
        }
        .stat-box {
            background: #0f3460;
            padding: 20px 40px;
            border-radius: 10px;
            text-align: center;
            box-shadow: 0 4px 15px rgba(0,0,0,0.3);
        }
        .stat-number {
            font-size: 2.5em;
            font-weight: bold;
            color: #e94560;
        }
        .stat-label {
            color: #aaa;
            margin-top: 5px;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            background: #16213e;
            border-radius: 10px;
            overflow: hidden;
            box-shadow: 0 4px 15px rgba(0,0,0,0.3);
        }
        th {
            background: #e94560;
            color: white;
            padding: 15px 10px;
            text-align: left;
            font-weight: 600;
        }
        td {
            padding: 12px 10px;
            border-bottom: 1px solid #1a1a2e;
        }
        tr:hover {
            background: #1a1a2e;
        }
        tr:nth-child(even) {
            background: #0f3460;
        }
        tr:nth-child(even):hover {
            background: #1a1a2e;
        }
        .score {
            font-weight: bold;
            color: #e94560;
            text-align: center;
        }
        .home-team {
            text-align: right;
            font-weight: 500;
        }
        .away-team {
            text-align: left;
            font-weight: 500;
        }
        a {
            color: #4fc3f7;
            text-decoration: none;
        }
        a:hover {
            text-decoration: underline;
        }
        .generated {
            text-align: center;
            color: #666;
            margin-top: 40px;
            padding: 20px;
        }
    </style>
</head>
<body>
    <h1>🏆 Türkiye Süper Ligi - Son 3 Sezon Fikstür</h1>
"""

    total_matches = len(fixtures)
    total_seasons = len(seasons_data)

    html += f"""
    <div class="stats">
        <div class="stat-box">
            <div class="stat-number">{total_seasons}</div>
            <div class="stat-label">Sezon</div>
        </div>
        <div class="stat-box">
            <div class="stat-number">{total_matches}</div>
            <div class="stat-label">Toplam Maç</div>
        </div>
    </div>
"""

    for season in sorted(seasons_data.keys(), reverse=True):
        matches = seasons_data[season]
        html += f"""
    <h2>📅 {season} Sezonu ({len(matches)} maç)</h2>
    <table>
        <thead>
            <tr>
                <th>Hafta</th>
                <th>Tarih</th>
                <th>Gün</th>
                <th class="home-team">Ev Sahibi</th>
                <th class="score">Skor</th>
                <th class="away-team">Misafir</th>
            </tr>
        </thead>
        <tbody>
"""
        for match in matches:
            week = match[1] if match[1] else "-"
            date = match[3].strftime("%d.%m.%Y") if match[3] else "-"
            day = match[2] if match[2] else "-"
            home = match[4] if match[4] else "-"
            score = match[5] if match[5] else "-"
            away = match[6] if match[6] else "-"
            url = match[7]

            score_display = f'<a href="{url}" target="_blank">{score}</a>' if url else score

            html += f"""
            <tr>
                <td>{week}</td>
                <td>{date}</td>
                <td>{day}</td>
                <td class="home-team">{home}</td>
                <td class="score">{score_display}</td>
                <td class="away-team">{away}</td>
            </tr>
"""

        html += """
        </tbody>
    </table>
"""

    html += f"""
    <div class="generated">
        <p>🤖 FbrefSolution Scraper ile oluşturuldu</p>
        <p>Oluşturma tarihi: {datetime.now().strftime("%d.%m.%Y %H:%M:%S")}</p>
    </div>
</body>
</html>
"""

    return html


def main():
    print("=" * 60)
    print("FbrefSolution - Turkiye Super Ligi Son 3 Sezon Scraper")
    print("=" * 60)

    try:
        conn = get_db_connection()
        print("[OK] Veritabani baglantisi basarili")
    except Exception as e:
        print(f"[HATA] Veritabani baglanti hatasi: {e}")
        return

    all_fixtures = []

    for season in SEASONS:
        print(f"\n{'-' * 40}")
        print(f"[SEZON] {season['name']} isleniyor...")

        # Sezon kaydini olustur
        season_id = ensure_season_exists(conn, season['name'])
        print(f"  [OK] Sezon ID: {season_id}")

        # Lig kaydini olustur
        league_id = ensure_league_exists(
            conn, season_id, season['name'],
            season['url'], season['table_id']
        )
        print(f"  [OK] Lig ID: {league_id}")

        # HTML'i al
        try:
            html = get_html_with_playwright(season['url'])
            print(f"  [OK] HTML alindi ({len(html)} karakter)")
        except Exception as e:
            print(f"  [HATA] HTML alma hatasi: {e}")
            continue

        # Fixture'lari parse et
        fixtures = parse_fixtures(html, season['table_id'], league_id)
        print(f"  [OK] {len(fixtures)} mac bulundu")

        # Veritabanina kaydet
        inserted, updated = save_fixtures(conn, fixtures)
        print(f"  [OK] Veritabani: {inserted} yeni, {updated} guncellendi")

        all_fixtures.extend(fixtures)

    print(f"\n{'=' * 60}")
    print(f"[OK] Toplam {len(all_fixtures)} mac islendi")

    # Veritabanindan tum verileri al
    print("\n[RAPOR] HTML raporu olusturuluyor...")
    db_fixtures = get_all_fixtures(conn)

    html_report = generate_html_report(db_fixtures)

    # HTML dosyasini kaydet
    output_path = os.path.join(os.path.dirname(__file__), "super_lig_fixtures.html")
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_report)

    print(f"[OK] HTML rapor kaydedildi: {output_path}")

    # Tarayicida ac
    webbrowser.open('file://' + output_path)
    print("[OK] Rapor tarayicida acildi")

    conn.close()
    print("\n[OK] Islem tamamlandi!")


if __name__ == "__main__":
    main()
