# -*- coding: utf-8 -*-
"""
Süper Lig 2025-2026 Scraper
Sadece Summary + Keeper Stats (FBRef'te bu kadar var)
"""

import pyodbc
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import re
import sys

sys.stdout.reconfigure(encoding='utf-8')

# Veritabani baglanti bilgileri
CONNECTION_STRING = (
    "DRIVER={SQL Server};"
    "SERVER=195.201.146.224,1433;"
    "DATABASE=FBREF;"
    "UID=sa;"
    "PWD=FbRef2024Str0ng;"
)

# Süper Lig 2025-2026
LEAGUE_CONFIG = {
    "name": "Süper Lig",
    "country": "TÜRKİYE",
    "season": "2025-2026",
    "url": "https://fbref.com/en/comps/26/2025-2026/schedule/2025-2026-Super-Lig-Scores-and-Fixtures",
    "table_id": "sched_2025-2026_26_1",
    "comp_id": 26,
    "lig_id": 4  # TANIM.LIG tablosundaki ID
}

# Test modu: None = tüm maçlar, sayı = limit
TEST_LIMIT = None


def get_db_connection():
    return pyodbc.connect(CONNECTION_STRING)


def get_html(url):
    """Playwright ile sayfa HTML'ini al"""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = context.new_page()
        print(f"  Sayfa yukleniyor: {url[:80]}...")
        page.goto(url, timeout=90000, wait_until="networkidle")
        page.wait_for_timeout(3000)
        html = page.content()
        browser.close()
    return html


def get_or_create_takim(conn, takim_adi, url=None, ulke="TÜRKİYE"):
    """Takımı getir veya oluştur"""
    cursor = conn.cursor()

    # Önce URL ile ara
    if url:
        cursor.execute("SELECT TAKIM_ID FROM TANIM.TAKIM WHERE URL = ?", url)
        row = cursor.fetchone()
        if row:
            return row[0]

    # İsimle ara
    cursor.execute("SELECT TAKIM_ID FROM TANIM.TAKIM WHERE TAKIM_ADI = ?", takim_adi)
    row = cursor.fetchone()
    if row:
        return row[0]

    # Yeni takım ekle
    cursor.execute("""
        INSERT INTO TANIM.TAKIM (TAKIM_ADI, URL, ULKE, KAYIT_TARIHI)
        VALUES (?, ?, ?, GETDATE())
    """, takim_adi, url, ulke)
    conn.commit()

    cursor.execute("SELECT @@IDENTITY")
    return cursor.fetchone()[0]


def get_or_create_oyuncu(conn, oyuncu_adi, url=None, ulke=None, pozisyon=None):
    """Oyuncuyu getir veya oluştur"""
    cursor = conn.cursor()

    # URL ile ara (UNIQUE constraint var)
    if url:
        cursor.execute("SELECT OYUNCU_ID FROM TANIM.OYUNCU WHERE URL = ?", url)
        row = cursor.fetchone()
        if row:
            return row[0]

    # Yeni oyuncu ekle
    cursor.execute("""
        INSERT INTO TANIM.OYUNCU (OYUNCU_ADI, URL, ULKE, POZISYON, KAYIT_TARIHI)
        VALUES (?, ?, ?, ?, GETDATE())
    """, oyuncu_adi, url, ulke, pozisyon)
    conn.commit()

    cursor.execute("SELECT @@IDENTITY")
    return cursor.fetchone()[0]


def create_fikstur(conn, match_data):
    """Fikstür kaydı oluştur"""
    cursor = conn.cursor()

    # Zaten var mı kontrol et
    cursor.execute("SELECT FIKSTURID FROM FIKSTUR.FIKSTUR WHERE URL = ?", match_data['url'])
    row = cursor.fetchone()
    if row:
        return row[0]

    cursor.execute("""
        INSERT INTO FIKSTUR.FIKSTUR (
            LIG_ID, SEZON_ID, EV_TAKIM_ID, DEP_TAKIM_ID,
            MAC_TARIHI, EV_SKOR, DEP_SKOR, URL, KAYIT_TARIHI
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
    """,
        LEAGUE_CONFIG['lig_id'],
        4,  # SEZON_ID for 2025-2026
        match_data['home_team_id'],
        match_data['away_team_id'],
        match_data.get('date'),
        match_data.get('home_score'),
        match_data.get('away_score'),
        match_data['url']
    )
    conn.commit()

    cursor.execute("SELECT @@IDENTITY")
    return cursor.fetchone()[0]


def parse_int(value):
    """String'i int'e çevir"""
    if not value or value == '':
        return None
    try:
        return int(value)
    except:
        return None


def parse_decimal(value):
    """String'i decimal'e çevir"""
    if not value or value == '':
        return None
    try:
        return float(value)
    except:
        return None


def parse_player_stats(soup, team_key, takim_id):
    """Oyuncu istatistiklerini parse et (Summary tablosu)"""
    players = {}

    # Summary tablosunu bul
    tables = soup.find_all('table', id=re.compile(r'stats_.*_summary$'))

    if len(tables) < 2:
        print(f"    UYARI: Summary tablosu bulunamadi!")
        return players

    table = tables[0] if team_key == 'home' else tables[1]

    tbody = table.find('tbody')
    if not tbody:
        return players

    for row in tbody.find_all('tr'):
        # Oyuncu linkini al
        player_cell = row.find('th', {'data-stat': 'player'})
        if not player_cell:
            continue

        player_link = player_cell.find('a')
        if not player_link:
            continue

        player_name = player_link.get_text(strip=True)
        player_url = "https://fbref.com" + player_link.get('href', '')

        # Temel bilgiler
        stats = {
            'name': player_name,
            'url': player_url,
            'team_key': team_key,
            'takim_id': takim_id,
        }

        # Summary sütunlarını parse et
        summary_mapping = {
            'shirtnumber': 'FORMA_NO',
            'nationality': 'ULKE',
            'position': 'POZISYON',
            'age': 'YAS',
            'minutes': 'SURE',
            'goals': 'GOL',
            'assists': 'ASIST',
            'pens_made': 'PENALTI_GOL',
            'pens_att': 'PENALTI_ATISI',
            'shots': 'SUT',
            'shots_on_target': 'ISABETLI_SUT',
            'cards_yellow': 'SARI_KART',
            'cards_red': 'KIRMIZI_KART',
            'fouls': 'FAUL_YAPILAN',
            'fouled': 'FAUL_MARUZ',
            'offsides': 'OFSAYT',
            'crosses': 'ORTA',
            'touches': 'TEMAS',
            'tackles': 'TOP_KAPMA',
            'interceptions': 'MUDAHALE',
            'blocks': 'BLOK',
            'xg': 'BEKLENEN_GOL',
            'npxg': 'PENALTISIZ_XG',
            'xg_assist': 'BEKLENEN_ASIST',
            'sca': 'SUT_YARATAN_AKSIYON',
            'gca': 'GOL_YARATAN_AKSIYON',
        }

        int_fields = ['FORMA_NO', 'SURE', 'GOL', 'ASIST', 'PENALTI_GOL', 'PENALTI_ATISI',
                      'SUT', 'ISABETLI_SUT', 'SARI_KART', 'KIRMIZI_KART', 'FAUL_YAPILAN',
                      'FAUL_MARUZ', 'OFSAYT', 'ORTA', 'TEMAS', 'TOP_KAPMA', 'MUDAHALE',
                      'BLOK', 'SUT_YARATAN_AKSIYON', 'GOL_YARATAN_AKSIYON']

        decimal_fields = ['BEKLENEN_GOL', 'PENALTISIZ_XG', 'BEKLENEN_ASIST']

        for data_stat, db_col in summary_mapping.items():
            cell = row.find('td', {'data-stat': data_stat})
            if cell:
                value = cell.get_text(strip=True)
                if db_col in int_fields:
                    stats[db_col] = parse_int(value)
                elif db_col in decimal_fields:
                    stats[db_col] = parse_decimal(value)
                else:
                    stats[db_col] = value if value else None

        players[player_url] = stats

    return players


def parse_keeper_stats(soup, team_key, takim_id):
    """Kaleci istatistiklerini parse et"""
    keepers = {}

    # Keeper tablosunu bul
    tables = soup.find_all('table', id=re.compile(r'keeper_stats_'))

    if len(tables) < 2:
        return keepers

    table = tables[0] if team_key == 'home' else tables[1]

    tbody = table.find('tbody')
    if not tbody:
        return keepers

    for row in tbody.find_all('tr'):
        player_cell = row.find('th', {'data-stat': 'player'})
        if not player_cell:
            continue

        player_link = player_cell.find('a')
        if not player_link:
            continue

        player_name = player_link.get_text(strip=True)
        player_url = "https://fbref.com" + player_link.get('href', '')

        stats = {
            'name': player_name,
            'url': player_url,
            'team_key': team_key,
            'takim_id': takim_id,
        }

        # Kaleci sütunları
        keeper_mapping = {
            'gk_shots_on_target_against': 'KALEYE_SUT',
            'gk_goals_against': 'YENILEN_GOL',
            'gk_saves': 'KURTARIS',
            'gk_save_pct': 'KURTARIS_YUZDESI',
            'gk_psxg': 'BEKLENEN_GOL_KURTARIS',
            'minutes': 'SURE',
            'age': 'YAS',
        }

        for data_stat, db_col in keeper_mapping.items():
            cell = row.find('td', {'data-stat': data_stat})
            if cell:
                value = cell.get_text(strip=True)
                if db_col in ['KALEYE_SUT', 'YENILEN_GOL', 'KURTARIS', 'SURE']:
                    stats[db_col] = parse_int(value)
                elif db_col in ['KURTARIS_YUZDESI', 'BEKLENEN_GOL_KURTARIS']:
                    stats[db_col] = parse_decimal(value)
                else:
                    stats[db_col] = value if value else None

        keepers[player_url] = stats

    return keepers


def save_performans(conn, fikstur_id, oyuncu_id, takim_id, stats):
    """Performans kaydını veritabanına kaydet"""
    cursor = conn.cursor()

    # Zaten var mı kontrol et
    cursor.execute("""
        SELECT PERFORMANS_ID FROM FIKSTUR.PERFORMANS
        WHERE FIKSTURID = ? AND OYUNCU_ID = ?
    """, fikstur_id, oyuncu_id)

    if cursor.fetchone():
        return  # Zaten var

    columns = ['FIKSTURID', 'OYUNCU_ID', 'TAKIM_ID', 'KAYIT_TARIHI']
    values = [fikstur_id, oyuncu_id, takim_id, datetime.now()]

    # İstatistikleri ekle
    stat_columns = ['FORMA_NO', 'POZISYON', 'YAS', 'SURE', 'GOL', 'ASIST',
                    'PENALTI_GOL', 'PENALTI_ATISI', 'SUT', 'ISABETLI_SUT',
                    'SARI_KART', 'KIRMIZI_KART', 'TEMAS', 'TOP_KAPMA',
                    'MUDAHALE', 'BLOK', 'BEKLENEN_GOL', 'PENALTISIZ_XG',
                    'BEKLENEN_ASIST', 'SUT_YARATAN_AKSIYON', 'GOL_YARATAN_AKSIYON']

    for col in stat_columns:
        if col in stats and stats[col] is not None:
            columns.append(col)
            values.append(stats[col])

    placeholders = ', '.join(['?' for _ in values])
    column_names = ', '.join(columns)

    cursor.execute(f"""
        INSERT INTO FIKSTUR.PERFORMANS ({column_names})
        VALUES ({placeholders})
    """, values)
    conn.commit()


def save_kaleci_performans(conn, fikstur_id, oyuncu_id, takim_id, stats):
    """Kaleci performans kaydını veritabanına kaydet"""
    cursor = conn.cursor()

    # Zaten var mı kontrol et
    cursor.execute("""
        SELECT KALECI_PERFORMANS_ID FROM FIKSTUR.KALECI_PERFORMANS
        WHERE FIKSTURID = ? AND OYUNCU_ID = ?
    """, fikstur_id, oyuncu_id)

    if cursor.fetchone():
        return

    columns = ['FIKSTURID', 'OYUNCU_ID', 'TAKIM_ID', 'KAYIT_TARIHI']
    values = [fikstur_id, oyuncu_id, takim_id, datetime.now()]

    stat_columns = ['SURE', 'YAS', 'KALEYE_SUT', 'YENILEN_GOL', 'KURTARIS',
                    'KURTARIS_YUZDESI', 'BEKLENEN_GOL_KURTARIS']

    for col in stat_columns:
        if col in stats and stats[col] is not None:
            columns.append(col)
            values.append(stats[col])

    placeholders = ', '.join(['?' for _ in values])
    column_names = ', '.join(columns)

    cursor.execute(f"""
        INSERT INTO FIKSTUR.KALECI_PERFORMANS ({column_names})
        VALUES ({placeholders})
    """, values)
    conn.commit()


def process_match(conn, match_url, home_team, away_team, score, match_date=None):
    """Tek bir maçı işle"""
    print(f"\n{'='*60}")
    print(f"MAC: {home_team} vs {away_team} ({score})")
    print(f"{'='*60}")

    # Maç sayfasını yükle
    html = get_html(match_url)
    soup = BeautifulSoup(html, 'html.parser')

    # Skoru parse et
    home_score, away_score = None, None
    if score and '–' in score:
        parts = score.split('–')
        home_score = parse_int(parts[0].strip())
        away_score = parse_int(parts[1].strip())

    # Takımları oluştur/getir
    home_team_id = get_or_create_takim(conn, home_team)
    away_team_id = get_or_create_takim(conn, away_team)
    print(f"  Takimlar: {home_team} (ID:{home_team_id}), {away_team} (ID:{away_team_id})")

    # Fikstür kaydı oluştur
    match_data = {
        'url': match_url,
        'home_team_id': home_team_id,
        'away_team_id': away_team_id,
        'home_score': home_score,
        'away_score': away_score,
        'date': match_date
    }
    fikstur_id = create_fikstur(conn, match_data)
    print(f"  Fikstur ID: {fikstur_id}")

    # Oyuncu istatistiklerini parse et
    home_players = parse_player_stats(soup, 'home', home_team_id)
    away_players = parse_player_stats(soup, 'away', away_team_id)
    all_players = {**home_players, **away_players}
    print(f"  Oyuncular: {len(home_players)} ev, {len(away_players)} deplasman")

    # Kaleci istatistiklerini parse et
    home_keepers = parse_keeper_stats(soup, 'home', home_team_id)
    away_keepers = parse_keeper_stats(soup, 'away', away_team_id)
    all_keepers = {**home_keepers, **away_keepers}
    print(f"  Kaleciler: {len(home_keepers)} ev, {len(away_keepers)} deplasman")

    # Veritabanına kaydet
    saved_players = 0
    saved_keepers = 0

    for player_url, stats in all_players.items():
        oyuncu_id = get_or_create_oyuncu(
            conn,
            stats['name'],
            player_url,
            stats.get('ULKE'),
            stats.get('POZISYON')
        )
        save_performans(conn, fikstur_id, oyuncu_id, stats['takim_id'], stats)
        saved_players += 1

    for keeper_url, stats in all_keepers.items():
        oyuncu_id = get_or_create_oyuncu(conn, stats['name'], keeper_url)
        save_kaleci_performans(conn, fikstur_id, oyuncu_id, stats['takim_id'], stats)
        saved_keepers += 1

    print(f"  Kaydedildi: {saved_players} oyuncu, {saved_keepers} kaleci")
    return True


def get_played_matches():
    """Oynanmış maçları getir"""
    print("Fikstur sayfasi yukleniyor...")
    html = get_html(LEAGUE_CONFIG['url'])
    soup = BeautifulSoup(html, 'html.parser')

    table = soup.find('table', id=LEAGUE_CONFIG['table_id'])
    if not table:
        print("HATA: Fikstur tablosu bulunamadi!")
        return []

    matches = []
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

        home_cell = row.find('td', {'data-stat': 'home_team'})
        away_cell = row.find('td', {'data-stat': 'away_team'})
        date_cell = row.find('td', {'data-stat': 'date'})

        home_team = home_cell.get_text(strip=True) if home_cell else "?"
        away_team = away_cell.get_text(strip=True) if away_cell else "?"
        match_date = date_cell.get_text(strip=True) if date_cell else None

        matches.append({
            'url': match_url,
            'home_team': home_team,
            'away_team': away_team,
            'score': score,
            'date': match_date
        })

    return matches


def main():
    print("=" * 70)
    print(f"SUPER LIG 2025-2026 SCRAPER")
    print(f"Sadece Summary + Keeper Stats")
    print("=" * 70)

    # Veritabanı bağlantısı
    conn = get_db_connection()
    print("Veritabani baglantisi basarili")

    # Oynanmış maçları getir
    matches = get_played_matches()
    print(f"\nToplam oynanan mac: {len(matches)}")

    if TEST_LIMIT:
        matches = matches[:TEST_LIMIT]
        print(f"TEST MODU: Sadece ilk {TEST_LIMIT} mac islenecek")

    # Her maçı işle
    success_count = 0
    for i, match in enumerate(matches, 1):
        print(f"\n[{i}/{len(matches)}] Isleniyor...")
        try:
            process_match(
                conn,
                match['url'],
                match['home_team'],
                match['away_team'],
                match['score'],
                match['date']
            )
            success_count += 1
        except Exception as e:
            print(f"  HATA: {e}")

    # Özet
    print("\n" + "=" * 70)
    print("OZET")
    print("=" * 70)
    print(f"Toplam mac: {len(matches)}")
    print(f"Basarili: {success_count}")
    print(f"Hatali: {len(matches) - success_count}")

    conn.close()
    print("\nTamamlandi!")


if __name__ == "__main__":
    main()
