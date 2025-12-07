# -*- coding: utf-8 -*-
"""
FBRef Summary-Only Scraper
Sadece Summary + Keeper Stats olan ligler için
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

# Summary-Only Ligler
SUMMARY_LEAGUES = [
    {"lig_id": 4, "name": "Süper Lig", "comp_id": 26, "url_name": "Super-Lig", "country": "TÜRKİYE"},
    {"lig_id": 13, "name": "Scottish Premiership", "comp_id": 40, "url_name": "Scottish-Premiership", "country": "İSKOÇYA"},
    {"lig_id": 16, "name": "Austrian Bundesliga", "comp_id": 56, "url_name": "Austrian-Bundesliga", "country": "AVUSTURYA"},
    {"lig_id": 18, "name": "Superliga", "comp_id": 50, "url_name": "Danish-Superliga", "country": "DANİMARKA"},
    {"lig_id": 19, "name": "Champions League", "comp_id": 8, "url_name": "Champions-League", "country": "AVRUPA"},
    {"lig_id": 20, "name": "Europa League", "comp_id": 19, "url_name": "Europa-League", "country": "AVRUPA"},
    {"lig_id": 21, "name": "Europa Conference League", "comp_id": 882, "url_name": "Europa-Conference-League", "country": "AVRUPA"},
    {"lig_id": 22, "name": "Serbian SuperLiga", "comp_id": 54, "url_name": "Serbian-SuperLiga", "country": "SIRBİSTAN"},
    {"lig_id": 23, "name": "Swiss Super League", "comp_id": 57, "url_name": "Swiss-Super-League", "country": "İSVİÇRE"},
    {"lig_id": 24, "name": "Ekstraklasa", "comp_id": 36, "url_name": "Ekstraklasa", "country": "POLONYA"},
    {"lig_id": 25, "name": "Super League Greece", "comp_id": 27, "url_name": "Super-League-Greece", "country": "YUNANİSTAN"},
    {"lig_id": 26, "name": "Czech First League", "comp_id": 66, "url_name": "Czech-First-League", "country": "ÇEKYA"},
    {"lig_id": 28, "name": "Veikkausliiga", "comp_id": 61, "url_name": "Veikkausliiga", "country": "FİNLANDİYA"},
    {"lig_id": 29, "name": "Eliteserien", "comp_id": 28, "url_name": "Eliteserien", "country": "NORVEÇ"},
    {"lig_id": 30, "name": "Allsvenskan", "comp_id": 29, "url_name": "Allsvenskan", "country": "İSVEÇ"},
]

SEASON = "2025-2026"
SEZON_ID = 4

# Summary tablosu mapping
SUMMARY_MAPPING = {
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
    'xg': 'BEKLENEN_GOL',
    'npxg': 'PENALTISIZ_XG',
    'xg_assist': 'BEKLENEN_ASIST',
    'sca': 'SUT_YARATAN_AKSIYON',
    'gca': 'GOL_YARATAN_AKSIYON',
    'touches': 'TEMAS',
    'tackles': 'TOP_KAPMA',
    'interceptions': 'MUDAHALE',
    'blocks': 'BLOK',
    'fouls': 'FAUL_YAPILAN',
    'fouled': 'FAUL_MARUZ',
    'offsides': 'OFSAYT',
    'crosses': 'ORTA',
}

INT_FIELDS = ['FORMA_NO', 'SURE', 'GOL', 'ASIST', 'PENALTI_GOL', 'PENALTI_ATISI',
              'SUT', 'ISABETLI_SUT', 'SARI_KART', 'KIRMIZI_KART', 'FAUL_YAPILAN',
              'FAUL_MARUZ', 'OFSAYT', 'ORTA', 'TEMAS', 'TOP_KAPMA', 'MUDAHALE',
              'BLOK', 'SUT_YARATAN_AKSIYON', 'GOL_YARATAN_AKSIYON']

DECIMAL_FIELDS = ['BEKLENEN_GOL', 'PENALTISIZ_XG', 'BEKLENEN_ASIST']


def get_db_connection():
    return pyodbc.connect(CONNECTION_STRING)


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


def parse_int(value):
    if not value or value == '':
        return None
    try:
        return int(value.replace(',', ''))
    except:
        return None


def parse_decimal(value):
    if not value or value == '':
        return None
    try:
        return float(value.replace(',', '.'))
    except:
        return None


def get_or_create_takim(conn, takim_adi, url=None, ulke=None):
    cursor = conn.cursor()
    if url:
        cursor.execute("SELECT TAKIM_ID FROM TANIM.TAKIM WHERE URL = ?", url)
        row = cursor.fetchone()
        if row:
            return row[0]

    cursor.execute("SELECT TAKIM_ID FROM TANIM.TAKIM WHERE TAKIM_ADI = ?", takim_adi)
    row = cursor.fetchone()
    if row:
        return row[0]

    cursor.execute("""
        INSERT INTO TANIM.TAKIM (TAKIM_ADI, URL, ULKE, KAYIT_TARIHI)
        VALUES (?, ?, ?, GETDATE())
    """, takim_adi, url, ulke)
    conn.commit()
    cursor.execute("SELECT @@IDENTITY")
    return cursor.fetchone()[0]


def get_or_create_oyuncu(conn, oyuncu_adi, url=None, ulke=None, pozisyon=None):
    cursor = conn.cursor()
    if url:
        cursor.execute("SELECT OYUNCU_ID FROM TANIM.OYUNCU WHERE URL = ?", url)
        row = cursor.fetchone()
        if row:
            return row[0]

    cursor.execute("""
        INSERT INTO TANIM.OYUNCU (OYUNCU_ADI, URL, ULKE, POZISYON, KAYIT_TARIHI)
        VALUES (?, ?, ?, ?, GETDATE())
    """, oyuncu_adi, url, ulke, pozisyon)
    conn.commit()
    cursor.execute("SELECT @@IDENTITY")
    return cursor.fetchone()[0]


def create_fikstur(conn, match_data, lig_id):
    cursor = conn.cursor()
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
        lig_id, SEZON_ID, match_data['home_team_id'], match_data['away_team_id'],
        match_data.get('date'), match_data.get('home_score'), match_data.get('away_score'),
        match_data['url']
    )
    conn.commit()
    cursor.execute("SELECT @@IDENTITY")
    return cursor.fetchone()[0]


def parse_player_stats(soup, team_key, takim_id):
    """Summary tablosunu parse et"""
    players = {}
    tables = soup.find_all('table', id=re.compile(r'stats_.*_summary$'))

    if len(tables) < 2:
        return players

    table = tables[0] if team_key == 'home' else tables[1]
    tbody = table.find('tbody')
    if not tbody:
        return players

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

        for data_stat, db_col in SUMMARY_MAPPING.items():
            cell = row.find('td', {'data-stat': data_stat})
            if cell:
                value = cell.get_text(strip=True)
                if db_col in INT_FIELDS:
                    stats[db_col] = parse_int(value)
                elif db_col in DECIMAL_FIELDS:
                    stats[db_col] = parse_decimal(value)
                else:
                    stats[db_col] = value if value else None

        players[player_url] = stats

    return players


def parse_keeper_stats(soup, team_key, takim_id):
    """Kaleci istatistiklerini parse et"""
    keepers = {}
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
    cursor = conn.cursor()
    cursor.execute("""
        SELECT PERFORMANS_ID FROM FIKSTUR.PERFORMANS
        WHERE FIKSTURID = ? AND OYUNCU_ID = ?
    """, fikstur_id, oyuncu_id)

    if cursor.fetchone():
        return

    columns = ['FIKSTURID', 'OYUNCU_ID', 'TAKIM_ID', 'KAYIT_TARIHI']
    values = [fikstur_id, oyuncu_id, takim_id, datetime.now()]

    for col in INT_FIELDS + DECIMAL_FIELDS + ['POZISYON', 'YAS', 'ULKE']:
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
    cursor = conn.cursor()
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


def get_played_matches(html, table_id):
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table', id=table_id)
    if not table:
        table = soup.find('table', id=re.compile(r'sched_'))

    if not table:
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
            continue

        score = score_link.get_text(strip=True)
        match_url = "https://fbref.com" + score_link.get('href', '')

        home_cell = row.find('td', {'data-stat': 'home_team'})
        away_cell = row.find('td', {'data-stat': 'away_team'})
        date_cell = row.find('td', {'data-stat': 'date'})

        matches.append({
            'url': match_url,
            'home_team': home_cell.get_text(strip=True) if home_cell else "?",
            'away_team': away_cell.get_text(strip=True) if away_cell else "?",
            'score': score,
            'date': date_cell.get_text(strip=True) if date_cell else None
        })

    return matches


def process_match(conn, match, lig_id, ulke):
    print(f"    {match['home_team']} vs {match['away_team']} ({match['score']})")

    html = get_html(match['url'])
    if not html:
        return False

    soup = BeautifulSoup(html, 'html.parser')

    home_score, away_score = None, None
    if match['score'] and '–' in match['score']:
        parts = match['score'].split('–')
        home_score = parse_int(parts[0].strip())
        away_score = parse_int(parts[1].strip())

    home_team_id = get_or_create_takim(conn, match['home_team'], ulke=ulke)
    away_team_id = get_or_create_takim(conn, match['away_team'], ulke=ulke)

    match_data = {
        'url': match['url'],
        'home_team_id': home_team_id,
        'away_team_id': away_team_id,
        'home_score': home_score,
        'away_score': away_score,
        'date': match.get('date')
    }
    fikstur_id = create_fikstur(conn, match_data, lig_id)

    home_players = parse_player_stats(soup, 'home', home_team_id)
    away_players = parse_player_stats(soup, 'away', away_team_id)
    all_players = {**home_players, **away_players}

    home_keepers = parse_keeper_stats(soup, 'home', home_team_id)
    away_keepers = parse_keeper_stats(soup, 'away', away_team_id)
    all_keepers = {**home_keepers, **away_keepers}

    for player_url, stats in all_players.items():
        oyuncu_id = get_or_create_oyuncu(
            conn, stats['name'], player_url,
            stats.get('ULKE'), stats.get('POZISYON')
        )
        save_performans(conn, fikstur_id, oyuncu_id, stats['takim_id'], stats)

    for keeper_url, stats in all_keepers.items():
        oyuncu_id = get_or_create_oyuncu(conn, stats['name'], keeper_url)
        save_kaleci_performans(conn, fikstur_id, oyuncu_id, stats['takim_id'], stats)

    print(f"      -> {len(all_players)} oyuncu, {len(all_keepers)} kaleci")
    return True


def scrape_league(conn, league, test_limit=None):
    print(f"\n{'='*60}")
    print(f"{league['name']} ({league['country']})")
    print(f"{'='*60}")

    fixture_url = f"https://fbref.com/en/comps/{league['comp_id']}/{SEASON}/schedule/{SEASON}-{league['url_name']}-Scores-and-Fixtures"
    table_id = f"sched_{SEASON}_{league['comp_id']}_1"

    print("Fikstur sayfasi yukleniyor...")
    fixture_html = get_html(fixture_url)
    if not fixture_html:
        print("HATA: Fikstur sayfasi yuklenemedi")
        return 0

    matches = get_played_matches(fixture_html, table_id)
    print(f"Oynanan mac sayisi: {len(matches)}")

    if test_limit:
        matches = matches[:test_limit]
        print(f"TEST: Sadece ilk {test_limit} mac")

    success = 0
    for i, match in enumerate(matches, 1):
        print(f"  [{i}/{len(matches)}]", end=" ")
        try:
            if process_match(conn, match, league['lig_id'], league['country']):
                success += 1
        except Exception as e:
            print(f"HATA: {e}")

    return success


def main(selected_leagues=None, test_limit=None):
    print("=" * 70)
    print("FBREF SUMMARY-ONLY SCRAPER")
    print("Sadece Summary + Keeper Stats")
    print("=" * 70)

    conn = get_db_connection()
    print("Veritabani baglantisi basarili")

    leagues = selected_leagues if selected_leagues else SUMMARY_LEAGUES

    total_matches = 0
    for league in leagues:
        success = scrape_league(conn, league, test_limit)
        total_matches += success

    print("\n" + "=" * 70)
    print(f"TOPLAM: {total_matches} mac islendi")
    print("=" * 70)

    conn.close()


if __name__ == "__main__":
    # Test: Her ligden 1 maç
    main(test_limit=1)
