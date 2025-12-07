"""
FbrefSolution - Kapsamli Futbol Istatistik Scraper v2
Tablolar: FIKSTUR, DETAY, TAKIM, OYUNCU, PERFORMANS, KALECI_PERFORMANS
"""

import pyodbc
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import re
import webbrowser
import os

# Veritabani baglanti bilgileri
CONNECTION_STRING = (
    "DRIVER={SQL Server};"
    "SERVER=195.201.146.224,1433;"
    "DATABASE=FBREF;"
    "UID=sa;"
    "PWD=FbRef2024Str0ng;"
)

# Premier Lig 2025-26 (Test icin)
LEAGUES = [
    {
        "name": "Premier League",
        "country": "INGILTERE",
        "season": "2025-2026",
        "url": "https://fbref.com/en/comps/9/2025-2026/schedule/2025-2026-Premier-League-Scores-and-Fixtures",
        "table_id": "sched_2025-2026_9_1",
        "comp_id": 9
    }
]

# Test: Sadece ilk N mac
TEST_LIMIT = 3


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
        try:
            page.goto(url, timeout=90000, wait_until="networkidle")
            page.wait_for_timeout(3000)
            html = page.content()
        finally:
            browser.close()
    return html


# ============== TAKIM ISLEMLERI ==============

def get_or_create_team(cursor, team_name, team_url=None, country=None):
    """Takim varsa ID dondur, yoksa olustur"""
    cursor.execute("SELECT TAKIM_ID FROM TANIM.TAKIM WHERE TAKIM_ADI = ?", team_name)
    row = cursor.fetchone()
    if row:
        return row[0]

    cursor.execute("""
        INSERT INTO TANIM.TAKIM (TAKIM_ADI, URL, ULKE, KAYIT_TARIHI)
        VALUES (?, ?, ?, GETDATE())
    """, team_name, team_url, country)
    cursor.execute("SELECT TAKIM_ID FROM TANIM.TAKIM WHERE TAKIM_ADI = ?", team_name)
    return cursor.fetchone()[0]


# ============== OYUNCU ISLEMLERI ==============

def get_or_create_player(cursor, player_name, player_url=None, country=None, position=None):
    """Oyuncu varsa ID dondur, yoksa olustur"""
    # URL ile kontrol (daha guvenilir)
    if player_url:
        cursor.execute("SELECT OYUNCU_ID FROM TANIM.OYUNCU WHERE URL = ?", player_url)
        row = cursor.fetchone()
        if row:
            return row[0]

    # Isim ile kontrol
    cursor.execute("SELECT OYUNCU_ID FROM TANIM.OYUNCU WHERE OYUNCU_ADI = ?", player_name)
    row = cursor.fetchone()
    if row:
        return row[0]

    # Yeni oyuncu olustur
    cursor.execute("""
        INSERT INTO TANIM.OYUNCU (OYUNCU_ADI, URL, ULKE, POZISYON, KAYIT_TARIHI)
        VALUES (?, ?, ?, ?, GETDATE())
    """, player_name, player_url, country, position)
    cursor.execute("SELECT OYUNCU_ID FROM TANIM.OYUNCU WHERE OYUNCU_ADI = ?", player_name)
    return cursor.fetchone()[0]


# ============== SEZON/LIG ISLEMLERI ==============

def ensure_season(cursor, season_name):
    cursor.execute("SELECT SEZON_ID FROM FIKSTUR.SEZON WHERE SEZON = ?", season_name)
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute("""
        INSERT INTO FIKSTUR.SEZON (SEZON, DURUM, SON_ISLEM_ZAMANI)
        VALUES (?, 1, GETDATE())
    """, season_name)
    cursor.execute("SELECT SEZON_ID FROM FIKSTUR.SEZON WHERE SEZON = ?", season_name)
    return cursor.fetchone()[0]


def ensure_league(cursor, season_id, league_name, url, table_id, country):
    cursor.execute("""
        SELECT LIG_ID FROM TANIM.LIG WHERE SEZON_ID = ? AND LIG_ADI = ?
    """, season_id, league_name)
    row = cursor.fetchone()
    if row:
        cursor.execute("""
            UPDATE TANIM.LIG SET URL = ?, FIKSTUR_TABLO_ID = ?, SON_ISLEM_ZAMANI = GETDATE()
            WHERE LIG_ID = ?
        """, url, table_id, row[0])
        return row[0]

    cursor.execute("""
        INSERT INTO TANIM.LIG (LIG_ADI, URL, ULKE, SEZON, FIKSTUR_TABLO_ID, DURUM, SEZON_ID, SON_ISLEM_ZAMANI)
        VALUES (?, ?, ?, ?, ?, 1, ?, GETDATE())
    """, league_name, url, country, league_name, table_id, season_id)
    cursor.execute("SELECT LIG_ID FROM TANIM.LIG WHERE SEZON_ID = ? AND LIG_ADI = ?", season_id, league_name)
    return cursor.fetchone()[0]


# ============== FIKSTUR ISLEMLERI ==============

def parse_fixtures_from_list(html, table_id):
    """Fikstur listesinden maclari parse et"""
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table', id=table_id)
    if not table:
        return []

    fixtures = []
    rows = table.find_all('tr')

    for row in rows:
        cells = row.find_all(['td', 'th'])
        if len(cells) < 7:
            continue

        # Skoru olan (oynanmis) maclari al
        score_cell = row.find('td', {'data-stat': 'score'})
        if not score_cell:
            continue

        score_link = score_cell.find('a')
        if not score_link or not score_link.get_text(strip=True):
            continue  # Oynanmamis mac

        # Verileri cek
        week = row.find('th', {'data-stat': 'gameweek'})
        date = row.find('td', {'data-stat': 'date'})
        time = row.find('td', {'data-stat': 'time'})
        home = row.find('td', {'data-stat': 'home_team'})
        away = row.find('td', {'data-stat': 'away_team'})
        attendance = row.find('td', {'data-stat': 'attendance'})
        venue = row.find('td', {'data-stat': 'venue'})
        referee = row.find('td', {'data-stat': 'referee'})

        fixture = {
            'week': int(week.get_text(strip=True)) if week and week.get_text(strip=True).isdigit() else None,
            'date': date.get_text(strip=True) if date else None,
            'time': time.get_text(strip=True) if time else None,
            'home': home.get_text(strip=True) if home else None,
            'away': away.get_text(strip=True) if away else None,
            'score': score_link.get_text(strip=True),
            'match_url': "https://fbref.com" + score_link.get('href') if score_link.get('href') else None,
            'attendance': int(attendance.get_text(strip=True).replace(',', '')) if attendance and attendance.get_text(strip=True).replace(',', '').isdigit() else None,
            'venue': venue.get_text(strip=True) if venue else None,
            'referee': referee.get_text(strip=True) if referee else None
        }

        # Takim URL'lerini al
        home_link = home.find('a') if home else None
        away_link = away.find('a') if away else None
        fixture['home_url'] = "https://fbref.com" + home_link.get('href') if home_link else None
        fixture['away_url'] = "https://fbref.com" + away_link.get('href') if away_link else None

        fixtures.append(fixture)

    return fixtures


def save_fixture(cursor, fixture, league_id):
    """Fiksturu kaydet ve FIKSTURID dondur"""
    match_date = None
    if fixture['date']:
        try:
            match_date = datetime.strptime(fixture['date'], "%Y-%m-%d")
        except:
            pass

    # Mevcut kayit kontrolu
    cursor.execute("""
        SELECT FIKSTURID FROM FIKSTUR.FIKSTUR
        WHERE LIG_ID = ? AND TARIH = ? AND EVSAHIBI = ? AND MISAFIR = ?
    """, league_id, match_date, fixture['home'], fixture['away'])

    row = cursor.fetchone()
    if row:
        fixture_id = row[0]
        cursor.execute("""
            UPDATE FIKSTUR.FIKSTUR
            SET HAFTA = ?, SKOR = ?, SEYIRCI = ?, STADYUM = ?, HAKEM = ?, URL = ?, DEGISIKLIK_TARIHI = GETDATE()
            WHERE FIKSTURID = ?
        """, fixture['week'], fixture['score'], fixture['attendance'],
             fixture['venue'], fixture['referee'], fixture['match_url'], fixture_id)
    else:
        cursor.execute("""
            INSERT INTO FIKSTUR.FIKSTUR
            (LIG_ID, HAFTA, TARIH, EVSAHIBI, SKOR, MISAFIR, SEYIRCI, STADYUM, HAKEM, URL, DURUM, KAYIT_TARIHI, DEGISIKLIK_TARIHI)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, GETDATE(), GETDATE())
        """, league_id, fixture['week'], match_date, fixture['home'], fixture['score'],
             fixture['away'], fixture['attendance'], fixture['venue'], fixture['referee'], fixture['match_url'])

        cursor.execute("""
            SELECT FIKSTURID FROM FIKSTUR.FIKSTUR
            WHERE LIG_ID = ? AND TARIH = ? AND EVSAHIBI = ? AND MISAFIR = ?
        """, league_id, match_date, fixture['home'], fixture['away'])
        fixture_id = cursor.fetchone()[0]

    return fixture_id


# ============== MAC DETAY ISLEMLERI ==============

def parse_match_details(html):
    """Mac detay sayfasindan tum verileri cek"""
    soup = BeautifulSoup(html, 'html.parser')

    details = {
        'home_manager': None,
        'away_manager': None,
        'home_formation': None,
        'away_formation': None,
        'home_possession': None,
        'away_possession': None,
        'home_players': [],
        'away_players': [],
        'home_keeper': None,
        'away_keeper': None
    }

    # Lineup/Formation
    lineups = soup.find_all('div', class_='lineup')
    formations = []
    for lineup in lineups:
        th = lineup.find('th')
        if th:
            text = th.get_text(strip=True)
            if re.match(r'^\d+-\d+(-\d+)?(-\d+)?$', text):
                formations.append(text)

    if len(formations) >= 2:
        details['home_formation'] = formations[0]
        details['away_formation'] = formations[1]

    # Possession - team_stats_extra
    team_stats = soup.find('div', id='team_stats_extra')
    if team_stats:
        text = team_stats.get_text()
        poss = re.findall(r'(\d+)%', text)
        if len(poss) >= 2:
            details['home_possession'] = poss[0]
            details['away_possession'] = poss[1]

    # Player Stats - Summary tablolari
    stats_tables = soup.find_all('table', id=re.compile(r'stats_.*_summary'))

    for i, table in enumerate(stats_tables[:2]):  # Ev ve misafir
        players = parse_player_stats(table)
        if i == 0:
            details['home_players'] = players
        else:
            details['away_players'] = players

    # Keeper Stats
    keeper_tables = soup.find_all('table', id=re.compile(r'keeper_stats_'))
    for i, table in enumerate(keeper_tables[:2]):
        keeper = parse_keeper_stats(table)
        if keeper:
            if i == 0:
                details['home_keeper'] = keeper
            else:
                details['away_keeper'] = keeper

    return details


def parse_player_stats(table):
    """Oyuncu istatistik tablosunu parse et"""
    players = []
    tbody = table.find('tbody')
    if not tbody:
        return players

    for row in tbody.find_all('tr'):
        # Toplam satiri atla
        if 'thead' in row.get('class', []):
            continue

        player = {}

        # Oyuncu bilgisi
        player_cell = row.find('th', {'data-stat': 'player'})
        if player_cell:
            player['name'] = player_cell.get_text(strip=True)
            link = player_cell.find('a')
            player['url'] = "https://fbref.com" + link.get('href') if link else None
        else:
            continue

        # Diger istatistikler
        stat_mapping = {
            'shirtnumber': 'shirt_number',
            'nationality': 'nationality',
            'position': 'position',
            'age': 'age',
            'minutes': 'minutes',
            'goals': 'goals',
            'assists': 'assists',
            'pens_made': 'pens_made',
            'pens_att': 'pens_att',
            'shots': 'shots',
            'shots_on_target': 'shots_on_target',
            'cards_yellow': 'yellow_cards',
            'cards_red': 'red_cards',
            'touches': 'touches',
            'tackles': 'tackles',
            'interceptions': 'interceptions',
            'blocks': 'blocks',
            'xg': 'xg',
            'npxg': 'npxg',
            'xg_assist': 'xag',
            'sca': 'sca',
            'gca': 'gca',
            'passes_completed': 'passes_completed',
            'passes': 'passes_attempted',
            'passes_pct': 'pass_pct',
            'progressive_passes': 'progressive_passes',
            'carries': 'carries',
            'progressive_carries': 'progressive_carries',
            'take_ons': 'take_ons_attempted',
            'take_ons_won': 'take_ons_won'
        }

        for data_stat, key in stat_mapping.items():
            cell = row.find('td', {'data-stat': data_stat})
            if cell:
                value = cell.get_text(strip=True)
                # Sayisal degerleri donustur
                if key in ['shirt_number', 'minutes', 'goals', 'assists', 'pens_made', 'pens_att',
                           'shots', 'shots_on_target', 'yellow_cards', 'red_cards', 'touches',
                           'tackles', 'interceptions', 'blocks', 'sca', 'gca',
                           'passes_completed', 'passes_attempted', 'progressive_passes',
                           'carries', 'progressive_carries', 'take_ons_attempted', 'take_ons_won']:
                    player[key] = int(value) if value.isdigit() else None
                elif key in ['xg', 'npxg', 'xag', 'pass_pct']:
                    try:
                        player[key] = float(value) if value else None
                    except:
                        player[key] = None
                else:
                    player[key] = value if value else None

        players.append(player)

    return players


def parse_keeper_stats(table):
    """Kaleci istatistik tablosunu parse et"""
    tbody = table.find('tbody')
    if not tbody:
        return None

    row = tbody.find('tr')
    if not row:
        return None

    keeper = {}

    player_cell = row.find('th', {'data-stat': 'player'})
    if player_cell:
        keeper['name'] = player_cell.get_text(strip=True)
        link = player_cell.find('a')
        keeper['url'] = "https://fbref.com" + link.get('href') if link else None

    # Kaleci istatistikleri
    keeper_stats = {
        'shirtnumber': 'shirt_number',
        'age': 'age',
        'minutes': 'minutes',
        'gk_shots_on_target_against': 'shots_against',
        'gk_goals_against': 'goals_against',
        'gk_saves': 'saves',
        'gk_save_pct': 'save_pct',
        'gk_psxg': 'psxg'
    }

    for data_stat, key in keeper_stats.items():
        cell = row.find('td', {'data-stat': data_stat})
        if cell:
            value = cell.get_text(strip=True)
            if key in ['shirt_number', 'minutes', 'shots_against', 'goals_against', 'saves']:
                keeper[key] = int(value) if value.isdigit() else None
            elif key in ['save_pct', 'psxg']:
                try:
                    keeper[key] = float(value) if value else None
                except:
                    keeper[key] = None
            else:
                keeper[key] = value

    return keeper


def save_match_details(cursor, fixture_id, details, home_team_id, away_team_id):
    """Mac detaylarini ve performanslari kaydet"""

    # DETAY tablosuna kaydet
    cursor.execute("SELECT DETAY_ID FROM FIKSTUR.DETAY WHERE FIKSTURID = ?", fixture_id)
    if cursor.fetchone():
        cursor.execute("""
            UPDATE FIKSTUR.DETAY
            SET EV_MENAJER = ?, MISAFIR_MENAJER = ?, EV_DIZILIS = ?, MISAFIR_DIZILIS = ?,
                EV_SAHIPOLMA = ?, MISAFIR_SAHIPOLMA = ?, DEGISIKLIK_TARIHI = GETDATE()
            WHERE FIKSTURID = ?
        """, details['home_manager'], details['away_manager'],
             details['home_formation'], details['away_formation'],
             details['home_possession'], details['away_possession'], fixture_id)
    else:
        cursor.execute("""
            INSERT INTO FIKSTUR.DETAY
            (FIKSTURID, EV_MENAJER, MISAFIR_MENAJER, EV_DIZILIS, MISAFIR_DIZILIS,
             EV_SAHIPOLMA, MISAFIR_SAHIPOLMA, KAYIT_TARIHI, DEGISIKLIK_TARIHI)
            VALUES (?, ?, ?, ?, ?, ?, ?, GETDATE(), GETDATE())
        """, fixture_id, details['home_manager'], details['away_manager'],
             details['home_formation'], details['away_formation'],
             details['home_possession'], details['away_possession'])

    # Ev sahibi oyuncu performanslari
    for player in details['home_players']:
        save_player_performance(cursor, fixture_id, home_team_id, player)

    # Misafir oyuncu performanslari
    for player in details['away_players']:
        save_player_performance(cursor, fixture_id, away_team_id, player)

    # Kaleci performanslari
    if details['home_keeper']:
        save_keeper_performance(cursor, fixture_id, home_team_id, details['home_keeper'])
    if details['away_keeper']:
        save_keeper_performance(cursor, fixture_id, away_team_id, details['away_keeper'])


def save_player_performance(cursor, fixture_id, team_id, player):
    """Oyuncu performansini kaydet"""
    if not player.get('name'):
        return

    # Oyuncuyu bul veya olustur
    player_id = get_or_create_player(
        cursor,
        player['name'],
        player.get('url'),
        player.get('nationality'),
        player.get('position')
    )

    # Mevcut performans kontrolu
    cursor.execute("""
        SELECT PERFORMANS_ID FROM FIKSTUR.PERFORMANS
        WHERE FIKSTURID = ? AND OYUNCU_ID = ?
    """, fixture_id, player_id)

    if cursor.fetchone():
        cursor.execute("""
            UPDATE FIKSTUR.PERFORMANS SET
                TAKIM_ID = ?, FORMA_NO = ?, POZISYON = ?, YAS = ?, SURE = ?,
                GOL = ?, ASIST = ?, PENALTI_GOL = ?, PENALTI_ATISI = ?,
                SUT = ?, ISABETLI_SUT = ?, SARI_KART = ?, KIRMIZI_KART = ?,
                TEMAS = ?, TOP_KAPMA = ?, MUDAHALE = ?, BLOK = ?,
                BEKLENEN_GOL = ?, PENALTISIZ_XG = ?, BEKLENEN_ASIST = ?,
                SUT_YARATAN_AKSIYON = ?, GOL_YARATAN_AKSIYON = ?,
                BASARILI_PAS = ?, PAS_DENEMESI = ?, PAS_ISABET = ?, ILERIYE_PAS = ?,
                TOP_TASIMA = ?, ILERIYE_TASIMA = ?,
                CARPISMA_GIRISIMI = ?, BASARILI_CARPISMA = ?,
                DEGISIKLIK_TARIHI = GETDATE()
            WHERE FIKSTURID = ? AND OYUNCU_ID = ?
        """, team_id, player.get('shirt_number'), player.get('position'), player.get('age'),
             player.get('minutes'), player.get('goals'), player.get('assists'),
             player.get('pens_made'), player.get('pens_att'),
             player.get('shots'), player.get('shots_on_target'),
             player.get('yellow_cards'), player.get('red_cards'),
             player.get('touches'), player.get('tackles'),
             player.get('interceptions'), player.get('blocks'),
             player.get('xg'), player.get('npxg'), player.get('xag'),
             player.get('sca'), player.get('gca'),
             player.get('passes_completed'), player.get('passes_attempted'),
             player.get('pass_pct'), player.get('progressive_passes'),
             player.get('carries'), player.get('progressive_carries'),
             player.get('take_ons_attempted'), player.get('take_ons_won'),
             fixture_id, player_id)
    else:
        cursor.execute("""
            INSERT INTO FIKSTUR.PERFORMANS
            (FIKSTURID, OYUNCU_ID, TAKIM_ID, FORMA_NO, POZISYON, YAS, SURE,
             GOL, ASIST, PENALTI_GOL, PENALTI_ATISI, SUT, ISABETLI_SUT,
             SARI_KART, KIRMIZI_KART, TEMAS, TOP_KAPMA, MUDAHALE, BLOK,
             BEKLENEN_GOL, PENALTISIZ_XG, BEKLENEN_ASIST,
             SUT_YARATAN_AKSIYON, GOL_YARATAN_AKSIYON,
             BASARILI_PAS, PAS_DENEMESI, PAS_ISABET, ILERIYE_PAS,
             TOP_TASIMA, ILERIYE_TASIMA, CARPISMA_GIRISIMI, BASARILI_CARPISMA,
             KAYIT_TARIHI, DEGISIKLIK_TARIHI)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), GETDATE())
        """, fixture_id, player_id, team_id,
             player.get('shirt_number'), player.get('position'), player.get('age'),
             player.get('minutes'), player.get('goals'), player.get('assists'),
             player.get('pens_made'), player.get('pens_att'),
             player.get('shots'), player.get('shots_on_target'),
             player.get('yellow_cards'), player.get('red_cards'),
             player.get('touches'), player.get('tackles'),
             player.get('interceptions'), player.get('blocks'),
             player.get('xg'), player.get('npxg'), player.get('xag'),
             player.get('sca'), player.get('gca'),
             player.get('passes_completed'), player.get('passes_attempted'),
             player.get('pass_pct'), player.get('progressive_passes'),
             player.get('carries'), player.get('progressive_carries'),
             player.get('take_ons_attempted'), player.get('take_ons_won'))


def save_keeper_performance(cursor, fixture_id, team_id, keeper):
    """Kaleci performansini kaydet"""
    if not keeper.get('name'):
        return

    player_id = get_or_create_player(cursor, keeper['name'], keeper.get('url'), None, 'GK')

    cursor.execute("""
        SELECT KALECI_PERFORMANS_ID FROM FIKSTUR.KALECI_PERFORMANS
        WHERE FIKSTURID = ? AND OYUNCU_ID = ?
    """, fixture_id, player_id)

    if cursor.fetchone():
        cursor.execute("""
            UPDATE FIKSTUR.KALECI_PERFORMANS SET
                TAKIM_ID = ?, FORMA_NO = ?, YAS = ?, SURE = ?,
                KALEYE_SUT = ?, YENILEN_GOL = ?, KURTARIS = ?,
                KURTARIS_YUZDESI = ?, BEKLENEN_GOL_KURTARIS = ?,
                DEGISIKLIK_TARIHI = GETDATE()
            WHERE FIKSTURID = ? AND OYUNCU_ID = ?
        """, team_id, keeper.get('shirt_number'), keeper.get('age'), keeper.get('minutes'),
             keeper.get('shots_against'), keeper.get('goals_against'), keeper.get('saves'),
             keeper.get('save_pct'), keeper.get('psxg'),
             fixture_id, player_id)
    else:
        cursor.execute("""
            INSERT INTO FIKSTUR.KALECI_PERFORMANS
            (FIKSTURID, OYUNCU_ID, TAKIM_ID, FORMA_NO, YAS, SURE,
             KALEYE_SUT, YENILEN_GOL, KURTARIS, KURTARIS_YUZDESI, BEKLENEN_GOL_KURTARIS,
             KAYIT_TARIHI, DEGISIKLIK_TARIHI)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), GETDATE())
        """, fixture_id, player_id, team_id,
             keeper.get('shirt_number'), keeper.get('age'), keeper.get('minutes'),
             keeper.get('shots_against'), keeper.get('goals_against'), keeper.get('saves'),
             keeper.get('save_pct'), keeper.get('psxg'))


# ============== ANA PROGRAM ==============

def main():
    print("=" * 70)
    print("FbrefSolution - Kapsamli Futbol Istatistik Scraper v2")
    print("=" * 70)

    conn = get_db_connection()
    cursor = conn.cursor()
    print("[OK] Veritabani baglantisi basarili")

    for league in LEAGUES:
        print(f"\n{'=' * 70}")
        print(f"LIG: {league['name']} - {league['season']}")
        print("=" * 70)

        # Sezon ve Lig kaydi
        season_id = ensure_season(cursor, league['season'])
        league_id = ensure_league(cursor, season_id, league['name'], league['url'],
                                  league['table_id'], league['country'])
        conn.commit()

        # Fikstur listesini cek
        print("\n[1] Fikstur listesi yukleniyor...")
        fixture_html = get_html(league['url'])

        fixtures = parse_fixtures_from_list(fixture_html, league['table_id'])
        print(f"    {len(fixtures)} oynanmis mac bulundu")

        # Test limiti
        if TEST_LIMIT:
            fixtures = fixtures[:TEST_LIMIT]
            print(f"    TEST: Sadece ilk {TEST_LIMIT} mac islenecek")

        # Her mac icin
        for i, fixture in enumerate(fixtures, 1):
            print(f"\n[MAC {i}/{len(fixtures)}] {fixture['home']} vs {fixture['away']} ({fixture['score']})")

            # Takimlari kaydet
            home_team_id = get_or_create_team(cursor, fixture['home'], fixture.get('home_url'), league['country'])
            away_team_id = get_or_create_team(cursor, fixture['away'], fixture.get('away_url'), league['country'])

            # Fiksturu kaydet
            fixture_id = save_fixture(cursor, fixture, league_id)
            print(f"    Fikstur ID: {fixture_id}")

            # Mac detay sayfasini cek
            if fixture['match_url']:
                print(f"    Mac detay sayfasi yukleniyor...")
                try:
                    match_html = get_html(fixture['match_url'])

                    # Detaylari parse et
                    details = parse_match_details(match_html)
                    print(f"    Dizilis: {details['home_formation']} vs {details['away_formation']}")
                    print(f"    Possession: {details['home_possession']}% - {details['away_possession']}%")
                    print(f"    Oyuncular: {len(details['home_players'])} + {len(details['away_players'])}")

                    # Kaydet
                    save_match_details(cursor, fixture_id, details, home_team_id, away_team_id)
                    conn.commit()
                    print(f"    [OK] Tum veriler kaydedildi!")

                except Exception as e:
                    print(f"    [HATA] {e}")

        conn.commit()

    cursor.close()
    conn.close()
    print(f"\n{'=' * 70}")
    print("[OK] Islem tamamlandi!")


if __name__ == "__main__":
    main()
