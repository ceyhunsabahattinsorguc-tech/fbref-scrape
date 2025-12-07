"""
FbrefSolution - Kapsamli Futbol Istatistik Scraper v3
TUM SEKMELER: Summary, Passing, Pass Types, Defensive, Possession, Misc
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


# ============== TAKIM/OYUNCU ISLEMLERI ==============

def get_or_create_team(cursor, team_name, team_url=None, country=None):
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


def get_or_create_player(cursor, player_name, player_url=None, country=None, position=None):
    if player_url:
        cursor.execute("SELECT OYUNCU_ID FROM TANIM.OYUNCU WHERE URL = ?", player_url)
        row = cursor.fetchone()
        if row:
            return row[0]
    cursor.execute("SELECT OYUNCU_ID FROM TANIM.OYUNCU WHERE OYUNCU_ADI = ?", player_name)
    row = cursor.fetchone()
    if row:
        return row[0]
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
        return row[0]
    cursor.execute("""
        INSERT INTO TANIM.LIG (LIG_ADI, URL, ULKE, SEZON, FIKSTUR_TABLO_ID, DURUM, SEZON_ID, SON_ISLEM_ZAMANI)
        VALUES (?, ?, ?, ?, ?, 1, ?, GETDATE())
    """, league_name, url, country, league_name, table_id, season_id)
    cursor.execute("SELECT LIG_ID FROM TANIM.LIG WHERE SEZON_ID = ? AND LIG_ADI = ?", season_id, league_name)
    return cursor.fetchone()[0]


# ============== FIKSTUR ISLEMLERI ==============

def parse_fixtures_from_list(html, table_id):
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table', id=table_id)
    if not table:
        return []

    fixtures = []
    rows = table.find_all('tr')

    for row in rows:
        score_cell = row.find('td', {'data-stat': 'score'})
        if not score_cell:
            continue
        score_link = score_cell.find('a')
        if not score_link or not score_link.get_text(strip=True):
            continue

        week = row.find('th', {'data-stat': 'gameweek'})
        date = row.find('td', {'data-stat': 'date'})
        home = row.find('td', {'data-stat': 'home_team'})
        away = row.find('td', {'data-stat': 'away_team'})
        venue = row.find('td', {'data-stat': 'venue'})

        fixture = {
            'week': int(week.get_text(strip=True)) if week and week.get_text(strip=True).isdigit() else None,
            'date': date.get_text(strip=True) if date else None,
            'home': home.get_text(strip=True) if home else None,
            'away': away.get_text(strip=True) if away else None,
            'score': score_link.get_text(strip=True),
            'match_url': "https://fbref.com" + score_link.get('href') if score_link.get('href') else None,
            'venue': venue.get_text(strip=True) if venue else None,
        }

        home_link = home.find('a') if home else None
        away_link = away.find('a') if away else None
        fixture['home_url'] = "https://fbref.com" + home_link.get('href') if home_link else None
        fixture['away_url'] = "https://fbref.com" + away_link.get('href') if away_link else None

        fixtures.append(fixture)

    return fixtures


def save_fixture(cursor, fixture, league_id):
    match_date = None
    if fixture['date']:
        try:
            match_date = datetime.strptime(fixture['date'], "%Y-%m-%d")
        except:
            pass

    cursor.execute("""
        SELECT FIKSTURID FROM FIKSTUR.FIKSTUR
        WHERE LIG_ID = ? AND TARIH = ? AND EVSAHIBI = ? AND MISAFIR = ?
    """, league_id, match_date, fixture['home'], fixture['away'])

    row = cursor.fetchone()
    if row:
        return row[0]

    cursor.execute("""
        INSERT INTO FIKSTUR.FIKSTUR
        (LIG_ID, HAFTA, TARIH, EVSAHIBI, SKOR, MISAFIR, STADYUM, URL, DURUM, KAYIT_TARIHI)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, GETDATE())
    """, league_id, fixture['week'], match_date, fixture['home'], fixture['score'],
         fixture['away'], fixture['venue'], fixture['match_url'])

    cursor.execute("""
        SELECT FIKSTURID FROM FIKSTUR.FIKSTUR
        WHERE LIG_ID = ? AND TARIH = ? AND EVSAHIBI = ? AND MISAFIR = ?
    """, league_id, match_date, fixture['home'], fixture['away'])
    return cursor.fetchone()[0]


# ============== TUM TABLOLARI PARSE ET ==============

def parse_all_player_stats(soup):
    """Tum tablolardan oyuncu istatistiklerini birlestir"""

    # Her takim icin ayri dict
    teams_data = {'home': {}, 'away': {}}

    # Tablo tipleri ve data-stat mapping
    table_types = {
        'summary': {
            'goals': 'GOL', 'assists': 'ASIST', 'pens_made': 'PENALTI_GOL', 'pens_att': 'PENALTI_ATISI',
            'shots': 'SUT', 'shots_on_target': 'ISABETLI_SUT', 'cards_yellow': 'SARI_KART',
            'cards_red': 'KIRMIZI_KART', 'xg': 'BEKLENEN_GOL', 'npxg': 'PENALTISIZ_XG',
            'xg_assist': 'BEKLENEN_ASIST', 'sca': 'SUT_YARATAN_AKSIYON', 'gca': 'GOL_YARATAN_AKSIYON'
        },
        'passing': {
            'passes_completed': 'PAS_BASARILI', 'passes': 'PAS_DENEME', 'passes_pct': 'PAS_ISABET',
            'passes_total_distance': 'PAS_TOPLAM_MESAFE', 'passes_progressive_distance': 'PAS_ILERIYE_MESAFE',
            'passes_completed_short': 'KISA_PAS_BASARILI', 'passes_short': 'KISA_PAS_DENEME',
            'passes_pct_short': 'KISA_PAS_ISABET', 'passes_completed_medium': 'ORTA_PAS_BASARILI',
            'passes_medium': 'ORTA_PAS_DENEME', 'passes_pct_medium': 'ORTA_PAS_ISABET',
            'passes_completed_long': 'UZUN_PAS_BASARILI', 'passes_long': 'UZUN_PAS_DENEME',
            'passes_pct_long': 'UZUN_PAS_ISABET', 'assisted_shots': 'ANAHTAR_PAS',
            'passes_into_final_third': 'SON_UCUNCU_PAS', 'passes_into_penalty_area': 'CEZA_SAHASINA_PAS',
            'crosses_into_penalty_area': 'ORTA', 'progressive_passes': 'ILERIYE_PAS'
        },
        'passing_types': {
            'passes_live': 'CANLI_PAS', 'passes_dead': 'OLU_PAS', 'passes_free_kicks': 'SERBEST_VURUS_PASI',
            'through_balls': 'ARA_PAS', 'passes_switches': 'DEGISTIRME_PASI', 'throw_ins': 'TACI',
            'corner_kicks': 'KORNER', 'corner_kicks_in': 'KORNER_IC', 'corner_kicks_out': 'KORNER_DIS',
            'passes_completed': 'TAMAMLANAN_PAS', 'passes_offsides': 'OFSAYT_PASI', 'passes_blocked': 'BLOKE_PAS'
        },
        'defense': {
            'tackles': 'TOP_KAPMA', 'tackles_won': 'TOP_KAPMA_KAZANILAN',
            'tackles_def_3rd': 'TOP_KAPMA_DEFANS', 'tackles_mid_3rd': 'TOP_KAPMA_ORTA',
            'tackles_att_3rd': 'TOP_KAPMA_HUCUM', 'challenges': 'MEYDAN_OKUMA',
            'challenge_tackles': 'MEYDAN_OKUMA_BASARILI', 'challenge_tackles_pct': 'MEYDAN_OKUMA_ISABET',
            'challenges_lost': 'MEYDAN_OKUMA_KAYBEDILEN', 'blocks': 'BLOK',
            'blocked_shots': 'SUT_BLOK', 'blocked_passes': 'PAS_BLOK',
            'interceptions': 'MUDAHALE', 'clearances': 'TOP_TEMIZLEME', 'errors': 'HATA'
        },
        'possession': {
            'touches': 'TEMAS', 'touches_def_pen_area': 'TEMAS_DEFANS_CEZA',
            'touches_def_3rd': 'TEMAS_DEFANS', 'touches_mid_3rd': 'TEMAS_ORTA',
            'touches_att_3rd': 'TEMAS_HUCUM', 'touches_att_pen_area': 'TEMAS_HUCUM_CEZA',
            'touches_live_ball': 'TEMAS_CANLI', 'take_ons': 'CARPISMA_DENEME',
            'take_ons_won': 'CARPISMA_BASARILI', 'take_ons_won_pct': 'CARPISMA_ISABET',
            'take_ons_tackled': 'CARPISMA_ENGELLENEN', 'carries': 'TOP_TASIMA',
            'carries_distance': 'TOP_TASIMA_MESAFE', 'carries_progressive_distance': 'TOP_TASIMA_ILERIYE',
            'progressive_carries': 'ILERIYE_TASIMA', 'carries_into_final_third': 'SON_UCUNCU_TASIMA',
            'carries_into_penalty_area': 'CEZA_SAHASINA_TASIMA', 'miscontrols': 'YANLIS_KONTROL',
            'dispossessed': 'ELE_GECIRME', 'passes_received': 'HEDEF_ALINAN',
            'progressive_passes_received': 'ILERIYE_HEDEF'
        },
        'misc': {
            'fouls': 'FAUL_YAPILAN', 'fouled': 'FAUL_ALINAN', 'offsides': 'OFSAYT',
            'pens_won': 'PENALTI_KAZANILAN', 'pens_conceded': 'PENALTI_VERDIRILEN',
            'own_goals': 'KENDI_KALESINE', 'ball_recoveries': 'TOP_KAZANMA',
            'aerials_won': 'HAVA_TOPU_KAZANILAN', 'aerials_lost': 'HAVA_TOPU_KAYBEDILEN',
            'aerials_won_pct': 'HAVA_TOPU_ISABET'
        }
    }

    # Sayisal alanlar
    int_fields = ['GOL', 'ASIST', 'PENALTI_GOL', 'PENALTI_ATISI', 'SUT', 'ISABETLI_SUT',
                  'SARI_KART', 'KIRMIZI_KART', 'SUT_YARATAN_AKSIYON', 'GOL_YARATAN_AKSIYON',
                  'PAS_BASARILI', 'PAS_DENEME', 'PAS_TOPLAM_MESAFE', 'PAS_ILERIYE_MESAFE',
                  'KISA_PAS_BASARILI', 'KISA_PAS_DENEME', 'ORTA_PAS_BASARILI', 'ORTA_PAS_DENEME',
                  'UZUN_PAS_BASARILI', 'UZUN_PAS_DENEME', 'ANAHTAR_PAS', 'SON_UCUNCU_PAS',
                  'CEZA_SAHASINA_PAS', 'ORTA', 'ILERIYE_PAS', 'CANLI_PAS', 'OLU_PAS',
                  'SERBEST_VURUS_PASI', 'ARA_PAS', 'DEGISTIRME_PASI', 'TACI', 'KORNER',
                  'KORNER_IC', 'KORNER_DIS', 'TAMAMLANAN_PAS', 'OFSAYT_PASI', 'BLOKE_PAS',
                  'TOP_KAPMA', 'TOP_KAPMA_KAZANILAN', 'TOP_KAPMA_DEFANS', 'TOP_KAPMA_ORTA',
                  'TOP_KAPMA_HUCUM', 'MEYDAN_OKUMA', 'MEYDAN_OKUMA_BASARILI', 'MEYDAN_OKUMA_KAYBEDILEN',
                  'BLOK', 'SUT_BLOK', 'PAS_BLOK', 'MUDAHALE', 'TOP_TEMIZLEME', 'HATA',
                  'TEMAS', 'TEMAS_DEFANS_CEZA', 'TEMAS_DEFANS', 'TEMAS_ORTA', 'TEMAS_HUCUM',
                  'TEMAS_HUCUM_CEZA', 'TEMAS_CANLI', 'CARPISMA_DENEME', 'CARPISMA_BASARILI',
                  'CARPISMA_ENGELLENEN', 'TOP_TASIMA', 'TOP_TASIMA_MESAFE', 'TOP_TASIMA_ILERIYE',
                  'ILERIYE_TASIMA', 'SON_UCUNCU_TASIMA', 'CEZA_SAHASINA_TASIMA', 'YANLIS_KONTROL',
                  'ELE_GECIRME', 'HEDEF_ALINAN', 'ILERIYE_HEDEF', 'FAUL_YAPILAN', 'FAUL_ALINAN',
                  'OFSAYT', 'PENALTI_KAZANILAN', 'PENALTI_VERDIRILEN', 'KENDI_KALESINE',
                  'TOP_KAZANMA', 'HAVA_TOPU_KAZANILAN', 'HAVA_TOPU_KAYBEDILEN']

    decimal_fields = ['BEKLENEN_GOL', 'PENALTISIZ_XG', 'BEKLENEN_ASIST', 'PAS_ISABET',
                      'KISA_PAS_ISABET', 'ORTA_PAS_ISABET', 'UZUN_PAS_ISABET',
                      'MEYDAN_OKUMA_ISABET', 'CARPISMA_ISABET', 'HAVA_TOPU_ISABET']

    # Her tablo tipini isle
    for table_type, mapping in table_types.items():
        # passing_types ile karismasin diye $ ekle (string sonu)
        pattern = f'stats_.*_{table_type}$'
        tables = soup.find_all('table', id=re.compile(pattern))

        for idx, table in enumerate(tables[:2]):  # Sadece ilk 2 (ev ve misafir)
            team_key = 'home' if idx == 0 else 'away'
            tbody = table.find('tbody')
            if not tbody:
                continue

            for row in tbody.find_all('tr'):
                if 'thead' in row.get('class', []):
                    continue

                player_cell = row.find('th', {'data-stat': 'player'})
                if not player_cell:
                    continue

                player_name = player_cell.get_text(strip=True)
                if not player_name:
                    continue

                # Bu oyuncu icin dict yoksa olustur
                if player_name not in teams_data[team_key]:
                    link = player_cell.find('a')
                    teams_data[team_key][player_name] = {
                        'name': player_name,
                        'url': "https://fbref.com" + link.get('href') if link else None
                    }

                    # Temel bilgiler (ilk tablodan)
                    for basic_stat in ['shirtnumber', 'nationality', 'position', 'age', 'minutes']:
                        cell = row.find(['th', 'td'], {'data-stat': basic_stat})
                        if cell:
                            val = cell.get_text(strip=True)
                            if basic_stat == 'shirtnumber':
                                teams_data[team_key][player_name]['FORMA_NO'] = int(val) if val.isdigit() else None
                            elif basic_stat == 'nationality':
                                teams_data[team_key][player_name]['nationality'] = val
                            elif basic_stat == 'position':
                                teams_data[team_key][player_name]['POZISYON'] = val
                            elif basic_stat == 'age':
                                teams_data[team_key][player_name]['YAS'] = val
                            elif basic_stat == 'minutes':
                                teams_data[team_key][player_name]['SURE'] = int(val) if val.isdigit() else None

                # Tablo istatistiklerini ekle
                for data_stat, db_col in mapping.items():
                    cell = row.find('td', {'data-stat': data_stat})
                    if cell:
                        val = cell.get_text(strip=True)
                        if db_col in int_fields:
                            teams_data[team_key][player_name][db_col] = int(val) if val.lstrip('-').isdigit() else None
                        elif db_col in decimal_fields:
                            try:
                                teams_data[team_key][player_name][db_col] = float(val) if val else None
                            except:
                                teams_data[team_key][player_name][db_col] = None

    return teams_data


def save_player_performance(cursor, fixture_id, team_id, player):
    """Oyuncu performansini kaydet - TUM SUTUNLAR"""
    if not player.get('name'):
        return

    player_id = get_or_create_player(
        cursor, player['name'], player.get('url'),
        player.get('nationality'), player.get('POZISYON')
    )

    cursor.execute("""
        SELECT PERFORMANS_ID FROM FIKSTUR.PERFORMANS WHERE FIKSTURID = ? AND OYUNCU_ID = ?
    """, fixture_id, player_id)

    # Tum sutunlar
    columns = ['FIKSTURID', 'OYUNCU_ID', 'TAKIM_ID', 'FORMA_NO', 'POZISYON', 'YAS', 'SURE',
               'GOL', 'ASIST', 'PENALTI_GOL', 'PENALTI_ATISI', 'SUT', 'ISABETLI_SUT',
               'SARI_KART', 'KIRMIZI_KART', 'BEKLENEN_GOL', 'PENALTISIZ_XG', 'BEKLENEN_ASIST',
               'SUT_YARATAN_AKSIYON', 'GOL_YARATAN_AKSIYON',
               'PAS_BASARILI', 'PAS_DENEME', 'PAS_ISABET', 'PAS_TOPLAM_MESAFE', 'PAS_ILERIYE_MESAFE',
               'KISA_PAS_BASARILI', 'KISA_PAS_DENEME', 'KISA_PAS_ISABET',
               'ORTA_PAS_BASARILI', 'ORTA_PAS_DENEME', 'ORTA_PAS_ISABET',
               'UZUN_PAS_BASARILI', 'UZUN_PAS_DENEME', 'UZUN_PAS_ISABET',
               'ANAHTAR_PAS', 'SON_UCUNCU_PAS', 'CEZA_SAHASINA_PAS', 'ORTA', 'ILERIYE_PAS',
               'CANLI_PAS', 'OLU_PAS', 'SERBEST_VURUS_PASI', 'ARA_PAS', 'DEGISTIRME_PASI',
               'TACI', 'KORNER', 'KORNER_IC', 'KORNER_DIS', 'TAMAMLANAN_PAS', 'OFSAYT_PASI', 'BLOKE_PAS',
               'TOP_KAPMA', 'TOP_KAPMA_KAZANILAN', 'TOP_KAPMA_DEFANS', 'TOP_KAPMA_ORTA', 'TOP_KAPMA_HUCUM',
               'MEYDAN_OKUMA', 'MEYDAN_OKUMA_BASARILI', 'MEYDAN_OKUMA_ISABET', 'MEYDAN_OKUMA_KAYBEDILEN',
               'BLOK', 'SUT_BLOK', 'PAS_BLOK', 'MUDAHALE', 'TOP_TEMIZLEME', 'HATA',
               'TEMAS', 'TEMAS_DEFANS_CEZA', 'TEMAS_DEFANS', 'TEMAS_ORTA', 'TEMAS_HUCUM',
               'TEMAS_HUCUM_CEZA', 'TEMAS_CANLI', 'CARPISMA_DENEME', 'CARPISMA_BASARILI',
               'CARPISMA_ISABET', 'CARPISMA_ENGELLENEN', 'TOP_TASIMA', 'TOP_TASIMA_MESAFE',
               'TOP_TASIMA_ILERIYE', 'ILERIYE_TASIMA', 'SON_UCUNCU_TASIMA', 'CEZA_SAHASINA_TASIMA',
               'YANLIS_KONTROL', 'ELE_GECIRME', 'HEDEF_ALINAN', 'HEDEF_BASARILI', 'ILERIYE_HEDEF',
               'FAUL_YAPILAN', 'FAUL_ALINAN', 'OFSAYT', 'PENALTI_KAZANILAN', 'PENALTI_VERDIRILEN',
               'KENDI_KALESINE', 'TOP_KAZANMA', 'HAVA_TOPU_KAZANILAN', 'HAVA_TOPU_KAYBEDILEN', 'HAVA_TOPU_ISABET',
               'KAYIT_TARIHI']

    values = [fixture_id, player_id, team_id]
    for col in columns[3:-1]:  # FORMA_NO'dan HAVA_TOPU_ISABET'e kadar
        values.append(player.get(col))

    if cursor.fetchone():
        # Update
        set_clause = ', '.join([f'{col} = ?' for col in columns[3:-1]])
        cursor.execute(f"""
            UPDATE FIKSTUR.PERFORMANS SET {set_clause}, DEGISIKLIK_TARIHI = GETDATE()
            WHERE FIKSTURID = ? AND OYUNCU_ID = ?
        """, *values[3:], fixture_id, player_id)
    else:
        # Insert
        placeholders = ', '.join(['?' for _ in columns[:-1]]) + ', GETDATE()'
        cursor.execute(f"""
            INSERT INTO FIKSTUR.PERFORMANS ({', '.join(columns)})
            VALUES ({placeholders})
        """, *values)


# ============== ANA PROGRAM ==============

def main():
    print("=" * 70)
    print("FbrefSolution - Kapsamli Futbol Istatistik Scraper v3")
    print("TUM SEKMELER: Summary, Passing, Pass Types, Defensive, Possession, Misc")
    print("=" * 70)

    conn = get_db_connection()
    cursor = conn.cursor()
    print("[OK] Veritabani baglantisi basarili")

    for league in LEAGUES:
        print(f"\n{'=' * 70}")
        print(f"LIG: {league['name']} - {league['season']}")
        print("=" * 70)

        season_id = ensure_season(cursor, league['season'])
        league_id = ensure_league(cursor, season_id, league['name'], league['url'],
                                  league['table_id'], league['country'])
        conn.commit()

        print("\n[1] Fikstur listesi yukleniyor...")
        fixture_html = get_html(league['url'])
        fixtures = parse_fixtures_from_list(fixture_html, league['table_id'])
        print(f"    {len(fixtures)} oynanmis mac bulundu")

        if TEST_LIMIT:
            fixtures = fixtures[:TEST_LIMIT]
            print(f"    TEST: Sadece ilk {TEST_LIMIT} mac islenecek")

        for i, fixture in enumerate(fixtures, 1):
            print(f"\n[MAC {i}/{len(fixtures)}] {fixture['home']} vs {fixture['away']} ({fixture['score']})")

            home_team_id = get_or_create_team(cursor, fixture['home'], fixture.get('home_url'), league['country'])
            away_team_id = get_or_create_team(cursor, fixture['away'], fixture.get('away_url'), league['country'])
            fixture_id = save_fixture(cursor, fixture, league_id)
            print(f"    Fikstur ID: {fixture_id}")

            if fixture['match_url']:
                print(f"    Mac detay sayfasi yukleniyor...")
                try:
                    match_html = get_html(fixture['match_url'])
                    soup = BeautifulSoup(match_html, 'html.parser')

                    # Tum tablolardan verileri al
                    teams_data = parse_all_player_stats(soup)

                    home_count = len(teams_data['home'])
                    away_count = len(teams_data['away'])
                    print(f"    Oyuncular: {home_count} + {away_count}")

                    # Kaydet
                    for player_name, player_data in teams_data['home'].items():
                        save_player_performance(cursor, fixture_id, home_team_id, player_data)

                    for player_name, player_data in teams_data['away'].items():
                        save_player_performance(cursor, fixture_id, away_team_id, player_data)

                    conn.commit()
                    print(f"    [OK] Tum veriler kaydedildi!")

                except Exception as e:
                    print(f"    [HATA] {e}")
                    import traceback
                    traceback.print_exc()

        conn.commit()

    cursor.close()
    conn.close()
    print(f"\n{'=' * 70}")
    print("[OK] Islem tamamlandi!")


if __name__ == "__main__":
    main()
