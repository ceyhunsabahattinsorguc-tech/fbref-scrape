# -*- coding: utf-8 -*-
"""
FBRef Full Stats Scraper
6 Sekmeli Ligler: summary, passing, pass_types, defense, possession, misc
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

# 6 Sekmeli Ligler (Full Stats)
FULL_STATS_LEAGUES = [
    {"lig_id": 6, "name": "Premier League", "comp_id": 9, "url_name": "Premier-League"},
    {"lig_id": 7, "name": "La Liga", "comp_id": 12, "url_name": "La-Liga"},
    {"lig_id": 8, "name": "Serie A", "comp_id": 11, "url_name": "Serie-A"},
    {"lig_id": 9, "name": "Ligue 1", "comp_id": 13, "url_name": "Ligue-1"},
    {"lig_id": 10, "name": "Bundesliga", "comp_id": 20, "url_name": "Bundesliga"},
    {"lig_id": 11, "name": "Eredivisie", "comp_id": 23, "url_name": "Eredivisie"},
    {"lig_id": 12, "name": "Primeira Liga", "comp_id": 32, "url_name": "Primeira-Liga"},
    {"lig_id": 14, "name": "Brazilian Serie A", "comp_id": 24, "url_name": "Serie-A"},
    {"lig_id": 15, "name": "Championship", "comp_id": 10, "url_name": "Championship"},
    {"lig_id": 17, "name": "First Division A", "comp_id": 37, "url_name": "Belgian-Pro-League"},
]

SEASON = "2025-2026"
SEZON_ID = 4

# Tüm data-stat mapping'leri (DB sütun isimleriyle eşleştirilmiş)
TABLE_MAPPINGS = {
    'summary': {
        'shirtnumber': 'FORMA_NO',
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
    },
    'passing': {
        'passes_completed': 'PAS_BASARILI',
        'passes': 'PAS_DENEME',
        'passes_pct': 'PAS_ISABET',
        'passes_total_distance': 'PAS_TOPLAM_MESAFE',
        'passes_progressive_distance': 'PAS_ILERIYE_MESAFE',
        'passes_completed_short': 'KISA_PAS_BASARILI',
        'passes_short': 'KISA_PAS_DENEME',
        'passes_pct_short': 'KISA_PAS_ISABET',
        'passes_completed_medium': 'ORTA_PAS_BASARILI',
        'passes_medium': 'ORTA_PAS_DENEME',
        'passes_pct_medium': 'ORTA_PAS_ISABET',
        'passes_completed_long': 'UZUN_PAS_BASARILI',
        'passes_long': 'UZUN_PAS_DENEME',
        'passes_pct_long': 'UZUN_PAS_ISABET',
        'assisted_shots': 'ANAHTAR_PAS',
        'passes_into_final_third': 'SON_UCUNCU_PAS',
        'passes_into_penalty_area': 'CEZA_SAHASINA_PAS',
        'progressive_passes': 'ILERIYE_PAS',
    },
    'passing_types': {
        'passes_live': 'CANLI_PAS',
        'passes_dead': 'OLU_PAS',
        'passes_free_kicks': 'SERBEST_VURUS_PASI',
        'through_balls': 'ARA_PAS',
        'passes_switches': 'DEGISTIRME_PASI',
        'crosses': 'ORTA',
        'throw_ins': 'TACI',
        'corner_kicks': 'KORNER',
        'corner_kicks_in': 'KORNER_IC',
        'corner_kicks_out': 'KORNER_DIS',
        'passes_offside': 'OFSAYT_PASI',
        'passes_blocked': 'BLOKE_PAS',
    },
    'defense': {
        'tackles_won': 'TOP_KAPMA_KAZANILAN',
        'tackles_def_3rd': 'TOP_KAPMA_DEFANS',
        'tackles_mid_3rd': 'TOP_KAPMA_ORTA',
        'tackles_att_3rd': 'TOP_KAPMA_HUCUM',
        'challenge_tackles': 'MEYDAN_OKUMA_BASARILI',
        'challenges': 'MEYDAN_OKUMA',
        'challenge_tackles_pct': 'MEYDAN_OKUMA_ISABET',
        'challenges_lost': 'MEYDAN_OKUMA_KAYBEDILEN',
        'blocked_shots': 'SUT_BLOK',
        'blocked_passes': 'PAS_BLOK',
        'clearances': 'TOP_TEMIZLEME',
        'errors': 'HATA',
    },
    'possession': {
        'touches_def_pen_area': 'TEMAS_DEFANS_CEZA',
        'touches_def_3rd': 'TEMAS_DEFANS',
        'touches_mid_3rd': 'TEMAS_ORTA',
        'touches_att_3rd': 'TEMAS_HUCUM',
        'touches_att_pen_area': 'TEMAS_HUCUM_CEZA',
        'touches_live_ball': 'TEMAS_CANLI',
        'take_ons': 'CARPISMA_DENEME',
        'take_ons_won': 'CARPISMA_BASARILI',
        'take_ons_won_pct': 'CARPISMA_ISABET',
        'take_ons_tackled': 'CARPISMA_ENGELLENEN',
        'carries': 'TOP_TASIMA',
        'carries_distance': 'TOP_TASIMA_MESAFE',
        'carries_progressive_distance': 'TOP_TASIMA_ILERIYE',
        'progressive_carries': 'ILERIYE_TASIMA',
        'carries_into_final_third': 'SON_UCUNCU_TASIMA',
        'carries_into_penalty_area': 'CEZA_SAHASINA_TASIMA',
        'miscontrols': 'YANLIS_KONTROL',
        'dispossessed': 'ELE_GECIRME',
        'passes_received': 'HEDEF_ALINAN',
        'progressive_passes_received': 'ILERIYE_HEDEF',
    },
    'misc': {
        'fouls': 'FAUL_YAPILAN',
        'fouled': 'FAUL_ALINAN',
        'offsides': 'OFSAYT',
        'pens_won': 'PENALTI_KAZANILAN',
        'pens_conceded': 'PENALTI_VERDIRILEN',
        'own_goals': 'KENDI_KALESINE',
        'ball_recoveries': 'TOP_KAZANMA',
        'aerials_won': 'HAVA_TOPU_KAZANILAN',
        'aerials_lost': 'HAVA_TOPU_KAYBEDILEN',
        'aerials_won_pct': 'HAVA_TOPU_ISABET',
    }
}

# Int ve Decimal alanları (DB sütun isimleriyle eşleştirilmiş)
INT_FIELDS = [
    # Summary
    'FORMA_NO', 'SURE', 'GOL', 'ASIST', 'PENALTI_GOL', 'PENALTI_ATISI', 'SUT', 'ISABETLI_SUT',
    'SARI_KART', 'KIRMIZI_KART', 'TEMAS', 'TOP_KAPMA', 'MUDAHALE', 'BLOK',
    'SUT_YARATAN_AKSIYON', 'GOL_YARATAN_AKSIYON',
    # Passing
    'PAS_BASARILI', 'PAS_DENEME', 'PAS_TOPLAM_MESAFE', 'PAS_ILERIYE_MESAFE',
    'KISA_PAS_BASARILI', 'KISA_PAS_DENEME', 'ORTA_PAS_BASARILI', 'ORTA_PAS_DENEME',
    'UZUN_PAS_BASARILI', 'UZUN_PAS_DENEME', 'ANAHTAR_PAS', 'SON_UCUNCU_PAS',
    'CEZA_SAHASINA_PAS', 'ILERIYE_PAS',
    # Passing Types
    'CANLI_PAS', 'OLU_PAS', 'SERBEST_VURUS_PASI', 'ARA_PAS', 'DEGISTIRME_PASI',
    'ORTA', 'TACI', 'KORNER', 'KORNER_IC', 'KORNER_DIS', 'OFSAYT_PASI', 'BLOKE_PAS',
    # Defense
    'TOP_KAPMA_KAZANILAN', 'TOP_KAPMA_DEFANS', 'TOP_KAPMA_ORTA', 'TOP_KAPMA_HUCUM',
    'MEYDAN_OKUMA_BASARILI', 'MEYDAN_OKUMA', 'MEYDAN_OKUMA_KAYBEDILEN',
    'SUT_BLOK', 'PAS_BLOK', 'TOP_TEMIZLEME', 'HATA',
    # Possession
    'TEMAS_DEFANS_CEZA', 'TEMAS_DEFANS', 'TEMAS_ORTA', 'TEMAS_HUCUM',
    'TEMAS_HUCUM_CEZA', 'TEMAS_CANLI', 'CARPISMA_DENEME', 'CARPISMA_BASARILI',
    'CARPISMA_ENGELLENEN', 'TOP_TASIMA', 'TOP_TASIMA_MESAFE', 'TOP_TASIMA_ILERIYE',
    'ILERIYE_TASIMA', 'SON_UCUNCU_TASIMA', 'CEZA_SAHASINA_TASIMA',
    'YANLIS_KONTROL', 'ELE_GECIRME', 'HEDEF_ALINAN', 'ILERIYE_HEDEF',
    # Misc
    'FAUL_YAPILAN', 'FAUL_ALINAN', 'OFSAYT', 'PENALTI_KAZANILAN', 'PENALTI_VERDIRILEN',
    'KENDI_KALESINE', 'TOP_KAZANMA', 'HAVA_TOPU_KAZANILAN', 'HAVA_TOPU_KAYBEDILEN',
]

DECIMAL_FIELDS = [
    'BEKLENEN_GOL', 'PENALTISIZ_XG', 'BEKLENEN_ASIST', 'PAS_ISABET',
    'KISA_PAS_ISABET', 'ORTA_PAS_ISABET', 'UZUN_PAS_ISABET',
    'MEYDAN_OKUMA_ISABET', 'CARPISMA_ISABET', 'HAVA_TOPU_ISABET',
]


def get_db_connection():
    return pyodbc.connect(CONNECTION_STRING)


def get_html(url, timeout=90000):
    """Playwright ile sayfa HTML'ini al"""
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

    # Skor formatla (ornegin: "2-1")
    skor = None
    if match_data.get('home_score') is not None and match_data.get('away_score') is not None:
        skor = f"{match_data['home_score']}-{match_data['away_score']}"

    cursor.execute("""
        INSERT INTO FIKSTUR.FIKSTUR (
            LIG_ID, EVSAHIBI, MISAFIR, SKOR, TARIH, URL, KAYIT_TARIHI, DURUM
        )
        VALUES (?, ?, ?, ?, ?, ?, GETDATE(), 1)
    """,
        lig_id, match_data.get('home_team'), match_data.get('away_team'),
        skor, match_data.get('date'), match_data['url']
    )
    conn.commit()
    cursor.execute("SELECT @@IDENTITY")
    return cursor.fetchone()[0]


def parse_all_player_stats(soup, team_key, takim_id):
    """Tüm tabloları parse et ve birleştir"""
    players = {}

    for table_type, mapping in TABLE_MAPPINGS.items():
        # passing_types için özel regex
        if table_type == 'passing_types':
            pattern = r'stats_.*_passing_types$'
        else:
            pattern = f'stats_.*_{table_type}$'

        tables = soup.find_all('table', id=re.compile(pattern))

        if len(tables) < 2:
            continue

        table = tables[0] if team_key == 'home' else tables[1]
        tbody = table.find('tbody')
        if not tbody:
            continue

        for row in tbody.find_all('tr'):
            player_cell = row.find('th', {'data-stat': 'player'})
            if not player_cell:
                continue

            player_link = player_cell.find('a')
            if not player_link:
                continue

            player_name = player_link.get_text(strip=True)
            player_url = "https://fbref.com" + player_link.get('href', '')

            if player_url not in players:
                players[player_url] = {
                    'name': player_name,
                    'url': player_url,
                    'team_key': team_key,
                    'takim_id': takim_id,
                }

            # Bu tablodaki verileri ekle
            for data_stat, db_col in mapping.items():
                cell = row.find('td', {'data-stat': data_stat})
                if cell:
                    value = cell.get_text(strip=True)
                    if db_col in INT_FIELDS:
                        players[player_url][db_col] = parse_int(value)
                    elif db_col in DECIMAL_FIELDS:
                        players[player_url][db_col] = parse_decimal(value)
                    else:
                        players[player_url][db_col] = value if value else None

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

    for col in INT_FIELDS + DECIMAL_FIELDS + ['POZISYON', 'YAS']:
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
    """Oynanmış maçları getir"""
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
    """Tek bir maçı işle"""
    print(f"    {match['home_team']} vs {match['away_team']} ({match['score']})")

    html = get_html(match['url'])
    if not html:
        return False

    soup = BeautifulSoup(html, 'html.parser')

    # Skor parse
    home_score, away_score = None, None
    if match['score'] and '–' in match['score']:
        parts = match['score'].split('–')
        home_score = parse_int(parts[0].strip())
        away_score = parse_int(parts[1].strip())

    # Takımlar
    home_team_id = get_or_create_takim(conn, match['home_team'], ulke=ulke)
    away_team_id = get_or_create_takim(conn, match['away_team'], ulke=ulke)

    # Fikstur
    match_data = {
        'url': match['url'],
        'home_team': match['home_team'],
        'away_team': match['away_team'],
        'home_team_id': home_team_id,
        'away_team_id': away_team_id,
        'home_score': home_score,
        'away_score': away_score,
        'date': match.get('date')
    }
    fikstur_id = create_fikstur(conn, match_data, lig_id)

    # Oyuncu istatistikleri (tüm 6 sekme)
    home_players = parse_all_player_stats(soup, 'home', home_team_id)
    away_players = parse_all_player_stats(soup, 'away', away_team_id)
    all_players = {**home_players, **away_players}

    # Kaleci istatistikleri
    home_keepers = parse_keeper_stats(soup, 'home', home_team_id)
    away_keepers = parse_keeper_stats(soup, 'away', away_team_id)
    all_keepers = {**home_keepers, **away_keepers}

    # Kaydet
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
    """Bir ligi scrape et"""
    print(f"\n{'='*60}")
    print(f"{league['name']}")
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
            if process_match(conn, match, league['lig_id'], None):
                success += 1
        except Exception as e:
            print(f"HATA: {e}")

    return success


def main(selected_leagues=None, test_limit=None):
    """Ana fonksiyon"""
    print("=" * 70)
    print("FBREF FULL STATS SCRAPER")
    print("6 Sekmeli Ligler")
    print("=" * 70)

    conn = get_db_connection()
    print("Veritabani baglantisi basarili")

    leagues = selected_leagues if selected_leagues else FULL_STATS_LEAGUES

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
