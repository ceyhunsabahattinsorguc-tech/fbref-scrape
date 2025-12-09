# -*- coding: utf-8 -*-
"""
FBRef Goalkeeper (Kaleci) Scraper
Maç bazlı kaleci istatistiklerini çeker
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')

import pyodbc
from bs4 import BeautifulSoup
from datetime import datetime
from playwright.sync_api import sync_playwright
import time
import re

CONNECTION_STRING = (
    "DRIVER={SQL Server};"
    "SERVER=195.201.146.224,1433;"
    "DATABASE=FBREF;"
    "UID=sa;"
    "PWD=FbRef2024Str0ng;"
)


def get_db_connection():
    return pyodbc.connect(CONNECTION_STRING)


def create_goalkeeper_table(conn):
    """Kaleci tablosunu oluştur"""
    cursor = conn.cursor()

    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'KALECI_PERFORMANS' AND schema_id = SCHEMA_ID('FIKSTUR'))
        CREATE TABLE FIKSTUR.KALECI_PERFORMANS (
            KALECI_PERF_ID INT IDENTITY(1,1) PRIMARY KEY,
            FIKSTURID INT NOT NULL,
            OYUNCU_ID INT NULL,
            TAKIM_ID INT NULL,
            KALECI_ADI NVARCHAR(100) NOT NULL,
            TAKIM_ADI NVARCHAR(100) NOT NULL,

            -- Temel istatistikler
            SURE INT NULL,                     -- Oynanan dakika
            SOTA INT NULL,                     -- Shots on Target Against (Kaleye gelen şut)
            YENILEN_GOL INT NULL,              -- Goals Against
            KURTARIS INT NULL,                 -- Saves
            KURTARIS_YUZDESI DECIMAL(5,2) NULL, -- Save%

            -- İleri düzey metrikler
            PSXG DECIMAL(5,2) NULL,            -- Post-Shot Expected Goals
            PSXG_FARKI DECIMAL(5,2) NULL,      -- PSxG +/- (PSxG - GA)

            -- Penaltı
            PENALTI_KARSISINDA INT NULL,       -- Penalty Kicks Faced
            PENALTI_GOL INT NULL,              -- Penalty Goals Against
            PENALTI_KURTARIS INT NULL,         -- Penalty Saved
            PENALTI_KACIRILAN INT NULL,        -- Penalty Missed

            -- Uzun pas
            PAS_DENEME INT NULL,               -- Passes Attempted
            ATILAN_PAS INT NULL,               -- Throws
            UZUN_PAS_HEDEF INT NULL,           -- Launch % (Goal Kicks)

            -- Diğer
            CROSS_DURDURMA INT NULL,           -- Crosses Stopped
            OPA INT NULL,                      -- #OPA (Defensive actions outside penalty area)
            ORTALAMA_MESAFE DECIMAL(5,2) NULL, -- AvgDist (Avg distance from goal)

            KAYIT_TARIHI DATETIME DEFAULT GETDATE()
        )
    """)
    conn.commit()
    print("FIKSTUR.KALECI_PERFORMANS tablosu hazır.")


def get_html(url, timeout=60000):
    """Playwright ile HTML al"""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        page = context.new_page()

        try:
            page.goto(url, timeout=timeout, wait_until='networkidle')
            time.sleep(3)
            html = page.content()
        finally:
            browser.close()

        return html


def parse_goalkeeper_stats(html, match_url):
    """Kaleci istatistiklerini parse et"""
    soup = BeautifulSoup(html, 'html.parser')
    goalkeepers = []

    # Kaleci tablosu: keeper_stats_home ve keeper_stats_away
    for table_id in ['keeper_stats_' + t for t in ['home', 'away', 'Home', 'Away']]:
        table = soup.find('table', {'id': lambda x: x and table_id.lower() in x.lower()})
        if not table:
            # Alternatif: id içinde 'keeper' geçen tablolar
            tables = soup.find_all('table', {'id': lambda x: x and 'keeper' in x.lower()})
            for t in tables:
                table = t
                break

        if table:
            tbody = table.find('tbody')
            if tbody:
                for row in tbody.find_all('tr'):
                    if 'thead' in row.get('class', []):
                        continue

                    try:
                        # Oyuncu adı
                        player_cell = row.find('th', {'data-stat': 'player'})
                        if not player_cell:
                            continue

                        player_name = player_cell.get_text(strip=True)
                        if not player_name:
                            continue

                        # İstatistikleri çek
                        stats = {
                            'player_name': player_name,
                            'minutes': get_stat(row, 'minutes'),
                            'sota': get_stat(row, 'gk_shots_on_target_against'),
                            'goals_against': get_stat(row, 'gk_goals_against'),
                            'saves': get_stat(row, 'gk_saves'),
                            'save_pct': get_stat(row, 'gk_save_pct'),
                            'psxg': get_stat(row, 'gk_psxg'),
                            'psxg_diff': get_stat(row, 'gk_psxg_net'),
                            'pk_att': get_stat(row, 'gk_pens_att'),
                            'pk_allowed': get_stat(row, 'gk_pens_allowed'),
                            'pk_saved': get_stat(row, 'gk_pens_saved'),
                            'pk_missed': get_stat(row, 'gk_pens_missed'),
                            'passes_att': get_stat(row, 'gk_passes'),
                            'throws': get_stat(row, 'gk_throws'),
                            'launch_pct': get_stat(row, 'gk_pct_passes_launched'),
                            'crosses_stopped': get_stat(row, 'gk_crosses_stopped'),
                            'opa': get_stat(row, 'gk_def_acts_outside_pen_area'),
                            'avg_dist': get_stat(row, 'gk_avg_distance'),
                        }

                        # Takım adını bulmaya çalış (tablo caption'dan)
                        caption = table.find('caption')
                        team_name = caption.get_text(strip=True) if caption else 'Unknown'
                        stats['team_name'] = team_name.replace(' Goalkeeper Stats', '').strip()

                        goalkeepers.append(stats)

                    except Exception as e:
                        continue

    return goalkeepers


def get_stat(row, stat_name):
    """Satırdan belirli bir istatistiği çek"""
    cell = row.find(['td', 'th'], {'data-stat': stat_name})
    if cell:
        text = cell.get_text(strip=True)
        if text:
            try:
                # Yüzde işaretini kaldır
                text = text.replace('%', '').replace(',', '.')
                if '.' in text:
                    return float(text)
                return int(text)
            except:
                return None
    return None


def save_goalkeeper_stats(conn, gk_stats, fikstur_id):
    """Kaleci istatistiklerini kaydet"""
    cursor = conn.cursor()

    # Mevcut kayıt kontrolü
    cursor.execute("""
        SELECT KALECI_PERF_ID FROM FIKSTUR.KALECI_PERFORMANS
        WHERE FIKSTURID = ? AND KALECI_ADI = ?
    """, fikstur_id, gk_stats['player_name'])

    row = cursor.fetchone()

    if row:
        # Güncelle
        cursor.execute("""
            UPDATE FIKSTUR.KALECI_PERFORMANS
            SET SURE = ?, SOTA = ?, YENILEN_GOL = ?, KURTARIS = ?,
                KURTARIS_YUZDESI = ?, PSXG = ?, PSXG_FARKI = ?,
                PENALTI_KARSISINDA = ?, PENALTI_GOL = ?, PENALTI_KURTARIS = ?
            WHERE KALECI_PERF_ID = ?
        """, gk_stats['minutes'], gk_stats['sota'], gk_stats['goals_against'],
            gk_stats['saves'], gk_stats['save_pct'], gk_stats['psxg'],
            gk_stats['psxg_diff'], gk_stats['pk_att'], gk_stats['pk_allowed'],
            gk_stats['pk_saved'], row[0])
        conn.commit()
        return row[0], False
    else:
        # Yeni kayıt
        cursor.execute("""
            INSERT INTO FIKSTUR.KALECI_PERFORMANS (
                FIKSTURID, KALECI_ADI, TAKIM_ADI,
                SURE, SOTA, YENILEN_GOL, KURTARIS, KURTARIS_YUZDESI,
                PSXG, PSXG_FARKI, PENALTI_KARSISINDA, PENALTI_GOL, PENALTI_KURTARIS
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, fikstur_id, gk_stats['player_name'], gk_stats['team_name'],
            gk_stats['minutes'], gk_stats['sota'], gk_stats['goals_against'],
            gk_stats['saves'], gk_stats['save_pct'], gk_stats['psxg'],
            gk_stats['psxg_diff'], gk_stats['pk_att'], gk_stats['pk_allowed'],
            gk_stats['pk_saved'])
        conn.commit()
        cursor.execute("SELECT @@IDENTITY")
        return cursor.fetchone()[0], True


def get_matches_without_goalkeeper_data(conn, lig_id=None, limit=50):
    """Kaleci verisi olmayan maçları getir"""
    cursor = conn.cursor()

    query = """
        SELECT TOP (?) f.FIKSTURID, f.URL, f.EVSAHIBI, f.MISAFIR, f.TARIH
        FROM FIKSTUR.FIKSTUR f
        LEFT JOIN FIKSTUR.KALECI_PERFORMANS kp ON f.FIKSTURID = kp.FIKSTURID
        WHERE f.DURUM = 1  -- Oynanmış
        AND f.URL IS NOT NULL
        AND kp.KALECI_PERF_ID IS NULL
    """
    params = [limit]

    if lig_id:
        query += " AND f.LIG_ID = ?"
        params.append(lig_id)

    query += " ORDER BY f.TARIH DESC"

    cursor.execute(query, params)
    return cursor.fetchall()


def main(lig_id=None, limit=50):
    """Ana fonksiyon"""
    conn = get_db_connection()

    # Tablo oluştur
    create_goalkeeper_table(conn)

    print("=" * 70)
    print("FBREF GOALKEEPER SCRAPER")
    print(f"Limit: {limit} maç")
    print("=" * 70)

    # Kaleci verisi olmayan maçları al
    matches = get_matches_without_goalkeeper_data(conn, lig_id, limit)
    print(f"\nİşlenecek maç sayısı: {len(matches)}")

    total_new = 0
    total_updated = 0

    for i, match in enumerate(matches):
        fikstur_id, url, home, away, date = match

        if not url:
            continue

        print(f"\n[{i+1}/{len(matches)}] {home} vs {away} ({date})")

        try:
            html = get_html(url)
            goalkeepers = parse_goalkeeper_stats(html, url)

            new_count = 0
            update_count = 0

            for gk in goalkeepers:
                _, is_new = save_goalkeeper_stats(conn, gk, fikstur_id)
                if is_new:
                    new_count += 1
                else:
                    update_count += 1

            print(f"  Kaleci: {len(goalkeepers)} | Yeni: {new_count} | Güncellendi: {update_count}")

            for gk in goalkeepers:
                print(f"    {gk['player_name']}: {gk['saves']} kurtarış, {gk['goals_against']} gol yeme")

            total_new += new_count
            total_updated += update_count

            # Rate limiting
            time.sleep(4)

        except Exception as e:
            print(f"  HATA: {e}")
            continue

    print("\n" + "=" * 70)
    print(f"TOPLAM: {total_new} yeni, {total_updated} güncellendi")
    print("=" * 70)

    conn.close()


# =============================================
# KALECİ FORM VIEW'I İÇİN YARDIMCI FONKSİYONLAR
# =============================================

def create_goalkeeper_view(conn):
    """Kaleci form view'ı oluştur"""
    cursor = conn.cursor()

    view_sql = """
    CREATE OR ALTER VIEW TAHMIN.v_Kaleci_Form AS
    WITH KaleciStats AS (
        SELECT
            kp.KALECI_ADI,
            kp.TAKIM_ADI,
            f.LIG_ID,
            COUNT(*) AS MAC_SAYISI,

            -- Toplam istatistikler
            SUM(ISNULL(kp.KURTARIS, 0)) AS TOPLAM_KURTARIS,
            SUM(ISNULL(kp.YENILEN_GOL, 0)) AS TOPLAM_YENILEN_GOL,
            SUM(ISNULL(kp.SOTA, 0)) AS TOPLAM_SOTA,

            -- Ortalamalar
            AVG(CAST(kp.KURTARIS AS FLOAT)) AS KURTARIS_ORT,
            AVG(CAST(kp.YENILEN_GOL AS FLOAT)) AS YENILEN_GOL_ORT,
            AVG(kp.KURTARIS_YUZDESI) AS KURTARIS_YUZDESI_ORT,

            -- Clean sheet
            SUM(CASE WHEN kp.YENILEN_GOL = 0 THEN 1 ELSE 0 END) AS CLEAN_SHEET,

            -- PSxG performansı
            SUM(ISNULL(kp.PSXG_FARKI, 0)) AS TOPLAM_PSXG_PREVENTED,
            AVG(kp.PSXG_FARKI) AS PSXG_PREVENTED_ORT,

            -- Penaltı
            SUM(ISNULL(kp.PENALTI_KURTARIS, 0)) AS PENALTI_KURTARIS,
            SUM(ISNULL(kp.PENALTI_KARSISINDA, 0)) AS PENALTI_KARSISINDA,

            MAX(f.TARIH) AS SON_MAC

        FROM FIKSTUR.KALECI_PERFORMANS kp
        JOIN FIKSTUR.FIKSTUR f ON kp.FIKSTURID = f.FIKSTURID
        GROUP BY kp.KALECI_ADI, kp.TAKIM_ADI, f.LIG_ID
        HAVING COUNT(*) >= 3
    )
    SELECT
        *,
        -- Kaleci Skoru (0-100)
        CASE WHEN KURTARIS_YUZDESI_ORT >= 80 THEN 40
             WHEN KURTARIS_YUZDESI_ORT >= 70 THEN 30
             WHEN KURTARIS_YUZDESI_ORT >= 60 THEN 20
             ELSE 10 END +
        CASE WHEN PSXG_PREVENTED_ORT > 0.5 THEN 30
             WHEN PSXG_PREVENTED_ORT > 0 THEN 20
             WHEN PSXG_PREVENTED_ORT > -0.5 THEN 10
             ELSE 0 END +
        CASE WHEN CAST(CLEAN_SHEET AS FLOAT) / MAC_SAYISI >= 0.4 THEN 30
             WHEN CAST(CLEAN_SHEET AS FLOAT) / MAC_SAYISI >= 0.3 THEN 20
             WHEN CAST(CLEAN_SHEET AS FLOAT) / MAC_SAYISI >= 0.2 THEN 10
             ELSE 0 END
        AS KALECI_SKORU,

        -- Golcü için bu kaleciye karşı avantaj (ters)
        100 - (
            CASE WHEN KURTARIS_YUZDESI_ORT >= 80 THEN 40
                 WHEN KURTARIS_YUZDESI_ORT >= 70 THEN 30
                 WHEN KURTARIS_YUZDESI_ORT >= 60 THEN 20
                 ELSE 10 END +
            CASE WHEN PSXG_PREVENTED_ORT > 0.5 THEN 30
                 WHEN PSXG_PREVENTED_ORT > 0 THEN 20
                 WHEN PSXG_PREVENTED_ORT > -0.5 THEN 10
                 ELSE 0 END +
            CASE WHEN CAST(CLEAN_SHEET AS FLOAT) / MAC_SAYISI >= 0.4 THEN 30
                 WHEN CAST(CLEAN_SHEET AS FLOAT) / MAC_SAYISI >= 0.3 THEN 20
                 WHEN CAST(CLEAN_SHEET AS FLOAT) / MAC_SAYISI >= 0.2 THEN 10
                 ELSE 0 END
        ) AS GOLCU_AVANTAJ

    FROM KaleciStats
    """

    try:
        cursor.execute(view_sql)
        conn.commit()
        print("TAHMIN.v_Kaleci_Form VIEW oluşturuldu")
    except Exception as e:
        print(f"VIEW oluşturma hatası: {e}")


if __name__ == "__main__":
    # Premier League kaleci verileri - 20 maç
    main(lig_id=6, limit=20)
