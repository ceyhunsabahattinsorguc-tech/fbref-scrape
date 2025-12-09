# -*- coding: utf-8 -*-
"""FBRef Puan Durumu Scraper - Lig sıralamaları"""

import sys
sys.stdout.reconfigure(encoding='utf-8')

import asyncio
import pyodbc
from playwright.async_api import async_playwright
from datetime import datetime
import re

CONNECTION_STRING = (
    "DRIVER={SQL Server};"
    "SERVER=195.201.146.224,1433;"
    "DATABASE=FBREF;"
    "UID=sa;"
    "PWD=FbRef2024Str0ng;"
)

# Lig tanımları (Veritabanındaki LIG_ID'lere göre)
LEAGUES = [
    {"lig_id": 6, "name": "Premier League", "comp_id": 9, "url_name": "Premier-League"},
    {"lig_id": 7, "name": "La Liga", "comp_id": 12, "url_name": "La-Liga"},
    {"lig_id": 8, "name": "Serie A", "comp_id": 11, "url_name": "Serie-A"},
    {"lig_id": 10, "name": "Bundesliga", "comp_id": 20, "url_name": "Bundesliga"},
    {"lig_id": 9, "name": "Ligue 1", "comp_id": 13, "url_name": "Ligue-1"},
    {"lig_id": 1, "name": "Süper Lig", "comp_id": 26, "url_name": "Super-Lig"},
    {"lig_id": 11, "name": "Eredivisie", "comp_id": 23, "url_name": "Eredivisie"},
    {"lig_id": 12, "name": "Primeira Liga", "comp_id": 32, "url_name": "Primeira-Liga"},
    {"lig_id": 17, "name": "Belgian Pro League", "comp_id": 37, "url_name": "Belgian-Pro-League"},
    {"lig_id": 13, "name": "Scottish Premiership", "comp_id": 40, "url_name": "Scottish-Premiership"},
]

SEASON = "2024-2025"


def create_table():
    """Puan durumu tablosunu oluştur"""
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()

    # Tablo yoksa oluştur
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'PUAN_DURUMU' AND schema_id = SCHEMA_ID('FIKSTUR'))
        BEGIN
            CREATE TABLE FIKSTUR.PUAN_DURUMU (
                ID INT IDENTITY(1,1) PRIMARY KEY,
                LIG_ID INT NOT NULL,
                TAKIM_ADI NVARCHAR(100) NOT NULL,
                SIRA INT NOT NULL,
                OYNANAN INT DEFAULT 0,
                GALIBIYET INT DEFAULT 0,
                BERABERLIK INT DEFAULT 0,
                MAGLUBIYET INT DEFAULT 0,
                ATILAN_GOL INT DEFAULT 0,
                YENILEN_GOL INT DEFAULT 0,
                AVERAJ INT DEFAULT 0,
                PUAN INT DEFAULT 0,
                SON_5 NVARCHAR(20),
                GUNCELLEME_TARIHI DATETIME DEFAULT GETDATE(),
                CONSTRAINT FK_PUAN_DURUMU_LIG FOREIGN KEY (LIG_ID) REFERENCES TANIM.LIG(LIG_ID)
            )

            CREATE INDEX IX_PUAN_DURUMU_LIG ON FIKSTUR.PUAN_DURUMU(LIG_ID)
            CREATE INDEX IX_PUAN_DURUMU_TAKIM ON FIKSTUR.PUAN_DURUMU(TAKIM_ADI)
        END
    """)
    conn.commit()
    print("FIKSTUR.PUAN_DURUMU tablosu hazır")

    conn.close()


async def scrape_standings(page, league):
    """Tek bir lig için puan durumu çek"""
    url = f"https://fbref.com/en/comps/{league['comp_id']}/{SEASON}/{SEASON}-{league['url_name']}-Stats"
    print(f"\n{'='*60}")
    print(f"Lig: {league['name']}")
    print(f"URL: {url}")

    try:
        await page.goto(url, wait_until="networkidle", timeout=60000)
        await asyncio.sleep(3)

        standings = []

        # Puan durumu tablosunu bul
        # FBRef'te genelde "overall" veya "Regular season" tablosu
        table_selectors = [
            "table#results{}_overall",  # results2024-2025_overall gibi
            "table[id*='overall']",
            "table.stats_table",
        ]

        table = None
        for selector in table_selectors:
            if "{}" in selector:
                selector = selector.format(SEASON)
            try:
                table = await page.query_selector(selector)
                if table:
                    break
            except:
                continue

        if not table:
            # Alternatif: tüm tabloları kontrol et
            tables = await page.query_selector_all("table.stats_table")
            for t in tables:
                header = await t.query_selector("caption")
                if header:
                    text = await header.inner_text()
                    if "Regular" in text or "Overall" in text or "Table" in text:
                        table = t
                        break

            if not table and tables:
                table = tables[0]  # İlk tabloyu al

        if not table:
            print(f"   UYARI: Puan durumu tablosu bulunamadı")
            return []

        # Satırları oku
        rows = await table.query_selector_all("tbody tr")

        for row in rows:
            try:
                cells = await row.query_selector_all("td, th")
                if len(cells) < 8:
                    continue

                # Sıra (ilk hücre)
                sira_cell = await row.query_selector("th[data-stat='rank']")
                if sira_cell:
                    sira_text = await sira_cell.inner_text()
                else:
                    sira_text = await cells[0].inner_text()

                sira = int(re.sub(r'[^\d]', '', sira_text) or 0)
                if sira == 0:
                    continue

                # Takım adı
                team_cell = await row.query_selector("td[data-stat='team'] a, th[data-stat='team'] a")
                if team_cell:
                    takim = await team_cell.inner_text()
                else:
                    takim = await cells[1].inner_text() if len(cells) > 1 else ""

                takim = takim.strip()
                if not takim:
                    continue

                # Diğer istatistikler
                def get_int(text):
                    try:
                        return int(re.sub(r'[^\d\-]', '', text) or 0)
                    except:
                        return 0

                # FBRef standart sıralama: Rk, Squad, MP, W, D, L, GF, GA, GD, Pts
                mp_cell = await row.query_selector("td[data-stat='games']")
                w_cell = await row.query_selector("td[data-stat='wins']")
                d_cell = await row.query_selector("td[data-stat='ties']")
                l_cell = await row.query_selector("td[data-stat='losses']")
                gf_cell = await row.query_selector("td[data-stat='goals_for']")
                ga_cell = await row.query_selector("td[data-stat='goals_against']")
                gd_cell = await row.query_selector("td[data-stat='goal_diff']")
                pts_cell = await row.query_selector("td[data-stat='points']")

                oynanan = get_int(await mp_cell.inner_text()) if mp_cell else 0
                galibiyet = get_int(await w_cell.inner_text()) if w_cell else 0
                beraberlik = get_int(await d_cell.inner_text()) if d_cell else 0
                maglubiyet = get_int(await l_cell.inner_text()) if l_cell else 0
                atilan = get_int(await gf_cell.inner_text()) if gf_cell else 0
                yenilen = get_int(await ga_cell.inner_text()) if ga_cell else 0
                averaj = get_int(await gd_cell.inner_text()) if gd_cell else (atilan - yenilen)
                puan = get_int(await pts_cell.inner_text()) if pts_cell else 0

                # Son 5 maç (varsa)
                son5_cell = await row.query_selector("td[data-stat='last_5']")
                son5 = ""
                if son5_cell:
                    son5 = await son5_cell.inner_text()
                    son5 = son5.strip()[:20]  # Max 20 karakter

                standings.append({
                    "lig_id": league["lig_id"],
                    "takim": takim,
                    "sira": sira,
                    "oynanan": oynanan,
                    "galibiyet": galibiyet,
                    "beraberlik": beraberlik,
                    "maglubiyet": maglubiyet,
                    "atilan": atilan,
                    "yenilen": yenilen,
                    "averaj": averaj,
                    "puan": puan,
                    "son5": son5
                })

            except Exception as e:
                continue

        print(f"   {len(standings)} takım bulundu")
        return standings

    except Exception as e:
        print(f"   HATA: {e}")
        return []


def save_standings(standings, lig_id):
    """Puan durumunu veritabanına kaydet"""
    if not standings:
        return

    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()

    # Önce bu ligin eski verilerini sil
    cursor.execute("DELETE FROM FIKSTUR.PUAN_DURUMU WHERE LIG_ID = ?", lig_id)

    # Yeni verileri ekle
    for s in standings:
        cursor.execute("""
            INSERT INTO FIKSTUR.PUAN_DURUMU
            (LIG_ID, TAKIM_ADI, SIRA, OYNANAN, GALIBIYET, BERABERLIK, MAGLUBIYET,
             ATILAN_GOL, YENILEN_GOL, AVERAJ, PUAN, SON_5, GUNCELLEME_TARIHI)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
        """, s["lig_id"], s["takim"], s["sira"], s["oynanan"], s["galibiyet"],
            s["beraberlik"], s["maglubiyet"], s["atilan"], s["yenilen"],
            s["averaj"], s["puan"], s["son5"])

    conn.commit()
    conn.close()
    print(f"   {len(standings)} takım kaydedildi")


async def main():
    """Ana fonksiyon"""
    print("=" * 70)
    print("FBREF PUAN DURUMU SCRAPER")
    print(f"Sezon: {SEASON}")
    print(f"Tarih: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 70)

    # Tabloyu oluştur
    create_table()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = await context.new_page()

        total_teams = 0

        for league in LEAGUES:
            standings = await scrape_standings(page, league)
            if standings:
                save_standings(standings, league["lig_id"])
                total_teams += len(standings)

            # Rate limiting
            await asyncio.sleep(3)

        await browser.close()

    print("\n" + "=" * 70)
    print(f"TAMAMLANDI: Toplam {total_teams} takım")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
