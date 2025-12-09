# -*- coding: utf-8 -*-
"""
Yerel kupaları veritabanına ekle
FA Cup, DFB-Pokal, Copa del Rey, Coppa Italia, Coupe de France, Türkiye Kupası vb.
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')

import pyodbc

CONNECTION_STRING = (
    "DRIVER={SQL Server};"
    "SERVER=195.201.146.224,1433;"
    "DATABASE=FBREF;"
    "UID=sa;"
    "PWD=FbRef2024Str0ng;"
)

# FBRef'ten yerel kupalar
# https://fbref.com/en/comps/ sayfasından alındı
DOMESTIC_CUPS = [
    # İngiltere
    {"lig_id": 33, "name": "FA Cup", "comp_id": 514, "url_name": "FA-Cup", "ulke": "İNGİLTERE"},
    {"lig_id": 34, "name": "EFL Cup", "comp_id": 515, "url_name": "EFL-Cup", "ulke": "İNGİLTERE"},

    # Almanya
    {"lig_id": 35, "name": "DFB-Pokal", "comp_id": 516, "url_name": "DFB-Pokal", "ulke": "ALMANYA"},

    # İspanya
    {"lig_id": 36, "name": "Copa del Rey", "comp_id": 569, "url_name": "Copa-del-Rey", "ulke": "İSPANYA"},

    # İtalya
    {"lig_id": 37, "name": "Coppa Italia", "comp_id": 521, "url_name": "Coppa-Italia", "ulke": "İTALYA"},

    # Fransa
    {"lig_id": 38, "name": "Coupe de France", "comp_id": 526, "url_name": "Coupe-de-France", "ulke": "FRANSA"},

    # Türkiye
    {"lig_id": 39, "name": "Türkiye Kupası", "comp_id": 573, "url_name": "Turkish-Cup", "ulke": "TÜRKİYE"},

    # Hollanda
    {"lig_id": 40, "name": "KNVB Cup", "comp_id": 520, "url_name": "KNVB-Beker", "ulke": "HOLLANDA"},

    # Portekiz
    {"lig_id": 41, "name": "Taça de Portugal", "comp_id": 518, "url_name": "Taca-de-Portugal", "ulke": "PORTEKİZ"},

    # Belçika
    {"lig_id": 42, "name": "Beker van België", "comp_id": 540, "url_name": "Belgian-Cup", "ulke": "BELÇİKA"},

    # İskoçya
    {"lig_id": 43, "name": "Scottish Cup", "comp_id": 519, "url_name": "Scottish-Cup", "ulke": "İSKOÇYA"},
    {"lig_id": 44, "name": "Scottish League Cup", "comp_id": 538, "url_name": "Scottish-League-Cup", "ulke": "İSKOÇYA"},
]

SEASON = "2024-2025"


def main():
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()

    print("=" * 70)
    print("YEREL KUPALARI EKLEME")
    print("=" * 70)

    # IDENTITY_INSERT açık
    cursor.execute("SET IDENTITY_INSERT TANIM.LIG ON")

    for cup in DOMESTIC_CUPS:
        lig_id = cup['lig_id']
        name = cup['name']
        comp_id = cup['comp_id']
        url_name = cup['url_name']
        ulke = cup['ulke']

        # URL oluştur
        url = f"https://fbref.com/en/comps/{comp_id}/{SEASON}/schedule/{SEASON}-{url_name}-Scores-and-Fixtures"
        fikstur_tablo_id = f"sched_{SEASON}_{comp_id}_1"

        # Mevcut kayıt kontrol
        cursor.execute("SELECT LIG_ID FROM TANIM.LIG WHERE LIG_ID = ?", lig_id)
        row = cursor.fetchone()

        if row:
            print(f"  [MEVCUT] {name} (ID: {lig_id})")
        else:
            # Yeni kayıt ekle
            try:
                cursor.execute("""
                    INSERT INTO TANIM.LIG (LIG_ID, LIG_ADI, URL, ULKE, SEZON, FIKSTUR_TABLO_ID, DURUM, SEZON_ID)
                    VALUES (?, ?, ?, ?, ?, ?, 1, 4)
                """, lig_id, name, url, ulke, SEASON, fikstur_tablo_id)
                conn.commit()
                print(f"  [YENİ] {name} (ID: {lig_id}) - {ulke}")
            except Exception as e:
                print(f"  [HATA] {name}: {e}")

    # IDENTITY_INSERT kapat
    cursor.execute("SET IDENTITY_INSERT TANIM.LIG OFF")
    conn.commit()

    print()
    print("=" * 70)
    print("EKLENEN KUPALAR:")
    print("=" * 70)

    cursor.execute("""
        SELECT LIG_ID, LIG_ADI, ULKE
        FROM TANIM.LIG
        WHERE LIG_ID >= 33
        ORDER BY LIG_ID
    """)

    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]} ({row[2]})")

    conn.close()
    print()
    print("Tamamlandı!")


if __name__ == "__main__":
    main()
