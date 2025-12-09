# -*- coding: utf-8 -*-
"""v_Takim_Guc_Analizi VIEW'ini veritabanina uygula"""

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

def main():
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()

    print("=" * 70)
    print("TAKIM GUC ANALIZI VIEW OLUSTURMA")
    print("=" * 70)

    # 1. Eski view'i sil
    print("\n1. Eski VIEW siliniyor...")
    try:
        cursor.execute("""
            IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'TAHMIN.v_Takim_Guc_Analizi'))
            DROP VIEW TAHMIN.v_Takim_Guc_Analizi
        """)
        conn.commit()
        print("   OK")
    except Exception as e:
        print(f"   Hata: {e}")

    # 2. Yeni view olustur
    print("\n2. v_Takim_Guc_Analizi VIEW oluşturuluyor...")
    view_sql = """
    CREATE VIEW TAHMIN.v_Takim_Guc_Analizi AS
    WITH MacSkorlari AS (
        SELECT
            f.FIKSTURID,
            f.LIG_ID,
            f.EVSAHIBI,
            f.MISAFIR,
            f.TARIH,
            CAST(LEFT(f.SKOR, CHARINDEX('-', REPLACE(f.SKOR, N'–', '-')) - 1) AS INT) AS EV_GOL,
            CAST(RIGHT(f.SKOR, LEN(f.SKOR) - CHARINDEX('-', REPLACE(f.SKOR, N'–', '-'))) AS INT) AS MIS_GOL
        FROM FIKSTUR.FIKSTUR f
        WHERE f.DURUM = 1
          AND f.SKOR IS NOT NULL
          AND f.SKOR LIKE '%[0-9]%-%[0-9]%'
    ),
    TakimMaclari AS (
        SELECT
            LIG_ID,
            EVSAHIBI AS TAKIM_ADI,
            TARIH,
            EV_GOL AS ATILAN_GOL,
            MIS_GOL AS YENILEN_GOL,
            'EV' AS MAC_TIPI,
            ROW_NUMBER() OVER (PARTITION BY LIG_ID, EVSAHIBI ORDER BY TARIH DESC) AS MAC_SIRASI
        FROM MacSkorlari

        UNION ALL

        SELECT
            LIG_ID,
            MISAFIR AS TAKIM_ADI,
            TARIH,
            MIS_GOL AS ATILAN_GOL,
            EV_GOL AS YENILEN_GOL,
            'DEPLASMAN' AS MAC_TIPI,
            ROW_NUMBER() OVER (PARTITION BY LIG_ID, MISAFIR ORDER BY TARIH DESC) AS MAC_SIRASI
        FROM MacSkorlari
    ),
    LigOrtalamalari AS (
        SELECT
            LIG_ID,
            COUNT(*) AS TOPLAM_MAC,
            AVG(CAST(EV_GOL + MIS_GOL AS FLOAT)) AS MAC_BASINA_GOL,
            AVG(CAST(EV_GOL AS FLOAT)) AS EV_GOL_ORT,
            AVG(CAST(MIS_GOL AS FLOAT)) AS DEPLASMAN_GOL_ORT
        FROM MacSkorlari
        GROUP BY LIG_ID
    ),
    TakimIstatistikleri AS (
        SELECT
            tm.LIG_ID,
            tm.TAKIM_ADI,
            COUNT(*) AS MAC_SAYISI,
            AVG(CAST(tm.ATILAN_GOL AS FLOAT)) AS ATILAN_GOL_ORT,
            AVG(CAST(tm.YENILEN_GOL AS FLOAT)) AS YENILEN_GOL_ORT,
            SUM(tm.ATILAN_GOL) AS TOPLAM_ATILAN,
            SUM(tm.YENILEN_GOL) AS TOPLAM_YENILEN,
            SUM(CASE WHEN tm.MAC_SIRASI <= 5 THEN tm.ATILAN_GOL ELSE 0 END) AS SON5_ATILAN,
            SUM(CASE WHEN tm.MAC_SIRASI <= 5 THEN tm.YENILEN_GOL ELSE 0 END) AS SON5_YENILEN,
            AVG(CASE WHEN tm.MAC_TIPI = 'EV' THEN CAST(tm.ATILAN_GOL AS FLOAT) END) AS EV_ATILAN_ORT,
            AVG(CASE WHEN tm.MAC_TIPI = 'DEPLASMAN' THEN CAST(tm.ATILAN_GOL AS FLOAT) END) AS DEP_ATILAN_ORT,
            AVG(CASE WHEN tm.MAC_TIPI = 'EV' THEN CAST(tm.YENILEN_GOL AS FLOAT) END) AS EV_YENILEN_ORT,
            AVG(CASE WHEN tm.MAC_TIPI = 'DEPLASMAN' THEN CAST(tm.YENILEN_GOL AS FLOAT) END) AS DEP_YENILEN_ORT,
            MAX(tm.TARIH) AS SON_MAC_TARIHI
        FROM TakimMaclari tm
        WHERE tm.MAC_SIRASI <= 10
        GROUP BY tm.LIG_ID, tm.TAKIM_ADI
        HAVING COUNT(*) >= 3
    )
    SELECT
        ts.LIG_ID,
        l.LIG_ADI,
        ts.TAKIM_ADI,
        ts.MAC_SAYISI,
        ts.TOPLAM_ATILAN,
        ts.TOPLAM_YENILEN,
        ROUND(ts.ATILAN_GOL_ORT, 3) AS ATILAN_GOL_ORT,
        ROUND(ts.YENILEN_GOL_ORT, 3) AS YENILEN_GOL_ORT,
        ROUND(ts.ATILAN_GOL_ORT / NULLIF(lo.MAC_BASINA_GOL / 2, 0), 3) AS SALDIRI_GUCU,
        ROUND(ts.YENILEN_GOL_ORT / NULLIF(lo.MAC_BASINA_GOL / 2, 0), 3) AS SAVUNMA_GUCU,
        ROUND(ts.EV_ATILAN_ORT / NULLIF(lo.EV_GOL_ORT, 0), 3) AS EV_SALDIRI_GUCU,
        ROUND(ts.EV_YENILEN_ORT / NULLIF(lo.DEPLASMAN_GOL_ORT, 0), 3) AS EV_SAVUNMA_GUCU,
        ROUND(ts.DEP_ATILAN_ORT / NULLIF(lo.DEPLASMAN_GOL_ORT, 0), 3) AS DEP_SALDIRI_GUCU,
        ROUND(ts.DEP_YENILEN_ORT / NULLIF(lo.EV_GOL_ORT, 0), 3) AS DEP_SAVUNMA_GUCU,
        ts.SON5_ATILAN,
        ts.SON5_YENILEN,
        ROUND(CAST(ts.SON5_ATILAN AS FLOAT) / 5, 2) AS SON5_ATILAN_ORT,
        ROUND(CAST(ts.SON5_YENILEN AS FLOAT) / 5, 2) AS SON5_YENILEN_ORT,
        ROUND(lo.MAC_BASINA_GOL, 3) AS LIG_GOL_ORT,
        ROUND(lo.EV_GOL_ORT, 3) AS LIG_EV_GOL_ORT,
        ROUND(lo.DEPLASMAN_GOL_ORT, 3) AS LIG_DEP_GOL_ORT,
        lo.TOPLAM_MAC AS LIG_MAC_SAYISI,
        ts.SON_MAC_TARIHI
    FROM TakimIstatistikleri ts
    JOIN TANIM.LIG l ON ts.LIG_ID = l.LIG_ID
    JOIN LigOrtalamalari lo ON ts.LIG_ID = lo.LIG_ID
    """
    try:
        cursor.execute(view_sql)
        conn.commit()
        print("   OK - VIEW oluşturuldu")
    except Exception as e:
        print(f"   Hata: {e}")
        conn.close()
        return

    # 3. Test
    print("\n" + "=" * 70)
    print("TEST SONUCLARI")
    print("=" * 70)

    # Kaç takım var?
    print("\n3. Takım sayısı kontrolü:")
    cursor.execute("SELECT COUNT(*) FROM TAHMIN.v_Takim_Guc_Analizi")
    count = cursor.fetchone()[0]
    print(f"   Toplam {count} takım analiz edildi")

    # Lig bazında dağılım
    print("\n4. Lig bazında dağılım:")
    cursor.execute("""
        SELECT LIG_ADI, COUNT(*) as TAKIM_SAYISI, ROUND(AVG(LIG_GOL_ORT), 2) AS GOL_ORT
        FROM TAHMIN.v_Takim_Guc_Analizi
        GROUP BY LIG_ADI, LIG_ID
        ORDER BY TAKIM_SAYISI DESC
    """)
    for row in cursor.fetchall():
        print(f"   {row[0]:<25} {row[1]} takım (Lig Ort: {row[2]} gol/maç)")

    # En güçlü hücum
    print("\n5. En güçlü saldırı (Top 10):")
    cursor.execute("""
        SELECT TOP 10 TAKIM_ADI, LIG_ADI, SALDIRI_GUCU, ATILAN_GOL_ORT
        FROM TAHMIN.v_Takim_Guc_Analizi
        ORDER BY SALDIRI_GUCU DESC
    """)
    for row in cursor.fetchall():
        print(f"   {row[0]:<20} {row[1]:<20} Güç: {row[2]:.2f} ({row[3]:.2f} gol/maç)")

    # En iyi savunma
    print("\n6. En iyi savunma (Top 10):")
    cursor.execute("""
        SELECT TOP 10 TAKIM_ADI, LIG_ADI, SAVUNMA_GUCU, YENILEN_GOL_ORT
        FROM TAHMIN.v_Takim_Guc_Analizi
        ORDER BY SAVUNMA_GUCU ASC
    """)
    for row in cursor.fetchall():
        print(f"   {row[0]:<20} {row[1]:<20} Güç: {row[2]:.2f} ({row[3]:.2f} gol/maç)")

    conn.close()
    print("\nTamamlandı!")


if __name__ == "__main__":
    main()
