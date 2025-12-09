# -*- coding: utf-8 -*-
"""
Maç Tahmin Sistemi Kurulum Scripti
==================================
Tüm VIEW'ları oluşturur ve test eder
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')

import pyodbc
import math
from datetime import datetime

CONNECTION_STRING = (
    "DRIVER={SQL Server};"
    "SERVER=195.201.146.224,1433;"
    "DATABASE=FBREF;"
    "UID=sa;"
    "PWD=FbRef2024Str0ng;"
)


def poisson_pmf(k, lam):
    if lam <= 0:
        return 0 if k > 0 else 1
    return (lam ** k) * math.exp(-lam) / math.factorial(k)


def calculate_probs(ev_lambda, mis_lambda, max_goals=7):
    matrix = {}
    for ev in range(max_goals + 1):
        for mis in range(max_goals + 1):
            matrix[(ev, mis)] = poisson_pmf(ev, ev_lambda) * poisson_pmf(mis, mis_lambda)

    ev_kazanir = sum(p for (ev, mis), p in matrix.items() if ev > mis)
    berabere = sum(p for (ev, mis), p in matrix.items() if ev == mis)
    mis_kazanir = sum(p for (ev, mis), p in matrix.items() if ev < mis)
    ust_25 = sum(p for (ev, mis), p in matrix.items() if ev + mis > 2.5)
    kg_var = sum(p for (ev, mis), p in matrix.items() if ev > 0 and mis > 0)

    en_olasi = max(matrix.items(), key=lambda x: x[1])

    return {
        '1': round(ev_kazanir * 100, 1),
        'X': round(berabere * 100, 1),
        '2': round(mis_kazanir * 100, 1),
        'ust_25': round(ust_25 * 100, 1),
        'alt_25': round((1 - ust_25) * 100, 1),
        'kg_var': round(kg_var * 100, 1),
        'kg_yok': round((1 - kg_var) * 100, 1),
        'en_olasi': f"{en_olasi[0][0]}-{en_olasi[0][1]}",
        'en_olasi_olas': round(en_olasi[1] * 100, 1)
    }


def main():
    print("=" * 80)
    print("MAC TAHMIN SISTEMI KURULUMU")
    print(f"Tarih: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 80)

    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()

    # =============================================
    # 1. v_Mac_Tahmin VIEW
    # =============================================
    print("\n[1/3] v_Mac_Tahmin VIEW oluşturuluyor...")
    try:
        cursor.execute("""
            IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'TAHMIN.v_Mac_Tahmin'))
            DROP VIEW TAHMIN.v_Mac_Tahmin
        """)
        conn.commit()
    except:
        pass

    view_sql = """
    CREATE VIEW TAHMIN.v_Mac_Tahmin AS
    WITH OynanmamisMaclar AS (
        SELECT f.FIKSTURID, f.LIG_ID, f.EVSAHIBI, f.MISAFIR, f.TARIH, f.HAFTA
        FROM FIKSTUR.FIKSTUR f WHERE f.DURUM = 0
    ),
    EvSahibiBilgi AS (
        SELECT om.FIKSTURID, tg.TAKIM_ADI,
            tg.SALDIRI_GUCU AS EV_SALDIRI, tg.SAVUNMA_GUCU AS EV_SAVUNMA,
            tg.EV_SALDIRI_GUCU, tg.EV_SAVUNMA_GUCU,
            tg.LIG_GOL_ORT, tg.LIG_EV_GOL_ORT, tg.LIG_DEP_GOL_ORT,
            tg.MAC_SAYISI AS EV_MAC_SAYISI, tg.SON5_ATILAN AS EV_SON5_ATILAN, tg.SON5_YENILEN AS EV_SON5_YENILEN
        FROM OynanmamisMaclar om
        JOIN TAHMIN.v_Takim_Guc_Analizi tg ON om.EVSAHIBI = tg.TAKIM_ADI AND om.LIG_ID = tg.LIG_ID
    ),
    MisafirBilgi AS (
        SELECT om.FIKSTURID, tg.TAKIM_ADI,
            tg.SALDIRI_GUCU AS MIS_SALDIRI, tg.SAVUNMA_GUCU AS MIS_SAVUNMA,
            tg.DEP_SALDIRI_GUCU, tg.DEP_SAVUNMA_GUCU,
            tg.MAC_SAYISI AS MIS_MAC_SAYISI, tg.SON5_ATILAN AS MIS_SON5_ATILAN, tg.SON5_YENILEN AS MIS_SON5_YENILEN
        FROM OynanmamisMaclar om
        JOIN TAHMIN.v_Takim_Guc_Analizi tg ON om.MISAFIR = tg.TAKIM_ADI AND om.LIG_ID = tg.LIG_ID
    )
    SELECT om.FIKSTURID, om.LIG_ID, l.LIG_ADI, om.EVSAHIBI, om.MISAFIR, om.TARIH, om.HAFTA,
        ev.EV_SALDIRI, ev.EV_SAVUNMA, ev.EV_SALDIRI_GUCU, ev.EV_SAVUNMA_GUCU, ev.EV_MAC_SAYISI,
        ev.EV_SON5_ATILAN, ev.EV_SON5_YENILEN,
        mis.MIS_SALDIRI, mis.MIS_SAVUNMA, mis.DEP_SALDIRI_GUCU, mis.DEP_SAVUNMA_GUCU, mis.MIS_MAC_SAYISI,
        mis.MIS_SON5_ATILAN, mis.MIS_SON5_YENILEN,
        ev.LIG_GOL_ORT, ev.LIG_EV_GOL_ORT, ev.LIG_DEP_GOL_ORT,
        ROUND(ISNULL(ev.EV_SALDIRI_GUCU, ev.EV_SALDIRI) * ISNULL(mis.DEP_SAVUNMA_GUCU, mis.MIS_SAVUNMA) * ev.LIG_EV_GOL_ORT, 3) AS EV_BEKLENEN_GOL,
        ROUND(ISNULL(mis.DEP_SALDIRI_GUCU, mis.MIS_SALDIRI) * ISNULL(ev.EV_SAVUNMA_GUCU, ev.EV_SAVUNMA) * ev.LIG_DEP_GOL_ORT, 3) AS MIS_BEKLENEN_GOL,
        ROUND(ISNULL(ev.EV_SALDIRI_GUCU, ev.EV_SALDIRI) * ISNULL(mis.DEP_SAVUNMA_GUCU, mis.MIS_SAVUNMA) * ev.LIG_EV_GOL_ORT +
              ISNULL(mis.DEP_SALDIRI_GUCU, mis.MIS_SALDIRI) * ISNULL(ev.EV_SAVUNMA_GUCU, ev.EV_SAVUNMA) * ev.LIG_DEP_GOL_ORT, 2) AS TOPLAM_BEKLENEN_GOL,
        ROUND(CAST(ev.EV_SON5_ATILAN AS FLOAT) / NULLIF(ev.EV_SON5_YENILEN, 0), 2) AS EV_FORM_ORANI,
        ROUND(CAST(mis.MIS_SON5_ATILAN AS FLOAT) / NULLIF(mis.MIS_SON5_YENILEN, 0), 2) AS MIS_FORM_ORANI,
        CASE WHEN ev.EV_MAC_SAYISI >= 10 AND mis.MIS_MAC_SAYISI >= 10 THEN 'YUKSEK'
             WHEN ev.EV_MAC_SAYISI >= 5 AND mis.MIS_MAC_SAYISI >= 5 THEN 'ORTA' ELSE 'DUSUK' END AS GUVENILIRLIK,
        ROUND((CAST(ev.EV_MAC_SAYISI AS FLOAT) / 10 * 50) + (CAST(mis.MIS_MAC_SAYISI AS FLOAT) / 10 * 50), 0) AS VERI_SKORU
    FROM OynanmamisMaclar om
    JOIN TANIM.LIG l ON om.LIG_ID = l.LIG_ID
    LEFT JOIN EvSahibiBilgi ev ON om.FIKSTURID = ev.FIKSTURID
    LEFT JOIN MisafirBilgi mis ON om.FIKSTURID = mis.FIKSTURID
    WHERE ev.FIKSTURID IS NOT NULL AND mis.FIKSTURID IS NOT NULL
    """
    try:
        cursor.execute(view_sql)
        conn.commit()
        print("   OK - v_Mac_Tahmin oluşturuldu")
    except Exception as e:
        print(f"   HATA: {e}")

    # =============================================
    # 2. v_Mac_Bahis_Oneri VIEW
    # =============================================
    print("\n[2/3] v_Mac_Bahis_Oneri VIEW oluşturuluyor...")
    try:
        cursor.execute("""
            IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'TAHMIN.v_Mac_Bahis_Oneri'))
            DROP VIEW TAHMIN.v_Mac_Bahis_Oneri
        """)
        conn.commit()
    except:
        pass

    bahis_view_sql = """
    CREATE VIEW TAHMIN.v_Mac_Bahis_Oneri AS
    WITH TahminBilgileri AS (
        SELECT FIKSTURID, LIG_ID, LIG_ADI, EVSAHIBI, MISAFIR, TARIH, HAFTA,
               EV_BEKLENEN_GOL, MIS_BEKLENEN_GOL, TOPLAM_BEKLENEN_GOL, GUVENILIRLIK, VERI_SKORU
        FROM TAHMIN.v_Mac_Tahmin
    ),
    TahminOlasiliklari AS (
        SELECT tb.*,
            CASE WHEN tb.EV_BEKLENEN_GOL > tb.MIS_BEKLENEN_GOL * 1.5 THEN 0.55
                 WHEN tb.EV_BEKLENEN_GOL > tb.MIS_BEKLENEN_GOL * 1.2 THEN 0.45
                 WHEN tb.EV_BEKLENEN_GOL > tb.MIS_BEKLENEN_GOL THEN 0.38
                 WHEN tb.EV_BEKLENEN_GOL = tb.MIS_BEKLENEN_GOL THEN 0.33
                 ELSE 0.25 END AS EV_KAZANIR_OLAS,
            CASE WHEN ABS(tb.EV_BEKLENEN_GOL - tb.MIS_BEKLENEN_GOL) < 0.3 THEN 0.30
                 WHEN ABS(tb.EV_BEKLENEN_GOL - tb.MIS_BEKLENEN_GOL) < 0.5 THEN 0.27 ELSE 0.24 END AS BERABERE_OLAS,
            CASE WHEN tb.MIS_BEKLENEN_GOL > tb.EV_BEKLENEN_GOL * 1.5 THEN 0.50
                 WHEN tb.MIS_BEKLENEN_GOL > tb.EV_BEKLENEN_GOL * 1.2 THEN 0.40
                 WHEN tb.MIS_BEKLENEN_GOL > tb.EV_BEKLENEN_GOL THEN 0.35 ELSE 0.25 END AS MIS_KAZANIR_OLAS,
            CASE WHEN tb.TOPLAM_BEKLENEN_GOL >= 3.5 THEN 0.70
                 WHEN tb.TOPLAM_BEKLENEN_GOL >= 3.0 THEN 0.60
                 WHEN tb.TOPLAM_BEKLENEN_GOL >= 2.5 THEN 0.50
                 WHEN tb.TOPLAM_BEKLENEN_GOL >= 2.0 THEN 0.40 ELSE 0.30 END AS UST_2_5_OLAS,
            CASE WHEN tb.EV_BEKLENEN_GOL >= 1.3 AND tb.MIS_BEKLENEN_GOL >= 1.3 THEN 0.65
                 WHEN tb.EV_BEKLENEN_GOL >= 1.0 AND tb.MIS_BEKLENEN_GOL >= 1.0 THEN 0.55 ELSE 0.45 END AS KG_VAR_OLAS
        FROM TahminBilgileri tb
    )
    SELECT to1.*,
        ROUND(1.0 / NULLIF(to1.EV_KAZANIR_OLAS, 0), 2) AS ADIL_ORAN_1,
        ROUND(1.0 / NULLIF(to1.BERABERE_OLAS, 0), 2) AS ADIL_ORAN_X,
        ROUND(1.0 / NULLIF(to1.MIS_KAZANIR_OLAS, 0), 2) AS ADIL_ORAN_2,
        ROUND(1.0 / NULLIF(to1.UST_2_5_OLAS, 0), 2) AS ADIL_ORAN_UST25,
        ROUND(1.0 / NULLIF(1 - to1.UST_2_5_OLAS, 0), 2) AS ADIL_ORAN_ALT25,
        CASE WHEN to1.EV_BEKLENEN_GOL >= to1.MIS_BEKLENEN_GOL * 1.3 AND to1.EV_KAZANIR_OLAS >= 0.45 THEN '1'
             WHEN to1.MIS_BEKLENEN_GOL >= to1.EV_BEKLENEN_GOL * 1.3 AND to1.MIS_KAZANIR_OLAS >= 0.45 THEN '2'
             WHEN ABS(to1.EV_BEKLENEN_GOL - to1.MIS_BEKLENEN_GOL) < 0.3 THEN 'X veya CS' ELSE 'CS' END AS SONUC_TAVSIYE,
        CASE WHEN to1.TOPLAM_BEKLENEN_GOL >= 3.0 THEN 'UST 2.5'
             WHEN to1.TOPLAM_BEKLENEN_GOL <= 2.0 THEN 'ALT 2.5' ELSE 'UST 1.5 veya KG' END AS GOL_TAVSIYE,
        CASE WHEN to1.EV_BEKLENEN_GOL >= 1.2 AND to1.MIS_BEKLENEN_GOL >= 1.2 THEN 'KG VAR'
             WHEN to1.EV_BEKLENEN_GOL < 0.8 OR to1.MIS_BEKLENEN_GOL < 0.8 THEN 'KG YOK' ELSE 'Belirsiz' END AS KG_TAVSIYE,
        CASE WHEN to1.GUVENILIRLIK = 'YUKSEK' AND to1.VERI_SKORU >= 80 THEN 'YUKSEK GUVEN'
             WHEN to1.GUVENILIRLIK = 'ORTA' AND to1.VERI_SKORU >= 60 THEN 'ORTA GUVEN' ELSE 'DUSUK GUVEN' END AS TAHMIN_GUVEN,
        CASE WHEN to1.GUVENILIRLIK = 'YUKSEK' AND to1.VERI_SKORU >= 80
                  AND (to1.EV_KAZANIR_OLAS >= 0.50 OR to1.MIS_KAZANIR_OLAS >= 0.50
                       OR to1.UST_2_5_OLAS >= 0.65 OR to1.KG_VAR_OLAS >= 0.60) THEN 9
             WHEN to1.GUVENILIRLIK = 'YUKSEK' AND to1.VERI_SKORU >= 70 THEN 7
             WHEN to1.GUVENILIRLIK = 'ORTA' AND to1.VERI_SKORU >= 60 THEN 5 ELSE 3 END AS ONERI_SKORU
    FROM TahminOlasiliklari to1
    """
    try:
        cursor.execute(bahis_view_sql)
        conn.commit()
        print("   OK - v_Mac_Bahis_Oneri oluşturuldu")
    except Exception as e:
        print(f"   HATA: {e}")

    # =============================================
    # 3. Test
    # =============================================
    print("\n[3/3] Sistem testi yapılıyor...")

    try:
        cursor.execute("SELECT COUNT(*) FROM TAHMIN.v_Mac_Tahmin")
        mac_sayisi = cursor.fetchone()[0]
        print(f"   Tahmin edilebilir maç: {mac_sayisi}")
    except Exception as e:
        print(f"   Tahmin VIEW testi başarısız: {e}")
        mac_sayisi = 0

    if mac_sayisi > 0:
        print("\n" + "=" * 80)
        print("ORNEK TAHMINLER (Poisson ile hesaplanmis)")
        print("=" * 80)

        cursor.execute("""
            SELECT TOP 10 FIKSTURID, LIG_ADI, EVSAHIBI, MISAFIR, TARIH,
                   EV_BEKLENEN_GOL, MIS_BEKLENEN_GOL, TOPLAM_BEKLENEN_GOL, GUVENILIRLIK
            FROM TAHMIN.v_Mac_Tahmin
            WHERE TARIH >= GETDATE()
            ORDER BY TARIH
        """)

        for row in cursor.fetchall():
            fid, lig, ev, mis, tarih, ev_l, mis_l, top, guven = row
            if ev_l and mis_l:
                probs = calculate_probs(float(ev_l), float(mis_l))
                print(f"\n  [{lig}] {ev} vs {mis}")
                print(f"    Tarih: {tarih.strftime('%Y-%m-%d %H:%M') if tarih else '-'}")
                print(f"    Beklenen: {ev_l:.2f} - {mis_l:.2f} (Top: {top:.2f})")
                print(f"    1X2: 1={probs['1']}% | X={probs['X']}% | 2={probs['2']}%")
                print(f"    U/A 2.5: U={probs['ust_25']}% | A={probs['alt_25']}%")
                print(f"    KG: Var={probs['kg_var']}% | Yok={probs['kg_yok']}%")
                print(f"    En Olasi: {probs['en_olasi']} ({probs['en_olasi_olas']}%)")

    conn.close()
    print("\n" + "=" * 80)
    print("Kurulum tamamlandi!")
    print("=" * 80)


if __name__ == "__main__":
    main()
