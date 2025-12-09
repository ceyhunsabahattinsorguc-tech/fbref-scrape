# -*- coding: utf-8 -*-
"""v_Mac_Tahmin VIEW'ini veritabanina uygula ve Poisson hesaplama yap"""

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
    """Poisson olasılık kütle fonksiyonu: P(X=k) = (λ^k * e^-λ) / k!"""
    if lam <= 0:
        return 0 if k > 0 else 1
    return (lam ** k) * math.exp(-lam) / math.factorial(k)


def calculate_match_probabilities(ev_lambda, mis_lambda, max_goals=6):
    """Maç sonuç olasılıklarını hesapla (1X2, Ü/A, vb.)"""

    # Skor matrisi oluştur
    score_matrix = {}
    for ev_gol in range(max_goals + 1):
        for mis_gol in range(max_goals + 1):
            prob = poisson_pmf(ev_gol, ev_lambda) * poisson_pmf(mis_gol, mis_lambda)
            score_matrix[(ev_gol, mis_gol)] = prob

    # 1X2 olasılıkları
    ev_kazanir = sum(p for (ev, mis), p in score_matrix.items() if ev > mis)
    berabere = sum(p for (ev, mis), p in score_matrix.items() if ev == mis)
    mis_kazanir = sum(p for (ev, mis), p in score_matrix.items() if ev < mis)

    # Üst/Alt gol olasılıkları
    ust_1_5 = sum(p for (ev, mis), p in score_matrix.items() if ev + mis > 1.5)
    ust_2_5 = sum(p for (ev, mis), p in score_matrix.items() if ev + mis > 2.5)
    ust_3_5 = sum(p for (ev, mis), p in score_matrix.items() if ev + mis > 3.5)

    # Karşılıklı Gol (KG)
    kg_var = sum(p for (ev, mis), p in score_matrix.items() if ev > 0 and mis > 0)

    # En olası skor
    en_olasi_skor = max(score_matrix.items(), key=lambda x: x[1])

    return {
        '1': round(ev_kazanir * 100, 1),
        'X': round(berabere * 100, 1),
        '2': round(mis_kazanir * 100, 1),
        'ust_1_5': round(ust_1_5 * 100, 1),
        'ust_2_5': round(ust_2_5 * 100, 1),
        'ust_3_5': round(ust_3_5 * 100, 1),
        'alt_2_5': round((1 - ust_2_5) * 100, 1),
        'kg_var': round(kg_var * 100, 1),
        'kg_yok': round((1 - kg_var) * 100, 1),
        'en_olasi_skor': f"{en_olasi_skor[0][0]}-{en_olasi_skor[0][1]}",
        'en_olasi_olasilik': round(en_olasi_skor[1] * 100, 1)
    }


def main():
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()

    print("=" * 70)
    print("MAC TAHMIN VIEW VE POISSON HESAPLAMA")
    print("=" * 70)

    # 1. Eski view'i sil
    print("\n1. Eski VIEW siliniyor...")
    try:
        cursor.execute("""
            IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'TAHMIN.v_Mac_Tahmin'))
            DROP VIEW TAHMIN.v_Mac_Tahmin
        """)
        conn.commit()
        print("   OK")
    except Exception as e:
        print(f"   Hata: {e}")

    # 2. Yeni view olustur
    print("\n2. v_Mac_Tahmin VIEW oluşturuluyor...")
    view_sql = """
    CREATE VIEW TAHMIN.v_Mac_Tahmin AS
    WITH OynanmamisMaclar AS (
        SELECT f.FIKSTURID, f.LIG_ID, f.EVSAHIBI, f.MISAFIR, f.TARIH, f.HAFTA
        FROM FIKSTUR.FIKSTUR f
        WHERE f.DURUM = 0
    ),
    EvSahibiBilgi AS (
        SELECT
            om.FIKSTURID,
            tg.TAKIM_ADI,
            tg.SALDIRI_GUCU AS EV_SALDIRI,
            tg.SAVUNMA_GUCU AS EV_SAVUNMA,
            tg.EV_SALDIRI_GUCU,
            tg.EV_SAVUNMA_GUCU,
            tg.LIG_GOL_ORT,
            tg.LIG_EV_GOL_ORT,
            tg.LIG_DEP_GOL_ORT,
            tg.MAC_SAYISI AS EV_MAC_SAYISI,
            tg.SON5_ATILAN AS EV_SON5_ATILAN,
            tg.SON5_YENILEN AS EV_SON5_YENILEN
        FROM OynanmamisMaclar om
        JOIN TAHMIN.v_Takim_Guc_Analizi tg
            ON om.EVSAHIBI = tg.TAKIM_ADI AND om.LIG_ID = tg.LIG_ID
    ),
    MisafirBilgi AS (
        SELECT
            om.FIKSTURID,
            tg.TAKIM_ADI,
            tg.SALDIRI_GUCU AS MIS_SALDIRI,
            tg.SAVUNMA_GUCU AS MIS_SAVUNMA,
            tg.DEP_SALDIRI_GUCU,
            tg.DEP_SAVUNMA_GUCU,
            tg.MAC_SAYISI AS MIS_MAC_SAYISI,
            tg.SON5_ATILAN AS MIS_SON5_ATILAN,
            tg.SON5_YENILEN AS MIS_SON5_YENILEN
        FROM OynanmamisMaclar om
        JOIN TAHMIN.v_Takim_Guc_Analizi tg
            ON om.MISAFIR = tg.TAKIM_ADI AND om.LIG_ID = tg.LIG_ID
    )
    SELECT
        om.FIKSTURID,
        om.LIG_ID,
        l.LIG_ADI,
        om.EVSAHIBI,
        om.MISAFIR,
        om.TARIH,
        om.HAFTA,
        ev.EV_SALDIRI,
        ev.EV_SAVUNMA,
        ev.EV_SALDIRI_GUCU,
        ev.EV_SAVUNMA_GUCU,
        ev.EV_MAC_SAYISI,
        ev.EV_SON5_ATILAN,
        ev.EV_SON5_YENILEN,
        mis.MIS_SALDIRI,
        mis.MIS_SAVUNMA,
        mis.DEP_SALDIRI_GUCU,
        mis.DEP_SAVUNMA_GUCU,
        mis.MIS_MAC_SAYISI,
        mis.MIS_SON5_ATILAN,
        mis.MIS_SON5_YENILEN,
        ev.LIG_GOL_ORT,
        ev.LIG_EV_GOL_ORT,
        ev.LIG_DEP_GOL_ORT,
        ROUND(
            ISNULL(ev.EV_SALDIRI_GUCU, ev.EV_SALDIRI) *
            ISNULL(mis.DEP_SAVUNMA_GUCU, mis.MIS_SAVUNMA) *
            ev.LIG_EV_GOL_ORT, 3
        ) AS EV_BEKLENEN_GOL,
        ROUND(
            ISNULL(mis.DEP_SALDIRI_GUCU, mis.MIS_SALDIRI) *
            ISNULL(ev.EV_SAVUNMA_GUCU, ev.EV_SAVUNMA) *
            ev.LIG_DEP_GOL_ORT, 3
        ) AS MIS_BEKLENEN_GOL,
        ROUND(
            ISNULL(ev.EV_SALDIRI_GUCU, ev.EV_SALDIRI) *
            ISNULL(mis.DEP_SAVUNMA_GUCU, mis.MIS_SAVUNMA) *
            ev.LIG_EV_GOL_ORT +
            ISNULL(mis.DEP_SALDIRI_GUCU, mis.MIS_SALDIRI) *
            ISNULL(ev.EV_SAVUNMA_GUCU, ev.EV_SAVUNMA) *
            ev.LIG_DEP_GOL_ORT, 2
        ) AS TOPLAM_BEKLENEN_GOL,
        ROUND(CAST(ev.EV_SON5_ATILAN AS FLOAT) / NULLIF(ev.EV_SON5_YENILEN, 0), 2) AS EV_FORM_ORANI,
        ROUND(CAST(mis.MIS_SON5_ATILAN AS FLOAT) / NULLIF(mis.MIS_SON5_YENILEN, 0), 2) AS MIS_FORM_ORANI,
        CASE
            WHEN ev.EV_MAC_SAYISI >= 10 AND mis.MIS_MAC_SAYISI >= 10 THEN 'YUKSEK'
            WHEN ev.EV_MAC_SAYISI >= 5 AND mis.MIS_MAC_SAYISI >= 5 THEN 'ORTA'
            ELSE 'DUSUK'
        END AS GUVENILIRLIK,
        ROUND(
            (CAST(ev.EV_MAC_SAYISI AS FLOAT) / 10 * 50) +
            (CAST(mis.MIS_MAC_SAYISI AS FLOAT) / 10 * 50), 0
        ) AS VERI_SKORU
    FROM OynanmamisMaclar om
    JOIN TANIM.LIG l ON om.LIG_ID = l.LIG_ID
    LEFT JOIN EvSahibiBilgi ev ON om.FIKSTURID = ev.FIKSTURID
    LEFT JOIN MisafirBilgi mis ON om.FIKSTURID = mis.FIKSTURID
    WHERE ev.FIKSTURID IS NOT NULL AND mis.FIKSTURID IS NOT NULL
    """
    try:
        cursor.execute(view_sql)
        conn.commit()
        print("   OK - VIEW oluşturuldu")
    except Exception as e:
        print(f"   Hata: {e}")
        conn.close()
        return

    # 3. Test: Kaç maç tahmin edilebilir?
    print("\n" + "=" * 70)
    print("POISSON TAHMIN ORNEKLERI")
    print("=" * 70)

    # Yaklaşan maçlar
    print("\n3. Yaklaşan maçlar (Poisson ile analiz):")
    cursor.execute("""
        SELECT TOP 15
            FIKSTURID, LIG_ADI, EVSAHIBI, MISAFIR, TARIH,
            EV_BEKLENEN_GOL, MIS_BEKLENEN_GOL, TOPLAM_BEKLENEN_GOL, GUVENILIRLIK
        FROM TAHMIN.v_Mac_Tahmin
        WHERE TARIH >= GETDATE()
        ORDER BY TARIH
    """)

    maclar = cursor.fetchall()

    for row in maclar:
        fikstur_id, lig, ev, mis, tarih, ev_lambda, mis_lambda, toplam, guven = row

        if ev_lambda and mis_lambda:
            probs = calculate_match_probabilities(float(ev_lambda), float(mis_lambda))

            print(f"\n  [{lig}] {ev} vs {mis}")
            print(f"    Tarih: {tarih.strftime('%Y-%m-%d %H:%M') if tarih else '-'}")
            print(f"    Beklenen Gol: Ev={ev_lambda:.2f} | Mis={mis_lambda:.2f} | Top={toplam:.2f}")
            print(f"    1X2: 1={probs['1']}% | X={probs['X']}% | 2={probs['2']}%")
            print(f"    Ü/A 2.5: Ü={probs['ust_2_5']}% | A={probs['alt_2_5']}%")
            print(f"    KG: Var={probs['kg_var']}% | Yok={probs['kg_yok']}%")
            print(f"    En Olası: {probs['en_olasi_skor']} ({probs['en_olasi_olasilik']}%)")
            print(f"    Güvenilirlik: {guven}")

    # Toplam istatistik
    print("\n" + "=" * 70)
    print("OZET ISTATISTIKLER")
    print("=" * 70)

    cursor.execute("SELECT COUNT(*) FROM TAHMIN.v_Mac_Tahmin")
    total = cursor.fetchone()[0]
    print(f"\n  Toplam tahmin edilebilir maç: {total}")

    cursor.execute("""
        SELECT GUVENILIRLIK, COUNT(*) as SAYI
        FROM TAHMIN.v_Mac_Tahmin
        GROUP BY GUVENILIRLIK
    """)
    for row in cursor.fetchall():
        print(f"    {row[0]}: {row[1]} maç")

    # Lig bazında dağılım
    print("\n  Lig bazında dağılım:")
    cursor.execute("""
        SELECT TOP 10 LIG_ADI, COUNT(*) as MAC_SAYISI
        FROM TAHMIN.v_Mac_Tahmin
        GROUP BY LIG_ADI
        ORDER BY MAC_SAYISI DESC
    """)
    for row in cursor.fetchall():
        print(f"    {row[0]:<30}: {row[1]} maç")

    conn.close()
    print("\nTamamlandı!")


if __name__ == "__main__":
    main()
