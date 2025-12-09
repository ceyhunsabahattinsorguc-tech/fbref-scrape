# -*- coding: utf-8 -*-
"""
Günlük Tahmin Raporu Üretici
============================
- Günlük maç tahminlerini üretir
- Bahis önerilerini listeler
- JSON ve text formatında çıktı verir
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')

import pyodbc
import json
from datetime import datetime, timedelta
from predict_match import MacTahmin, MacTahminMotoru, poisson_pmf
import math

CONNECTION_STRING = (
    "DRIVER={SQL Server};"
    "SERVER=195.201.146.224,1433;"
    "DATABASE=FBREF;"
    "UID=sa;"
    "PWD=FbRef2024Str0ng;"
)


def generate_daily_report(days_ahead: int = 3):
    """Günlük tahmin raporu üret"""
    print("=" * 80)
    print("GÜNLÜK BAHIS TAHMİN RAPORU")
    print(f"Tarih: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Kapsam: Bugün + {days_ahead} gün")
    print("=" * 80)

    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()

    # v_Mac_Tahmin VIEW'ından verileri çek
    cursor.execute("""
        SELECT
            FIKSTURID, LIG_ID, LIG_ADI, EVSAHIBI, MISAFIR, TARIH,
            EV_BEKLENEN_GOL, MIS_BEKLENEN_GOL, TOPLAM_BEKLENEN_GOL,
            GUVENILIRLIK, VERI_SKORU
        FROM TAHMIN.v_Mac_Tahmin
        WHERE TARIH >= GETDATE()
          AND TARIH <= DATEADD(day, ?, GETDATE())
        ORDER BY TARIH, LIG_ADI
    """, days_ahead)

    maclar = cursor.fetchall()

    if not maclar:
        print("\nTahmin edilebilir maç bulunamadı!")
        conn.close()
        return

    print(f"\nToplam {len(maclar)} maç bulundu.\n")

    # Lig bazında grupla
    lig_gruplari = {}
    tahmin_listesi = []

    for row in maclar:
        fikstur_id, lig_id, lig_adi, ev, mis, tarih, ev_lambda, mis_lambda, toplam, guven, veri = row

        if not ev_lambda or not mis_lambda:
            continue

        # Poisson hesaplama
        tahmin = MacTahmin(
            fikstur_id=fikstur_id,
            lig_adi=lig_adi,
            ev_sahibi=ev,
            misafir=mis,
            tarih=tarih,
            ev_lambda=float(ev_lambda),
            mis_lambda=float(mis_lambda)
        )

        # Gruplama
        if lig_adi not in lig_gruplari:
            lig_gruplari[lig_adi] = []

        mac_bilgi = {
            'fikstur_id': fikstur_id,
            'lig': lig_adi,
            'ev': ev,
            'mis': mis,
            'tarih': tarih,
            'tahmin': tahmin,
            'guven': guven,
            'veri_skoru': veri
        }

        lig_gruplari[lig_adi].append(mac_bilgi)
        tahmin_listesi.append(mac_bilgi)

    # Raporu yazdır
    for lig_adi, maclar in sorted(lig_gruplari.items()):
        print(f"\n{'='*80}")
        print(f"  {lig_adi.upper()}")
        print(f"{'='*80}")

        for m in maclar:
            t = m['tahmin']
            oranlar = t.get_1x2()
            ust_alt = t.get_ust_alt()
            kg = t.get_kg()
            skorlar = t.get_en_olasi_skorlar(3)

            print(f"\n  {m['ev']} vs {m['mis']}")
            print(f"    Tarih: {m['tarih'].strftime('%Y-%m-%d %H:%M') if m['tarih'] else '-'}")
            print(f"    Güvenilirlik: {m['guven']} (Veri: {m['veri_skoru']}%)")
            print(f"    Beklenen Gol: {t.ev_lambda:.2f} - {t.mis_lambda:.2f} (Top: {t.ev_lambda + t.mis_lambda:.2f})")
            print(f"    1X2: 1={oranlar['1']}% | X={oranlar['X']}% | 2={oranlar['2']}%")
            print(f"    Ü/A 2.5: Ü={ust_alt['ust_2.5']}% | A={ust_alt['alt_2.5']}%")
            print(f"    KG: Var={kg['var']}% | Yok={kg['yok']}%")
            print(f"    En Olası: {', '.join([f'{s[0]} ({s[1]}%)' for s in skorlar])}")

            # Tavsiyeler
            tavsiyeler = []

            # 1X2 tavsiyesi
            if oranlar['1'] >= 50:
                tavsiyeler.append(f"1 ({oranlar['1']}%)")
            elif oranlar['2'] >= 50:
                tavsiyeler.append(f"2 ({oranlar['2']}%)")
            elif oranlar['X'] >= 30:
                tavsiyeler.append(f"X veya ÇS ({oranlar['X']}%)")

            # Gol tavsiyesi
            if ust_alt['ust_2.5'] >= 60:
                tavsiyeler.append(f"Ü2.5 ({ust_alt['ust_2.5']}%)")
            elif ust_alt['alt_2.5'] >= 60:
                tavsiyeler.append(f"A2.5 ({ust_alt['alt_2.5']}%)")

            # KG tavsiyesi
            if kg['var'] >= 60:
                tavsiyeler.append(f"KG Var ({kg['var']}%)")
            elif kg['yok'] >= 60:
                tavsiyeler.append(f"KG Yok ({kg['yok']}%)")

            if tavsiyeler:
                print(f"    >>> TAVSİYE: {' | '.join(tavsiyeler)}")

    # En güçlü tahminler özeti
    print("\n" + "=" * 80)
    print("  EN GÜÇLÜ TAHMİNLER (Yüksek Güvenilirlik)")
    print("=" * 80)

    # Güçlü 1 tahminleri
    guclu_1 = [m for m in tahmin_listesi
               if m['tahmin'].ev_kazanir >= 0.50 and m['guven'] == 'YUKSEK']
    if guclu_1:
        print("\n  Ev Kazanır (1) Tahminleri:")
        for m in sorted(guclu_1, key=lambda x: x['tahmin'].ev_kazanir, reverse=True)[:5]:
            print(f"    - [{m['lig']}] {m['ev']} vs {m['mis']}: {round(m['tahmin'].ev_kazanir*100,1)}%")

    # Güçlü 2 tahminleri
    guclu_2 = [m for m in tahmin_listesi
               if m['tahmin'].mis_kazanir >= 0.45 and m['guven'] == 'YUKSEK']
    if guclu_2:
        print("\n  Misafir Kazanır (2) Tahminleri:")
        for m in sorted(guclu_2, key=lambda x: x['tahmin'].mis_kazanir, reverse=True)[:5]:
            print(f"    - [{m['lig']}] {m['ev']} vs {m['mis']}: {round(m['tahmin'].mis_kazanir*100,1)}%")

    # Üst 2.5 tahminleri
    ust_25 = [m for m in tahmin_listesi
              if m['tahmin'].ust_2_5 >= 0.65 and m['guven'] in ['YUKSEK', 'ORTA']]
    if ust_25:
        print("\n  Üst 2.5 Gol Tahminleri:")
        for m in sorted(ust_25, key=lambda x: x['tahmin'].ust_2_5, reverse=True)[:5]:
            print(f"    - [{m['lig']}] {m['ev']} vs {m['mis']}: {round(m['tahmin'].ust_2_5*100,1)}% (Top: {m['tahmin'].ev_lambda + m['tahmin'].mis_lambda:.2f})")

    # Alt 2.5 tahminleri
    alt_25 = [m for m in tahmin_listesi
              if (1 - m['tahmin'].ust_2_5) >= 0.60 and m['guven'] in ['YUKSEK', 'ORTA']]
    if alt_25:
        print("\n  Alt 2.5 Gol Tahminleri:")
        for m in sorted(alt_25, key=lambda x: 1-x['tahmin'].ust_2_5, reverse=True)[:5]:
            print(f"    - [{m['lig']}] {m['ev']} vs {m['mis']}: {round((1-m['tahmin'].ust_2_5)*100,1)}% (Top: {m['tahmin'].ev_lambda + m['tahmin'].mis_lambda:.2f})")

    # KG Var tahminleri
    kg_var = [m for m in tahmin_listesi
              if m['tahmin'].kg_var >= 0.65 and m['guven'] in ['YUKSEK', 'ORTA']]
    if kg_var:
        print("\n  Karşılıklı Gol Var Tahminleri:")
        for m in sorted(kg_var, key=lambda x: x['tahmin'].kg_var, reverse=True)[:5]:
            print(f"    - [{m['lig']}] {m['ev']} vs {m['mis']}: {round(m['tahmin'].kg_var*100,1)}%")

    conn.close()
    print("\n" + "=" * 80)
    print("Rapor tamamlandı!")
    print("=" * 80)

    return tahmin_listesi


def export_to_json(tahminler: list, filename: str = "predictions.json"):
    """Tahminleri JSON olarak kaydet"""
    output = {
        "generated_at": datetime.now().isoformat(),
        "predictions": []
    }

    for m in tahminler:
        t = m['tahmin']
        output["predictions"].append(t.to_dict())

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"Tahminler {filename} dosyasına kaydedildi.")


if __name__ == "__main__":
    tahminler = generate_daily_report(days_ahead=7)

    if tahminler:
        # JSON olarak da kaydet
        export_to_json(tahminler, "predictions.json")
