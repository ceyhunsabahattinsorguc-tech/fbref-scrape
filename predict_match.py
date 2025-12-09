# -*- coding: utf-8 -*-
"""
Poisson Tabanlı Maç Skor Tahmin Motoru
======================================
- Poisson dağılımı ile skor olasılıkları hesaplar
- 1X2, Üst/Alt, KG gibi bahis pazarlarını analiz eder
- Value Bet tespit eder
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')

import math
import pyodbc
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import json


CONNECTION_STRING = (
    "DRIVER={SQL Server};"
    "SERVER=195.201.146.224,1433;"
    "DATABASE=FBREF;"
    "UID=sa;"
    "PWD=FbRef2024Str0ng;"
)


def poisson_pmf(k: int, lam: float) -> float:
    """Poisson olasılık kütle fonksiyonu: P(X=k) = (λ^k * e^-λ) / k!"""
    if lam <= 0:
        return 0.0 if k > 0 else 1.0
    return (lam ** k) * math.exp(-lam) / math.factorial(k)


@dataclass
class MacTahmin:
    """Tek bir maç için tahmin sonuçları"""
    fikstur_id: int
    lig_adi: str
    ev_sahibi: str
    misafir: str
    tarih: datetime
    ev_lambda: float
    mis_lambda: float

    # 1X2 olasılıkları
    ev_kazanir: float = 0.0
    berabere: float = 0.0
    mis_kazanir: float = 0.0

    # Üst/Alt
    ust_1_5: float = 0.0
    ust_2_5: float = 0.0
    ust_3_5: float = 0.0

    # Karşılıklı Gol
    kg_var: float = 0.0
    kg_yok: float = 0.0

    # En olası skorlar
    en_olasi_skor: str = ""
    en_olasi_olasilik: float = 0.0

    # Skor matrisi
    skor_matrisi: Dict[Tuple[int, int], float] = None

    def __post_init__(self):
        if self.skor_matrisi is None:
            self.skor_matrisi = {}
        self._hesapla()

    def _hesapla(self, max_gol: int = 7):
        """Tüm olasılıkları hesapla"""
        # Skor matrisi oluştur
        for ev in range(max_gol + 1):
            for mis in range(max_gol + 1):
                prob = poisson_pmf(ev, self.ev_lambda) * poisson_pmf(mis, self.mis_lambda)
                self.skor_matrisi[(ev, mis)] = prob

        # 1X2
        self.ev_kazanir = sum(p for (ev, mis), p in self.skor_matrisi.items() if ev > mis)
        self.berabere = sum(p for (ev, mis), p in self.skor_matrisi.items() if ev == mis)
        self.mis_kazanir = sum(p for (ev, mis), p in self.skor_matrisi.items() if ev < mis)

        # Üst/Alt
        self.ust_1_5 = sum(p for (ev, mis), p in self.skor_matrisi.items() if ev + mis > 1.5)
        self.ust_2_5 = sum(p for (ev, mis), p in self.skor_matrisi.items() if ev + mis > 2.5)
        self.ust_3_5 = sum(p for (ev, mis), p in self.skor_matrisi.items() if ev + mis > 3.5)

        # KG
        self.kg_var = sum(p for (ev, mis), p in self.skor_matrisi.items() if ev > 0 and mis > 0)
        self.kg_yok = 1 - self.kg_var

        # En olası skor
        en_olasi = max(self.skor_matrisi.items(), key=lambda x: x[1])
        self.en_olasi_skor = f"{en_olasi[0][0]}-{en_olasi[0][1]}"
        self.en_olasi_olasilik = en_olasi[1]

    def get_1x2(self) -> Dict[str, float]:
        return {
            '1': round(self.ev_kazanir * 100, 1),
            'X': round(self.berabere * 100, 1),
            '2': round(self.mis_kazanir * 100, 1)
        }

    def get_oran_1x2(self) -> Dict[str, float]:
        """1X2 için tahmini bahis oranları (1/olasılık)"""
        return {
            '1': round(1 / max(self.ev_kazanir, 0.01), 2),
            'X': round(1 / max(self.berabere, 0.01), 2),
            '2': round(1 / max(self.mis_kazanir, 0.01), 2)
        }

    def get_ust_alt(self) -> Dict[str, float]:
        return {
            'ust_1.5': round(self.ust_1_5 * 100, 1),
            'alt_1.5': round((1 - self.ust_1_5) * 100, 1),
            'ust_2.5': round(self.ust_2_5 * 100, 1),
            'alt_2.5': round((1 - self.ust_2_5) * 100, 1),
            'ust_3.5': round(self.ust_3_5 * 100, 1),
            'alt_3.5': round((1 - self.ust_3_5) * 100, 1),
        }

    def get_kg(self) -> Dict[str, float]:
        return {
            'var': round(self.kg_var * 100, 1),
            'yok': round(self.kg_yok * 100, 1)
        }

    def get_en_olasi_skorlar(self, top_n: int = 5) -> List[Tuple[str, float]]:
        """En olası n skoru döndür"""
        sorted_scores = sorted(self.skor_matrisi.items(), key=lambda x: x[1], reverse=True)
        return [(f"{s[0][0]}-{s[0][1]}", round(s[1] * 100, 1)) for s in sorted_scores[:top_n]]

    def to_dict(self) -> dict:
        """Tüm tahminleri dict olarak döndür"""
        return {
            'fikstur_id': self.fikstur_id,
            'lig': self.lig_adi,
            'ev_sahibi': self.ev_sahibi,
            'misafir': self.misafir,
            'tarih': self.tarih.isoformat() if self.tarih else None,
            'beklenen_gol': {
                'ev': round(self.ev_lambda, 2),
                'misafir': round(self.mis_lambda, 2),
                'toplam': round(self.ev_lambda + self.mis_lambda, 2)
            },
            '1x2': self.get_1x2(),
            'oran_1x2': self.get_oran_1x2(),
            'ust_alt': self.get_ust_alt(),
            'kg': self.get_kg(),
            'en_olasi_skorlar': self.get_en_olasi_skorlar()
        }


def value_bet_kontrol(tahmin_olasilik: float, bahis_orani: float, esik: float = 1.05) -> Tuple[bool, float]:
    """
    Value Bet kontrolü
    VALUE = TAHMİN_OLASILIK × BAHİS_ORANI
    Eğer > esik (örn: 1.05) ise value bet var
    """
    value = tahmin_olasilik * bahis_orani
    return value >= esik, round(value, 3)


def kelly_criterion(tahmin_olasilik: float, bahis_orani: float) -> float:
    """
    Kelly Kriteri ile optimum bahis yüzdesi
    Kelly = (b*p - q) / b
    b = bahis oranı - 1 (net kazanç)
    p = tahmin edilen kazanma olasılığı
    q = 1 - p (kaybetme olasılığı)
    """
    if bahis_orani <= 1:
        return 0.0
    b = bahis_orani - 1
    p = tahmin_olasilik
    q = 1 - p
    kelly = (b * p - q) / b
    return max(0, round(kelly * 100, 2))  # Yüzde olarak, negatif ise 0


class MacTahminMotoru:
    """Maç tahmin ana sınıfı"""

    def __init__(self):
        self.conn = None

    def baglan(self):
        self.conn = pyodbc.connect(CONNECTION_STRING)

    def kapat(self):
        if self.conn:
            self.conn.close()

    def mac_tahmin_al(self, fikstur_id: int) -> Optional[MacTahmin]:
        """Belirli bir maç için tahmin al"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT FIKSTURID, LIG_ADI, EVSAHIBI, MISAFIR, TARIH,
                   EV_BEKLENEN_GOL, MIS_BEKLENEN_GOL
            FROM TAHMIN.v_Mac_Tahmin
            WHERE FIKSTURID = ?
        """, fikstur_id)
        row = cursor.fetchone()

        if not row:
            return None

        return MacTahmin(
            fikstur_id=row[0],
            lig_adi=row[1],
            ev_sahibi=row[2],
            misafir=row[3],
            tarih=row[4],
            ev_lambda=float(row[5]) if row[5] else 1.0,
            mis_lambda=float(row[6]) if row[6] else 1.0
        )

    def yaklasan_maclar(self, limit: int = 50) -> List[MacTahmin]:
        """Yaklaşan maçlar için tahminler"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT TOP (?)
                   FIKSTURID, LIG_ADI, EVSAHIBI, MISAFIR, TARIH,
                   EV_BEKLENEN_GOL, MIS_BEKLENEN_GOL
            FROM TAHMIN.v_Mac_Tahmin
            WHERE TARIH >= GETDATE()
            ORDER BY TARIH
        """, limit)

        tahminler = []
        for row in cursor.fetchall():
            if row[5] and row[6]:
                tahminler.append(MacTahmin(
                    fikstur_id=row[0],
                    lig_adi=row[1],
                    ev_sahibi=row[2],
                    misafir=row[3],
                    tarih=row[4],
                    ev_lambda=float(row[5]),
                    mis_lambda=float(row[6])
                ))
        return tahminler

    def lig_tahminleri(self, lig_id: int) -> List[MacTahmin]:
        """Belirli bir lig için yaklaşan maç tahminleri"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT FIKSTURID, LIG_ADI, EVSAHIBI, MISAFIR, TARIH,
                   EV_BEKLENEN_GOL, MIS_BEKLENEN_GOL
            FROM TAHMIN.v_Mac_Tahmin
            WHERE LIG_ID = ? AND TARIH >= GETDATE()
            ORDER BY TARIH
        """, lig_id)

        tahminler = []
        for row in cursor.fetchall():
            if row[5] and row[6]:
                tahminler.append(MacTahmin(
                    fikstur_id=row[0],
                    lig_adi=row[1],
                    ev_sahibi=row[2],
                    misafir=row[3],
                    tarih=row[4],
                    ev_lambda=float(row[5]),
                    mis_lambda=float(row[6])
                ))
        return tahminler


def main():
    """Test amaçlı ana fonksiyon"""
    print("=" * 70)
    print("POISSON MAC TAHMIN MOTORU")
    print("=" * 70)

    motor = MacTahminMotoru()
    motor.baglan()

    # Yaklaşan maçları al
    tahminler = motor.yaklasan_maclar(15)

    if not tahminler:
        print("\nHenüz tahmin edilebilir maç yok!")
        print("v_Mac_Tahmin VIEW'ının oluşturulduğundan emin olun.")
        motor.kapat()
        return

    print(f"\n{len(tahminler)} maç için tahminler:")
    print("-" * 70)

    for t in tahminler:
        print(f"\n[{t.lig_adi}]")
        print(f"  {t.ev_sahibi} vs {t.misafir}")
        print(f"  Tarih: {t.tarih.strftime('%Y-%m-%d %H:%M') if t.tarih else '-'}")
        print(f"  Beklenen Gol: {t.ev_lambda:.2f} - {t.mis_lambda:.2f} (Toplam: {t.ev_lambda + t.mis_lambda:.2f})")

        oranlar = t.get_1x2()
        print(f"  1X2: 1={oranlar['1']}% | X={oranlar['X']}% | 2={oranlar['2']}%")

        ust_alt = t.get_ust_alt()
        print(f"  Ü/A 2.5: Ü={ust_alt['ust_2.5']}% | A={ust_alt['alt_2.5']}%")

        kg = t.get_kg()
        print(f"  KG: Var={kg['var']}% | Yok={kg['yok']}%")

        skorlar = t.get_en_olasi_skorlar(3)
        print(f"  En Olası: {', '.join([f'{s[0]} ({s[1]}%)' for s in skorlar])}")

    motor.kapat()
    print("\n" + "=" * 70)
    print("Tamamlandı!")


if __name__ == "__main__":
    main()
