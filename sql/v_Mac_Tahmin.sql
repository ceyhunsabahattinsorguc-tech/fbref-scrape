-- =============================================
-- TAHMIN.v_Mac_Tahmin
-- Poisson tabanlı maç skor tahmini VIEW'i
-- Ev ve Misafir için beklenen gol (lambda) hesaplar
-- Oluşturma: 2025-12-09
-- =============================================

-- Eski view'i sil (varsa)
IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'TAHMIN.v_Mac_Tahmin'))
    DROP VIEW TAHMIN.v_Mac_Tahmin
GO

CREATE VIEW TAHMIN.v_Mac_Tahmin AS
WITH OynanmamisMaclar AS (
    -- Henüz oynanmamış maçları al
    SELECT
        f.FIKSTURID,
        f.LIG_ID,
        f.EVSAHIBI,
        f.MISAFIR,
        f.TARIH,
        f.HAFTA
    FROM FIKSTUR.FIKSTUR f
    WHERE f.DURUM = 0  -- Oynanmamış
),
EvSahibiBilgi AS (
    -- Ev sahibi takım güç analizi
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
    -- Misafir takım güç analizi
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

    -- Takım güç metrikleri
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

    -- Lig ortalamaları
    ev.LIG_GOL_ORT,
    ev.LIG_EV_GOL_ORT,
    ev.LIG_DEP_GOL_ORT,

    -- =============================================
    -- POISSON LAMBDA DEĞERLERİ (Beklenen Gol)
    -- Formula: EV_LAMBDA = EV_SALDIRI × MIS_SAVUNMA × LIG_EV_ORT
    --          MIS_LAMBDA = MIS_SALDIRI × EV_SAVUNMA × LIG_DEP_ORT
    -- =============================================
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

    -- Toplam beklenen gol
    ROUND(
        ISNULL(ev.EV_SALDIRI_GUCU, ev.EV_SALDIRI) *
        ISNULL(mis.DEP_SAVUNMA_GUCU, mis.MIS_SAVUNMA) *
        ev.LIG_EV_GOL_ORT +
        ISNULL(mis.DEP_SALDIRI_GUCU, mis.MIS_SALDIRI) *
        ISNULL(ev.EV_SAVUNMA_GUCU, ev.EV_SAVUNMA) *
        ev.LIG_DEP_GOL_ORT, 2
    ) AS TOPLAM_BEKLENEN_GOL,

    -- =============================================
    -- FORM FAKTÖRLERİ (Son 5 maç bazlı)
    -- =============================================
    ROUND(CAST(ev.EV_SON5_ATILAN AS FLOAT) / NULLIF(ev.EV_SON5_YENILEN, 0), 2) AS EV_FORM_ORANI,
    ROUND(CAST(mis.MIS_SON5_ATILAN AS FLOAT) / NULLIF(mis.MIS_SON5_YENILEN, 0), 2) AS MIS_FORM_ORANI,

    -- =============================================
    -- TAHMİN GÜVENİLİRLİK SKORU
    -- Daha fazla maç = daha güvenilir tahmin
    -- =============================================
    CASE
        WHEN ev.EV_MAC_SAYISI >= 10 AND mis.MIS_MAC_SAYISI >= 10 THEN 'YUKSEK'
        WHEN ev.EV_MAC_SAYISI >= 5 AND mis.MIS_MAC_SAYISI >= 5 THEN 'ORTA'
        ELSE 'DUSUK'
    END AS GUVENILIRLIK,

    -- Veri yeterliliği skoru (0-100)
    ROUND(
        (CAST(ev.EV_MAC_SAYISI AS FLOAT) / 10 * 50) +
        (CAST(mis.MIS_MAC_SAYISI AS FLOAT) / 10 * 50), 0
    ) AS VERI_SKORU

FROM OynanmamisMaclar om
JOIN TANIM.LIG l ON om.LIG_ID = l.LIG_ID
LEFT JOIN EvSahibiBilgi ev ON om.FIKSTURID = ev.FIKSTURID
LEFT JOIN MisafirBilgi mis ON om.FIKSTURID = mis.FIKSTURID
WHERE ev.FIKSTURID IS NOT NULL AND mis.FIKSTURID IS NOT NULL
GO

-- =============================================
-- ÖRNEK SORGULAR
-- =============================================

-- Tüm tahminler
-- SELECT * FROM TAHMIN.v_Mac_Tahmin ORDER BY TARIH

-- Premier League tahminleri
-- SELECT * FROM TAHMIN.v_Mac_Tahmin WHERE LIG_ID = 6 ORDER BY TARIH

-- Yüksek gollu maçlar (toplam > 3 beklenen gol)
-- SELECT EVSAHIBI, MISAFIR, TOPLAM_BEKLENEN_GOL
-- FROM TAHMIN.v_Mac_Tahmin
-- WHERE TOPLAM_BEKLENEN_GOL > 3
-- ORDER BY TOPLAM_BEKLENEN_GOL DESC

-- Düşük gollu maçlar (toplam < 2 beklenen gol)
-- SELECT EVSAHIBI, MISAFIR, TOPLAM_BEKLENEN_GOL
-- FROM TAHMIN.v_Mac_Tahmin
-- WHERE TOPLAM_BEKLENEN_GOL < 2
-- ORDER BY TOPLAM_BEKLENEN_GOL ASC
