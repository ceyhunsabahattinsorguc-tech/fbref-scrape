-- =============================================
-- TAHMIN.v_Mac_Bahis_Oneri
-- Value Bet analizi ve bahis önerileri VIEW'i
-- Bahisçi oranları ile kendi tahminlerimizi karşılaştırır
-- Oluşturma: 2025-12-09
-- =============================================

-- Not: Bu VIEW, veritabanında bahis oranları tablosu
-- (BAHIS.ORANLAR) mevcutsa çalışır. Yoksa temel tahminleri döndürür.

-- Eski view'i sil (varsa)
IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'TAHMIN.v_Mac_Bahis_Oneri'))
    DROP VIEW TAHMIN.v_Mac_Bahis_Oneri
GO

CREATE VIEW TAHMIN.v_Mac_Bahis_Oneri AS
WITH TahminBilgileri AS (
    SELECT
        mt.FIKSTURID,
        mt.LIG_ID,
        mt.LIG_ADI,
        mt.EVSAHIBI,
        mt.MISAFIR,
        mt.TARIH,
        mt.HAFTA,
        mt.EV_BEKLENEN_GOL,
        mt.MIS_BEKLENEN_GOL,
        mt.TOPLAM_BEKLENEN_GOL,
        mt.GUVENILIRLIK,
        mt.VERI_SKORU
    FROM TAHMIN.v_Mac_Tahmin mt
),
-- Basit Poisson hesaplaması için yaklaşık değerler
-- (Gerçek Poisson Python tarafında hesaplanır, burada yaklaşık)
TahminOlasiliklari AS (
    SELECT
        tb.*,

        -- 1X2 yaklaşık olasılıkları (Poisson temelli basitleştirilmiş)
        -- Ev kazanır: Ev lambda > Mis lambda ise yüksek
        CASE
            WHEN tb.EV_BEKLENEN_GOL > tb.MIS_BEKLENEN_GOL * 1.5 THEN 0.55
            WHEN tb.EV_BEKLENEN_GOL > tb.MIS_BEKLENEN_GOL * 1.2 THEN 0.45
            WHEN tb.EV_BEKLENEN_GOL > tb.MIS_BEKLENEN_GOL THEN 0.38
            WHEN tb.EV_BEKLENEN_GOL = tb.MIS_BEKLENEN_GOL THEN 0.33
            WHEN tb.EV_BEKLENEN_GOL * 1.2 > tb.MIS_BEKLENEN_GOL THEN 0.30
            ELSE 0.25
        END AS EV_KAZANIR_OLAS,

        CASE
            WHEN ABS(tb.EV_BEKLENEN_GOL - tb.MIS_BEKLENEN_GOL) < 0.3 THEN 0.30
            WHEN ABS(tb.EV_BEKLENEN_GOL - tb.MIS_BEKLENEN_GOL) < 0.5 THEN 0.27
            ELSE 0.24
        END AS BERABERE_OLAS,

        CASE
            WHEN tb.MIS_BEKLENEN_GOL > tb.EV_BEKLENEN_GOL * 1.5 THEN 0.50
            WHEN tb.MIS_BEKLENEN_GOL > tb.EV_BEKLENEN_GOL * 1.2 THEN 0.40
            WHEN tb.MIS_BEKLENEN_GOL > tb.EV_BEKLENEN_GOL THEN 0.35
            ELSE 0.25
        END AS MIS_KAZANIR_OLAS,

        -- Üst 2.5 olasılığı (basitleştirilmiş)
        CASE
            WHEN tb.TOPLAM_BEKLENEN_GOL >= 3.5 THEN 0.70
            WHEN tb.TOPLAM_BEKLENEN_GOL >= 3.0 THEN 0.60
            WHEN tb.TOPLAM_BEKLENEN_GOL >= 2.5 THEN 0.50
            WHEN tb.TOPLAM_BEKLENEN_GOL >= 2.0 THEN 0.40
            ELSE 0.30
        END AS UST_2_5_OLAS,

        -- KG Var olasılığı
        CASE
            WHEN tb.EV_BEKLENEN_GOL >= 1.3 AND tb.MIS_BEKLENEN_GOL >= 1.3 THEN 0.65
            WHEN tb.EV_BEKLENEN_GOL >= 1.0 AND tb.MIS_BEKLENEN_GOL >= 1.0 THEN 0.55
            ELSE 0.45
        END AS KG_VAR_OLAS

    FROM TahminBilgileri tb
)
SELECT
    to1.*,

    -- Tahmini adil oranlar (1/olasılık)
    ROUND(1.0 / NULLIF(to1.EV_KAZANIR_OLAS, 0), 2) AS ADIL_ORAN_1,
    ROUND(1.0 / NULLIF(to1.BERABERE_OLAS, 0), 2) AS ADIL_ORAN_X,
    ROUND(1.0 / NULLIF(to1.MIS_KAZANIR_OLAS, 0), 2) AS ADIL_ORAN_2,
    ROUND(1.0 / NULLIF(to1.UST_2_5_OLAS, 0), 2) AS ADIL_ORAN_UST25,
    ROUND(1.0 / NULLIF(1 - to1.UST_2_5_OLAS, 0), 2) AS ADIL_ORAN_ALT25,
    ROUND(1.0 / NULLIF(to1.KG_VAR_OLAS, 0), 2) AS ADIL_ORAN_KG_VAR,
    ROUND(1.0 / NULLIF(1 - to1.KG_VAR_OLAS, 0), 2) AS ADIL_ORAN_KG_YOK,

    -- =============================================
    -- BAHİS TAVSİYELERİ
    -- =============================================

    -- Favori tavsiyesi
    CASE
        WHEN to1.EV_BEKLENEN_GOL >= to1.MIS_BEKLENEN_GOL * 1.3 AND to1.EV_KAZANIR_OLAS >= 0.45 THEN '1'
        WHEN to1.MIS_BEKLENEN_GOL >= to1.EV_BEKLENEN_GOL * 1.3 AND to1.MIS_KAZANIR_OLAS >= 0.45 THEN '2'
        WHEN ABS(to1.EV_BEKLENEN_GOL - to1.MIS_BEKLENEN_GOL) < 0.3 THEN 'X veya ÇS'
        ELSE 'ÇS'
    END AS SONUC_TAVSIYE,

    -- Gol tavsiyesi
    CASE
        WHEN to1.TOPLAM_BEKLENEN_GOL >= 3.0 THEN 'UST 2.5'
        WHEN to1.TOPLAM_BEKLENEN_GOL <= 2.0 THEN 'ALT 2.5'
        ELSE 'UST 1.5 veya KG'
    END AS GOL_TAVSIYE,

    -- KG tavsiyesi
    CASE
        WHEN to1.EV_BEKLENEN_GOL >= 1.2 AND to1.MIS_BEKLENEN_GOL >= 1.2 THEN 'KG VAR'
        WHEN to1.EV_BEKLENEN_GOL < 0.8 OR to1.MIS_BEKLENEN_GOL < 0.8 THEN 'KG YOK'
        ELSE 'KG belirsiz'
    END AS KG_TAVSIYE,

    -- Güvenilirlik seviyesi
    CASE
        WHEN to1.GUVENILIRLIK = 'YUKSEK' AND to1.VERI_SKORU >= 80 THEN 'YUKSEK GUVEN'
        WHEN to1.GUVENILIRLIK = 'ORTA' AND to1.VERI_SKORU >= 60 THEN 'ORTA GUVEN'
        ELSE 'DUSUK GUVEN'
    END AS TAHMIN_GUVEN,

    -- Önerilen bahis skoru (1-10)
    CASE
        WHEN to1.GUVENILIRLIK = 'YUKSEK' AND to1.VERI_SKORU >= 80
             AND (to1.EV_KAZANIR_OLAS >= 0.50 OR to1.MIS_KAZANIR_OLAS >= 0.50
                  OR to1.UST_2_5_OLAS >= 0.65 OR to1.KG_VAR_OLAS >= 0.60) THEN 9
        WHEN to1.GUVENILIRLIK = 'YUKSEK' AND to1.VERI_SKORU >= 70 THEN 7
        WHEN to1.GUVENILIRLIK = 'ORTA' AND to1.VERI_SKORU >= 60 THEN 5
        ELSE 3
    END AS ONERI_SKORU

FROM TahminOlasiliklari to1
GO

-- =============================================
-- ÖRNEK SORGULAR
-- =============================================

-- Yüksek güvenli tahminler
-- SELECT * FROM TAHMIN.v_Mac_Bahis_Oneri
-- WHERE TAHMIN_GUVEN = 'YUKSEK GUVEN'
-- ORDER BY ONERI_SKORU DESC, TARIH

-- Bugünkü öneriler
-- SELECT * FROM TAHMIN.v_Mac_Bahis_Oneri
-- WHERE CAST(TARIH AS DATE) = CAST(GETDATE() AS DATE)
-- ORDER BY ONERI_SKORU DESC

-- Üst 2.5 önerileri
-- SELECT * FROM TAHMIN.v_Mac_Bahis_Oneri
-- WHERE GOL_TAVSIYE = 'UST 2.5'
-- ORDER BY TOPLAM_BEKLENEN_GOL DESC
