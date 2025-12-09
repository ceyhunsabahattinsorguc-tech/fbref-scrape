-- =============================================
-- TAHMIN.v_Oyuncu_Gol_Skoru
-- Oyuncu gol atma ihtimali skoru VIEW'i
-- Olusturma: 2025-12-08
-- =============================================

-- Eski view'i sil (varsa)
IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'TAHMIN.v_Oyuncu_Gol_Skoru'))
    DROP VIEW TAHMIN.v_Oyuncu_Gol_Skoru
GO

-- TAHMIN schema yoksa olustur
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'TAHMIN')
    EXEC('CREATE SCHEMA TAHMIN')
GO

CREATE VIEW TAHMIN.v_Oyuncu_Gol_Skoru AS
WITH OyuncuStats AS (
    SELECT
        o.OYUNCU_ID,
        o.OYUNCU_ADI,
        t.TAKIM_ADI,
        t.TAKIM_ID,
        l.LIG_ID,
        l.LIG_ADI,

        -- Toplam mac sayisi
        COUNT(DISTINCT p.FIKSTURID) AS MAC_SAYISI,

        -- Gol istatistikleri
        SUM(ISNULL(p.GOL, 0)) AS TOPLAM_GOL,
        SUM(ISNULL(p.SUT, 0)) AS TOPLAM_SUT,
        SUM(ISNULL(p.ISABETLI_SUT, 0)) AS TOPLAM_ISABETLI_SUT,
        SUM(ISNULL(p.BEKLENEN_GOL, 0)) AS TOPLAM_XG,

        -- Ortalamalar
        CAST(SUM(ISNULL(p.GOL, 0)) AS FLOAT) / NULLIF(COUNT(DISTINCT p.FIKSTURID), 0) AS GOL_ORTALAMASI,
        CAST(SUM(ISNULL(p.SUT, 0)) AS FLOAT) / NULLIF(COUNT(DISTINCT p.FIKSTURID), 0) AS SUT_ORTALAMASI,

        -- Sut donusum orani
        CAST(SUM(ISNULL(p.GOL, 0)) AS FLOAT) / NULLIF(SUM(ISNULL(p.SUT, 0)), 0) * 100 AS SUT_DONUSUM_ORANI,

        -- xG performansi (gol - xG)
        SUM(ISNULL(p.GOL, 0)) - SUM(ISNULL(p.BEKLENEN_GOL, 0)) AS XG_FARKI,

        -- Penalti istatistikleri
        SUM(ISNULL(p.PENALTI_ATISI, 0)) AS PENALTI_ATISI,
        SUM(ISNULL(p.PENALTI_GOL, 0)) AS PENALTI_GOL,

        -- Duran top
        SUM(ISNULL(p.KORNER, 0)) AS KORNER,
        SUM(ISNULL(p.SERBEST_VURUS_PASI, 0)) AS SERBEST_VURUS,

        -- Hava topu
        SUM(ISNULL(p.HAVA_TOPU_KAZANILAN, 0)) AS HAVA_TOPU_KAZANILAN,
        CAST(SUM(ISNULL(p.HAVA_TOPU_KAZANILAN, 0)) AS FLOAT) /
            NULLIF(SUM(ISNULL(p.HAVA_TOPU_KAZANILAN, 0)) + SUM(ISNULL(p.HAVA_TOPU_KAYBEDILEN, 0)), 0) * 100 AS HAVA_TOPU_BASARI,

        -- Ceza sahasi temasi
        SUM(ISNULL(p.TEMAS_HUCUM_CEZA, 0)) AS CEZA_SAHASI_TEMASI,
        CAST(SUM(ISNULL(p.TEMAS_HUCUM_CEZA, 0)) AS FLOAT) / NULLIF(COUNT(DISTINCT p.FIKSTURID), 0) AS CEZA_SAHASI_ORT,

        -- Dribling
        CAST(SUM(ISNULL(p.CARPISMA_BASARILI, 0)) AS FLOAT) /
            NULLIF(SUM(ISNULL(p.CARPISMA_DENEME, 0)), 0) * 100 AS DRIBLING_BASARI,

        -- Dakika
        AVG(ISNULL(p.SURE, 0)) AS ORTALAMA_DAKIKA,

        -- Son mac tarihi
        MAX(f.TARIH) AS SON_MAC_TARIHI

    FROM TANIM.OYUNCU o
    JOIN FIKSTUR.PERFORMANS p ON o.OYUNCU_ID = p.OYUNCU_ID
    JOIN FIKSTUR.FIKSTUR f ON p.FIKSTURID = f.FIKSTURID
    JOIN TANIM.TAKIM t ON p.TAKIM_ID = t.TAKIM_ID
    JOIN TANIM.LIG l ON f.LIG_ID = l.LIG_ID
    WHERE p.SURE > 0
    GROUP BY o.OYUNCU_ID, o.OYUNCU_ADI, t.TAKIM_ADI, t.TAKIM_ID, l.LIG_ID, l.LIG_ADI
    HAVING COUNT(DISTINCT p.FIKSTURID) >= 3  -- En az 3 mac oynamis
)
SELECT
    *,

    -- =============================================
    -- FORM PUANI (40 puan max)
    -- Gol ortalamasi, sut donusum, xG performansi
    -- =============================================
    CASE WHEN GOL_ORTALAMASI > 2 THEN 20 ELSE GOL_ORTALAMASI * 10 END +
    CASE WHEN SUT_DONUSUM_ORANI > 20 THEN 10 ELSE ISNULL(SUT_DONUSUM_ORANI, 0) * 0.5 END +
    CASE WHEN XG_FARKI > 5 THEN 10 WHEN XG_FARKI < -5 THEN 0 ELSE (XG_FARKI + 5) END
    AS FORM_PUANI,

    -- =============================================
    -- DURAN TOP PUANI (20 puan max)
    -- Penalti atici (+15), Korner/Frikik (+5)
    -- =============================================
    CASE WHEN PENALTI_ATISI > 0 THEN 15 ELSE 0 END +
    CASE WHEN KORNER > 5 OR SERBEST_VURUS > 10 THEN 5 ELSE 0 END
    AS DURAN_TOP_PUANI,

    -- =============================================
    -- FIZIKSEL PUAN (10 puan max)
    -- Hava topu basarisi, ceza sahasi temasi
    -- =============================================
    CASE WHEN HAVA_TOPU_BASARI > 50 THEN 5 ELSE ISNULL(HAVA_TOPU_BASARI, 0) / 10 END +
    CASE WHEN CEZA_SAHASI_ORT > 5 THEN 5 ELSE ISNULL(CEZA_SAHASI_ORT, 0) END
    AS FIZIKSEL_PUANI,

    -- =============================================
    -- DAKIKA PUANI (10 puan max)
    -- 90 dk oynama ihtimali
    -- =============================================
    CASE WHEN ORTALAMA_DAKIKA >= 85 THEN 10
         WHEN ORTALAMA_DAKIKA >= 70 THEN 8
         WHEN ORTALAMA_DAKIKA >= 60 THEN 6
         WHEN ORTALAMA_DAKIKA >= 45 THEN 4
         ELSE 2 END
    AS DAKIKA_PUANI,

    -- =============================================
    -- TOPLAM SKOR (80 puan max - Taktik esleme sonra eklenecek)
    -- Form(40) + Duran Top(20) + Fiziksel(10) + Dakika(10)
    -- =============================================
    (
        CASE WHEN GOL_ORTALAMASI > 2 THEN 20 ELSE GOL_ORTALAMASI * 10 END +
        CASE WHEN SUT_DONUSUM_ORANI > 20 THEN 10 ELSE ISNULL(SUT_DONUSUM_ORANI, 0) * 0.5 END +
        CASE WHEN XG_FARKI > 5 THEN 10 WHEN XG_FARKI < -5 THEN 0 ELSE (XG_FARKI + 5) END +
        CASE WHEN PENALTI_ATISI > 0 THEN 15 ELSE 0 END +
        CASE WHEN KORNER > 5 OR SERBEST_VURUS > 10 THEN 5 ELSE 0 END +
        CASE WHEN HAVA_TOPU_BASARI > 50 THEN 5 ELSE ISNULL(HAVA_TOPU_BASARI, 0) / 10 END +
        CASE WHEN CEZA_SAHASI_ORT > 5 THEN 5 ELSE ISNULL(CEZA_SAHASI_ORT, 0) END +
        CASE WHEN ORTALAMA_DAKIKA >= 85 THEN 10
             WHEN ORTALAMA_DAKIKA >= 70 THEN 8
             WHEN ORTALAMA_DAKIKA >= 60 THEN 6
             WHEN ORTALAMA_DAKIKA >= 45 THEN 4
             ELSE 2 END
    ) AS TOPLAM_SKOR

FROM OyuncuStats
GO

-- =============================================
-- ORNEK SORGULAR
-- =============================================

-- En yuksek skorlu 10 oyuncu
-- SELECT TOP 10 OYUNCU_ADI, TAKIM_ADI, LIG_ADI, TOPLAM_GOL, ROUND(TOPLAM_SKOR, 1) AS SKOR
-- FROM TAHMIN.v_Oyuncu_Gol_Skoru
-- ORDER BY TOPLAM_SKOR DESC

-- Lig bazli en iyi oyuncular
-- SELECT LIG_ADI, OYUNCU_ADI, TAKIM_ADI, TOPLAM_GOL, ROUND(TOPLAM_SKOR, 1) AS SKOR
-- FROM (
--     SELECT *, ROW_NUMBER() OVER (PARTITION BY LIG_ADI ORDER BY TOPLAM_SKOR DESC) AS SIRA
--     FROM TAHMIN.v_Oyuncu_Gol_Skoru
-- ) t
-- WHERE SIRA <= 3
-- ORDER BY LIG_ADI, SIRA

-- Penalti aticilar
-- SELECT OYUNCU_ADI, TAKIM_ADI, PENALTI_ATISI, PENALTI_GOL
-- FROM TAHMIN.v_Oyuncu_Gol_Skoru
-- WHERE PENALTI_ATISI > 0
-- ORDER BY PENALTI_ATISI DESC
