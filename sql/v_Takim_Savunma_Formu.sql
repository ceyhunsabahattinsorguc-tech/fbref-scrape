-- =============================================
-- TAHMIN.v_Takim_Savunma_Formu
-- Takım savunma ve kaleci form analizi VIEW'i
-- Olusturma: 2025-12-08
-- Not: Kaleci spesifik verisi yok, takım savunma metrikleri kullanılıyor
-- =============================================

-- Eski view'i sil (varsa)
IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'TAHMIN.v_Takim_Savunma_Formu'))
    DROP VIEW TAHMIN.v_Takim_Savunma_Formu
GO

CREATE VIEW TAHMIN.v_Takim_Savunma_Formu AS
WITH TakimMaclar AS (
    -- Her takımın her maçı için yenilen gol sayısı
    SELECT
        f.FIKSTURID,
        f.TARIH,
        f.LIG_ID,
        t_ev.TAKIM_ID AS EV_TAKIM_ID,
        t_ev.TAKIM_ADI AS EV_TAKIM,
        t_mis.TAKIM_ID AS MISAFIR_TAKIM_ID,
        t_mis.TAKIM_ADI AS MISAFIR_TAKIM,

        -- Skor parse
        CASE
            WHEN CHARINDEX('-', f.SKOR) > 0
            THEN TRY_CAST(LEFT(f.SKOR, CHARINDEX('-', f.SKOR) - 1) AS INT)
        END AS EV_GOL,
        CASE
            WHEN CHARINDEX('-', f.SKOR) > 0
            THEN TRY_CAST(RIGHT(f.SKOR, LEN(f.SKOR) - CHARINDEX('-', f.SKOR)) AS INT)
        END AS MISAFIR_GOL

    FROM FIKSTUR.FIKSTUR f
    JOIN TANIM.TAKIM t_ev ON f.EVSAHIBI = t_ev.TAKIM_ADI
    JOIN TANIM.TAKIM t_mis ON f.MISAFIR = t_mis.TAKIM_ADI
    WHERE f.DURUM = 1  -- Oynanmış maçlar
    AND f.SKOR IS NOT NULL
),
TakimYenilenGol AS (
    -- Ev sahibi olarak yenilen goller
    SELECT
        EV_TAKIM_ID AS TAKIM_ID,
        EV_TAKIM AS TAKIM_ADI,
        LIG_ID,
        TARIH,
        MISAFIR_GOL AS YENILEN_GOL,
        EV_GOL AS ATILAN_GOL,
        1 AS IC_SAHA
    FROM TakimMaclar
    WHERE EV_GOL IS NOT NULL

    UNION ALL

    -- Misafir olarak yenilen goller
    SELECT
        MISAFIR_TAKIM_ID AS TAKIM_ID,
        MISAFIR_TAKIM AS TAKIM_ADI,
        LIG_ID,
        TARIH,
        EV_GOL AS YENILEN_GOL,
        MISAFIR_GOL AS ATILAN_GOL,
        0 AS IC_SAHA
    FROM TakimMaclar
    WHERE MISAFIR_GOL IS NOT NULL
),
TakimSavunmaStats AS (
    SELECT
        TAKIM_ID,
        TAKIM_ADI,
        LIG_ID,

        -- Toplam maç
        COUNT(*) AS MAC_SAYISI,

        -- Yenilen gol istatistikleri
        SUM(YENILEN_GOL) AS TOPLAM_YENILEN_GOL,
        CAST(SUM(YENILEN_GOL) AS FLOAT) / NULLIF(COUNT(*), 0) AS YENILEN_GOL_ORT,

        -- Clean sheet (gol yemeden)
        SUM(CASE WHEN YENILEN_GOL = 0 THEN 1 ELSE 0 END) AS CLEAN_SHEET,
        CAST(SUM(CASE WHEN YENILEN_GOL = 0 THEN 1 ELSE 0 END) AS FLOAT) / NULLIF(COUNT(*), 0) * 100 AS CLEAN_SHEET_YUZDESI,

        -- İç/Dış saha analizi
        AVG(CASE WHEN IC_SAHA = 1 THEN CAST(YENILEN_GOL AS FLOAT) END) AS IC_SAHA_YENILEN_ORT,
        AVG(CASE WHEN IC_SAHA = 0 THEN CAST(YENILEN_GOL AS FLOAT) END) AS DIS_SAHA_YENILEN_ORT,

        -- Son maç
        MAX(TARIH) AS SON_MAC_TARIHI

    FROM TakimYenilenGol
    GROUP BY TAKIM_ID, TAKIM_ADI, LIG_ID
    HAVING COUNT(*) >= 3
),
Son5Mac AS (
    -- Son 5 maçtaki yenilen gol ortalaması
    SELECT
        TAKIM_ID,
        AVG(CAST(YENILEN_GOL AS FLOAT)) AS SON5_YENILEN_ORT,
        SUM(CASE WHEN YENILEN_GOL = 0 THEN 1 ELSE 0 END) AS SON5_CLEAN_SHEET
    FROM (
        SELECT
            TAKIM_ID,
            YENILEN_GOL,
            ROW_NUMBER() OVER (PARTITION BY TAKIM_ID ORDER BY TARIH DESC) AS SIRA
        FROM TakimYenilenGol
    ) sub
    WHERE SIRA <= 5
    GROUP BY TAKIM_ID
)
SELECT
    ts.*,
    l.LIG_ADI,

    -- Son 5 maç verileri
    s5.SON5_YENILEN_ORT,
    s5.SON5_CLEAN_SHEET,

    -- Form trendi (son 5 vs toplam)
    CASE
        WHEN s5.SON5_YENILEN_ORT < ts.YENILEN_GOL_ORT THEN 'İYİLEŞİYOR'
        WHEN s5.SON5_YENILEN_ORT > ts.YENILEN_GOL_ORT THEN 'KÖTÜLEŞIYOR'
        ELSE 'SABİT'
    END AS SAVUNMA_FORM_TRENDI,

    -- =============================================
    -- SAVUNMA SKOR (0-100)
    -- Düşük = Zayıf savunma (golcü için iyi)
    -- Yüksek = Güçlü savunma (golcü için kötü)
    -- =============================================
    CASE
        -- Clean sheet yüzdesi (max 40 puan)
        WHEN ts.CLEAN_SHEET_YUZDESI >= 40 THEN 40
        WHEN ts.CLEAN_SHEET_YUZDESI >= 30 THEN 30
        WHEN ts.CLEAN_SHEET_YUZDESI >= 20 THEN 20
        WHEN ts.CLEAN_SHEET_YUZDESI >= 10 THEN 10
        ELSE 0
    END +
    -- Yenilen gol ortalaması (max 40 puan - ters orantı)
    CASE
        WHEN ts.YENILEN_GOL_ORT <= 0.5 THEN 40
        WHEN ts.YENILEN_GOL_ORT <= 1.0 THEN 30
        WHEN ts.YENILEN_GOL_ORT <= 1.5 THEN 20
        WHEN ts.YENILEN_GOL_ORT <= 2.0 THEN 10
        ELSE 0
    END +
    -- Son 5 maç formu (max 20 puan)
    CASE
        WHEN s5.SON5_YENILEN_ORT <= 0.5 THEN 20
        WHEN s5.SON5_YENILEN_ORT <= 1.0 THEN 15
        WHEN s5.SON5_YENILEN_ORT <= 1.5 THEN 10
        WHEN s5.SON5_YENILEN_ORT <= 2.0 THEN 5
        ELSE 0
    END AS SAVUNMA_SKORU,

    -- Golcü için rakip savunma puanı (ters çevrilmiş)
    -- 100 - SAVUNMA_SKORU = Ne kadar düşükse golcü için o kadar iyi
    100 - (
        CASE
            WHEN ts.CLEAN_SHEET_YUZDESI >= 40 THEN 40
            WHEN ts.CLEAN_SHEET_YUZDESI >= 30 THEN 30
            WHEN ts.CLEAN_SHEET_YUZDESI >= 20 THEN 20
            WHEN ts.CLEAN_SHEET_YUZDESI >= 10 THEN 10
            ELSE 0
        END +
        CASE
            WHEN ts.YENILEN_GOL_ORT <= 0.5 THEN 40
            WHEN ts.YENILEN_GOL_ORT <= 1.0 THEN 30
            WHEN ts.YENILEN_GOL_ORT <= 1.5 THEN 20
            WHEN ts.YENILEN_GOL_ORT <= 2.0 THEN 10
            ELSE 0
        END +
        CASE
            WHEN s5.SON5_YENILEN_ORT <= 0.5 THEN 20
            WHEN s5.SON5_YENILEN_ORT <= 1.0 THEN 15
            WHEN s5.SON5_YENILEN_ORT <= 1.5 THEN 10
            WHEN s5.SON5_YENILEN_ORT <= 2.0 THEN 5
            ELSE 0
        END
    ) AS GOLCU_AVANTAJ_PUANI

FROM TakimSavunmaStats ts
JOIN TANIM.LIG l ON ts.LIG_ID = l.LIG_ID
LEFT JOIN Son5Mac s5 ON ts.TAKIM_ID = s5.TAKIM_ID
GO

-- =============================================
-- ORNEK SORGULAR
-- =============================================

-- En zayıf savunmalar (golcüler için en iyi rakipler)
-- SELECT TOP 10 TAKIM_ADI, LIG_ADI, YENILEN_GOL_ORT, CLEAN_SHEET_YUZDESI, GOLCU_AVANTAJ_PUANI
-- FROM TAHMIN.v_Takim_Savunma_Formu
-- ORDER BY GOLCU_AVANTAJ_PUANI DESC

-- En güçlü savunmalar
-- SELECT TOP 10 TAKIM_ADI, LIG_ADI, YENILEN_GOL_ORT, CLEAN_SHEET_YUZDESI, SAVUNMA_SKORU
-- FROM TAHMIN.v_Takim_Savunma_Formu
-- ORDER BY SAVUNMA_SKORU DESC

-- Lig bazlı savunma sıralaması
-- SELECT LIG_ADI, TAKIM_ADI, SAVUNMA_SKORU, SAVUNMA_FORM_TRENDI
-- FROM TAHMIN.v_Takim_Savunma_Formu
-- WHERE LIG_ADI = 'Premier League'
-- ORDER BY SAVUNMA_SKORU DESC
