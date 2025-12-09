-- =============================================
-- TAHMIN.v_Oyuncu_Gol_Skoru_v2
-- Oyuncu gol atma ihtimali skoru VIEW'i - V2
-- Olusturma: 2025-12-08
-- Guncelleme: Kaleci/Savunma faktoru eklendi
-- =============================================

-- Eski view'i sil (varsa)
IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'TAHMIN.v_Oyuncu_Gol_Skoru_v2'))
    DROP VIEW TAHMIN.v_Oyuncu_Gol_Skoru_v2
GO

-- TAHMIN schema yoksa olustur
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'TAHMIN')
    EXEC('CREATE SCHEMA TAHMIN')
GO

CREATE VIEW TAHMIN.v_Oyuncu_Gol_Skoru_v2 AS
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
    os.*,

    -- =============================================
    -- FORM PUANI (35 puan max) - V2 Guncelleme
    -- Gol ortalamasi, sut donusum, xG performansi
    -- =============================================
    CASE WHEN os.GOL_ORTALAMASI > 2 THEN 17.5 ELSE os.GOL_ORTALAMASI * 8.75 END +
    CASE WHEN os.SUT_DONUSUM_ORANI > 20 THEN 8.75 ELSE ISNULL(os.SUT_DONUSUM_ORANI, 0) * 0.4375 END +
    CASE WHEN os.XG_FARKI > 5 THEN 8.75 WHEN os.XG_FARKI < -5 THEN 0 ELSE (os.XG_FARKI + 5) * 0.875 END
    AS FORM_PUANI,

    -- =============================================
    -- DURAN TOP PUANI (20 puan max)
    -- Penalti atici (+15), Korner/Frikik (+5)
    -- =============================================
    CASE WHEN os.PENALTI_ATISI > 0 THEN 15 ELSE 0 END +
    CASE WHEN os.KORNER > 5 OR os.SERBEST_VURUS > 10 THEN 5 ELSE 0 END
    AS DURAN_TOP_PUANI,

    -- =============================================
    -- FIZIKSEL PUAN (10 puan max)
    -- Hava topu basarisi, ceza sahasi temasi
    -- =============================================
    CASE WHEN os.HAVA_TOPU_BASARI > 50 THEN 5 ELSE ISNULL(os.HAVA_TOPU_BASARI, 0) / 10 END +
    CASE WHEN os.CEZA_SAHASI_ORT > 5 THEN 5 ELSE ISNULL(os.CEZA_SAHASI_ORT, 0) END
    AS FIZIKSEL_PUANI,

    -- =============================================
    -- DAKIKA PUANI (10 puan max)
    -- 90 dk oynama ihtimali
    -- =============================================
    CASE WHEN os.ORTALAMA_DAKIKA >= 85 THEN 10
         WHEN os.ORTALAMA_DAKIKA >= 70 THEN 8
         WHEN os.ORTALAMA_DAKIKA >= 60 THEN 6
         WHEN os.ORTALAMA_DAKIKA >= 45 THEN 4
         ELSE 2 END
    AS DAKIKA_PUANI,

    -- =============================================
    -- TEMEL SKOR (75 puan max)
    -- Form(35) + Duran Top(20) + Fiziksel(10) + Dakika(10)
    -- Kaleci/Savunma (15) ve Eleme (10) faktorleri
    -- mac bazli hesaplanacak
    -- =============================================
    (
        CASE WHEN os.GOL_ORTALAMASI > 2 THEN 17.5 ELSE os.GOL_ORTALAMASI * 8.75 END +
        CASE WHEN os.SUT_DONUSUM_ORANI > 20 THEN 8.75 ELSE ISNULL(os.SUT_DONUSUM_ORANI, 0) * 0.4375 END +
        CASE WHEN os.XG_FARKI > 5 THEN 8.75 WHEN os.XG_FARKI < -5 THEN 0 ELSE (os.XG_FARKI + 5) * 0.875 END +
        CASE WHEN os.PENALTI_ATISI > 0 THEN 15 ELSE 0 END +
        CASE WHEN os.KORNER > 5 OR os.SERBEST_VURUS > 10 THEN 5 ELSE 0 END +
        CASE WHEN os.HAVA_TOPU_BASARI > 50 THEN 5 ELSE ISNULL(os.HAVA_TOPU_BASARI, 0) / 10 END +
        CASE WHEN os.CEZA_SAHASI_ORT > 5 THEN 5 ELSE ISNULL(os.CEZA_SAHASI_ORT, 0) END +
        CASE WHEN os.ORTALAMA_DAKIKA >= 85 THEN 10
             WHEN os.ORTALAMA_DAKIKA >= 70 THEN 8
             WHEN os.ORTALAMA_DAKIKA >= 60 THEN 6
             WHEN os.ORTALAMA_DAKIKA >= 45 THEN 4
             ELSE 2 END
    ) AS TEMEL_SKOR

FROM OyuncuStats os
GO

-- =============================================
-- MAC BAZLI TAHMİN FONKSIYONU
-- Bu fonksiyon belirli bir mac icin tum faktorleri
-- birlestirerek nihai skor hesaplar
-- =============================================

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'TAHMIN.fn_Golcu_Mac_Skoru') AND type = N'IF')
    DROP FUNCTION TAHMIN.fn_Golcu_Mac_Skoru
GO

CREATE FUNCTION TAHMIN.fn_Golcu_Mac_Skoru
(
    @FiksturId INT
)
RETURNS TABLE
AS
RETURN
(
    WITH MacBilgi AS (
        SELECT
            f.FIKSTURID,
            f.EVSAHIBI,
            f.MISAFIR,
            f.LIG_ID,
            l.LIG_ADI
        FROM FIKSTUR.FIKSTUR f
        JOIN TANIM.LIG l ON f.LIG_ID = l.LIG_ID
        WHERE f.FIKSTURID = @FiksturId
    ),
    EvSahibiOyuncular AS (
        -- Ev sahibi takimin oyunculari
        SELECT
            gs.OYUNCU_ID,
            gs.OYUNCU_ADI,
            gs.TAKIM_ADI,
            gs.TEMEL_SKOR,
            gs.FORM_PUANI,
            gs.DURAN_TOP_PUANI,
            gs.FIZIKSEL_PUANI,
            gs.DAKIKA_PUANI,
            gs.GOL_ORTALAMASI,
            gs.MAC_SAYISI,
            mb.FIKSTURID,
            mb.LIG_ADI,
            'EV' AS TARAF,
            mb.MISAFIR AS RAKIP_TAKIM,
            -- Rakip savunma faktorunu al (misafir takim)
            ISNULL(sf.GOLCU_AVANTAJ_PUANI, 50) * 0.15 AS SAVUNMA_FAKTORU
        FROM TAHMIN.v_Oyuncu_Gol_Skoru_v2 gs
        CROSS JOIN MacBilgi mb
        LEFT JOIN TAHMIN.v_Takim_Savunma_Formu sf
            ON sf.TAKIM_ADI = mb.MISAFIR AND sf.LIG_ID = mb.LIG_ID
        WHERE gs.TAKIM_ADI = mb.EVSAHIBI
    ),
    MisafirOyuncular AS (
        -- Misafir takimin oyunculari
        SELECT
            gs.OYUNCU_ID,
            gs.OYUNCU_ADI,
            gs.TAKIM_ADI,
            gs.TEMEL_SKOR,
            gs.FORM_PUANI,
            gs.DURAN_TOP_PUANI,
            gs.FIZIKSEL_PUANI,
            gs.DAKIKA_PUANI,
            gs.GOL_ORTALAMASI,
            gs.MAC_SAYISI,
            mb.FIKSTURID,
            mb.LIG_ADI,
            'MISAFIR' AS TARAF,
            mb.EVSAHIBI AS RAKIP_TAKIM,
            -- Rakip savunma faktorunu al (ev sahibi takim)
            ISNULL(sf.GOLCU_AVANTAJ_PUANI, 50) * 0.15 AS SAVUNMA_FAKTORU
        FROM TAHMIN.v_Oyuncu_Gol_Skoru_v2 gs
        CROSS JOIN MacBilgi mb
        LEFT JOIN TAHMIN.v_Takim_Savunma_Formu sf
            ON sf.TAKIM_ADI = mb.EVSAHIBI AND sf.LIG_ID = mb.LIG_ID
        WHERE gs.TAKIM_ADI = mb.MISAFIR
    )
    SELECT
        FIKSTURID,
        OYUNCU_ID,
        OYUNCU_ADI,
        TAKIM_ADI,
        RAKIP_TAKIM,
        TARAF,
        LIG_ADI,
        MAC_SAYISI,
        GOL_ORTALAMASI,
        FORM_PUANI,
        DURAN_TOP_PUANI,
        FIZIKSEL_PUANI,
        DAKIKA_PUANI,
        TEMEL_SKOR,
        SAVUNMA_FAKTORU,
        -- Nihai Skor: Temel (75) + Savunma Faktoru (15) = 90 max
        -- Eleme faktoru (10) mac bazli eklenebilir
        ROUND(TEMEL_SKOR + SAVUNMA_FAKTORU, 2) AS NIHAI_SKOR
    FROM EvSahibiOyuncular

    UNION ALL

    SELECT
        FIKSTURID,
        OYUNCU_ID,
        OYUNCU_ADI,
        TAKIM_ADI,
        RAKIP_TAKIM,
        TARAF,
        LIG_ADI,
        MAC_SAYISI,
        GOL_ORTALAMASI,
        FORM_PUANI,
        DURAN_TOP_PUANI,
        FIZIKSEL_PUANI,
        DAKIKA_PUANI,
        TEMEL_SKOR,
        SAVUNMA_FAKTORU,
        ROUND(TEMEL_SKOR + SAVUNMA_FAKTORU, 2) AS NIHAI_SKOR
    FROM MisafirOyuncular
)
GO

-- =============================================
-- ORNEK KULLANIM
-- =============================================

-- Belirli bir mac icin golcu tahminleri
-- SELECT TOP 10 *
-- FROM TAHMIN.fn_Golcu_Mac_Skoru(12345)  -- FiksturId
-- ORDER BY NIHAI_SKOR DESC

-- Tum oyuncularin temel skorlari
-- SELECT TOP 20 OYUNCU_ADI, TAKIM_ADI, LIG_ADI, ROUND(TEMEL_SKOR, 1) AS SKOR
-- FROM TAHMIN.v_Oyuncu_Gol_Skoru_v2
-- ORDER BY TEMEL_SKOR DESC
