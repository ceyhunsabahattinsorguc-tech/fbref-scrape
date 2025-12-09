-- =============================================
-- TAHMIN.v_Eleme_Senaryosu
-- Eleme/Kupa maçları için ek puan hesaplama VIEW'i
-- Olusturma: 2025-12-08
-- =============================================

-- Eski view'i sil (varsa)
IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'TAHMIN.v_Eleme_Senaryosu'))
    DROP VIEW TAHMIN.v_Eleme_Senaryosu
GO

CREATE VIEW TAHMIN.v_Eleme_Senaryosu AS
WITH KupaLigler AS (
    -- Hangi ligler kupa/eleme formatında
    SELECT LIG_ID, LIG_ADI, 'KUPA' AS LIG_TIPI
    FROM TANIM.LIG
    WHERE LIG_ADI IN (
        -- Yerel Kupalar
        'FA Cup', 'EFL Cup',
        'DFB-Pokal',
        'Copa del Rey',
        'Coppa Italia',
        'Coupe de France',
        'Türkiye Kupası',
        'KNVB Cup',
        'Taça de Portugal',
        'Beker van België',
        'Scottish Cup', 'Scottish League Cup'
    )

    UNION ALL

    -- Avrupa Kupaları (Grup + Eleme)
    SELECT LIG_ID, LIG_ADI, 'AVRUPA' AS LIG_TIPI
    FROM TANIM.LIG
    WHERE LIG_ADI IN (
        'Champions League',
        'Europa League',
        'Europa Conference League'
    )
),
MacTurleri AS (
    -- Maç turlarını belirle
    SELECT
        f.FIKSTURID,
        f.LIG_ID,
        l.LIG_ADI,
        f.EVSAHIBI,
        f.MISAFIR,
        f.TARIH,
        COALESCE(kl.LIG_TIPI, 'LIG') AS LIG_TIPI,

        -- Tur belirleme
        CASE
            -- Kupa maçları için round bilgisinden çıkar
            WHEN l.LIG_ADI LIKE '%Cup%' OR l.LIG_ADI LIKE '%Pokal%' OR l.LIG_ADI LIKE '%Copa%'
                 OR l.LIG_ADI LIKE '%Coppa%' OR l.LIG_ADI LIKE '%Coupe%' OR l.LIG_ADI LIKE '%Kupası%'
                 OR l.LIG_ADI LIKE '%Beker%' OR l.LIG_ADI LIKE '%Taça%' THEN 'ELEME'
            -- Avrupa kupaları için ayları kontrol et
            WHEN l.LIG_ADI IN ('Champions League', 'Europa League', 'Europa Conference League')
                 AND MONTH(f.TARIH) >= 2 THEN 'ELEME'
            WHEN l.LIG_ADI IN ('Champions League', 'Europa League', 'Europa Conference League')
                 AND MONTH(f.TARIH) < 2 THEN 'GRUP'
            ELSE 'LIG'
        END AS MAC_TURU,

        -- Final/Yarı Final kontrolü (tarih ve sıraya göre)
        CASE
            WHEN l.LIG_ADI LIKE '%Cup%' AND MONTH(f.TARIH) IN (4, 5) THEN 'FINAL_YAKIN'
            WHEN l.LIG_ADI = 'Champions League' AND MONTH(f.TARIH) IN (5, 6) THEN 'FINAL_YAKIN'
            ELSE 'NORMAL'
        END AS ONEM_SEVIYESI

    FROM FIKSTUR.FIKSTUR f
    JOIN TANIM.LIG l ON f.LIG_ID = l.LIG_ID
    LEFT JOIN KupaLigler kl ON f.LIG_ID = kl.LIG_ID
    WHERE f.DURUM = 0  -- Oynanmamış maçlar
)
SELECT
    mt.*,

    -- =============================================
    -- ELEME PUANI HESAPLAMA (Max 10 puan)
    -- =============================================
    CASE
        -- Final/Yarı Final maçları (10 puan)
        WHEN mt.ONEM_SEVIYESI = 'FINAL_YAKIN' THEN 10

        -- Normal eleme maçları (7 puan)
        WHEN mt.MAC_TURU = 'ELEME' THEN 7

        -- Avrupa kupası grup aşaması (5 puan)
        WHEN mt.MAC_TURU = 'GRUP' AND mt.LIG_TIPI = 'AVRUPA' THEN 5

        -- Yerel kupa erken turlar (5 puan)
        WHEN mt.LIG_TIPI = 'KUPA' THEN 5

        -- Normal lig maçı (0 puan)
        ELSE 0
    END AS ELEME_PUANI,

    -- =============================================
    -- MOTIVASYON FAKTÖRÜ
    -- Kupa maçlarında takımların motivasyonu artar
    -- =============================================
    CASE
        WHEN mt.MAC_TURU IN ('ELEME', 'GRUP') OR mt.LIG_TIPI IN ('KUPA', 'AVRUPA')
        THEN 1.1  -- %10 bonus
        ELSE 1.0
    END AS MOTIVASYON_CARPANI

FROM MacTurleri mt
GO

-- =============================================
-- NİHAİ GOL TAHMİN FONKSİYONU (ELİME DAHİL)
-- =============================================

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'TAHMIN.fn_Golcu_Mac_Skoru_Eleme') AND type = N'IF')
    DROP FUNCTION TAHMIN.fn_Golcu_Mac_Skoru_Eleme
GO

CREATE FUNCTION TAHMIN.fn_Golcu_Mac_Skoru_Eleme
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
            l.LIG_ADI,
            -- Eleme puanını al
            ISNULL(es.ELEME_PUANI, 0) AS ELEME_PUANI,
            ISNULL(es.MOTIVASYON_CARPANI, 1.0) AS MOTIVASYON_CARPANI
        FROM FIKSTUR.FIKSTUR f
        JOIN TANIM.LIG l ON f.LIG_ID = l.LIG_ID
        LEFT JOIN TAHMIN.v_Eleme_Senaryosu es ON f.FIKSTURID = es.FIKSTURID
        WHERE f.FIKSTURID = @FiksturId
    ),
    EvSahibiOyuncular AS (
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
            mb.ELEME_PUANI,
            mb.MOTIVASYON_CARPANI,
            -- Rakip savunma faktörünü al
            ISNULL(sf.GOLCU_AVANTAJ_PUANI, 50) * 0.15 AS SAVUNMA_FAKTORU
        FROM TAHMIN.v_Oyuncu_Gol_Skoru_v2 gs
        CROSS JOIN MacBilgi mb
        LEFT JOIN TAHMIN.v_Takim_Savunma_Formu sf
            ON sf.TAKIM_ADI = mb.MISAFIR AND sf.LIG_ID = mb.LIG_ID
        WHERE gs.TAKIM_ADI = mb.EVSAHIBI
    ),
    MisafirOyuncular AS (
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
            mb.ELEME_PUANI,
            mb.MOTIVASYON_CARPANI,
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
        ELEME_PUANI,
        MOTIVASYON_CARPANI,
        -- =============================================
        -- NİHAİ SKOR HESAPLA (Max 100 puan)
        -- Temel (75) + Savunma (15) + Eleme (10)
        -- =============================================
        ROUND(
            (TEMEL_SKOR + SAVUNMA_FAKTORU + ELEME_PUANI) * MOTIVASYON_CARPANI,
            2
        ) AS NIHAI_SKOR
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
        ELEME_PUANI,
        MOTIVASYON_CARPANI,
        ROUND(
            (TEMEL_SKOR + SAVUNMA_FAKTORU + ELEME_PUANI) * MOTIVASYON_CARPANI,
            2
        ) AS NIHAI_SKOR
    FROM MisafirOyuncular
)
GO

-- =============================================
-- ÖRNEK SORGULAR
-- =============================================

-- Eleme maçlarını listele
-- SELECT FIKSTURID, LIG_ADI, EVSAHIBI, MISAFIR, TARIH, MAC_TURU, ELEME_PUANI
-- FROM TAHMIN.v_Eleme_Senaryosu
-- WHERE ELEME_PUANI > 0
-- ORDER BY TARIH

-- Belirli bir maç için golcü tahminleri (eleme dahil)
-- SELECT TOP 10 *
-- FROM TAHMIN.fn_Golcu_Mac_Skoru_Eleme(12345)
-- ORDER BY NIHAI_SKOR DESC
