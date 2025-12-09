-- =============================================
-- TAHMIN.v_Takim_Guc_Analizi
-- Takım saldırı ve savunma gücü analizi VIEW'i
-- Poisson tabanlı maç skor tahmini için temel veri
-- Oluşturma: 2025-12-09
-- =============================================

-- Eski view'i sil (varsa)
IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'TAHMIN.v_Takim_Guc_Analizi'))
    DROP VIEW TAHMIN.v_Takim_Guc_Analizi
GO

CREATE VIEW TAHMIN.v_Takim_Guc_Analizi AS
WITH MacSkorlari AS (
    -- Skor kolonunu parse ederek ev ve misafir gollerini çıkar
    SELECT
        f.FIKSTURID,
        f.LIG_ID,
        f.EVSAHIBI,
        f.MISAFIR,
        f.TARIH,
        -- Skor formatı: "2-1" veya "2–1" (farklı tire karakterleri olabilir)
        CAST(LEFT(f.SKOR, CHARINDEX('-', REPLACE(f.SKOR, '–', '-')) - 1) AS INT) AS EV_GOL,
        CAST(RIGHT(f.SKOR, LEN(f.SKOR) - CHARINDEX('-', REPLACE(f.SKOR, '–', '-'))) AS INT) AS MIS_GOL
    FROM FIKSTUR.FIKSTUR f
    WHERE f.DURUM = 1
      AND f.SKOR IS NOT NULL
      AND f.SKOR LIKE '%[0-9]%-%[0-9]%'
),
TakimMaclari AS (
    -- Her takımın tüm maçlarını (ev+deplasman) birleştir
    SELECT
        LIG_ID,
        EVSAHIBI AS TAKIM_ADI,
        TARIH,
        EV_GOL AS ATILAN_GOL,
        MIS_GOL AS YENILEN_GOL,
        'EV' AS MAC_TIPI,
        ROW_NUMBER() OVER (PARTITION BY LIG_ID, EVSAHIBI ORDER BY TARIH DESC) AS MAC_SIRASI
    FROM MacSkorlari

    UNION ALL

    SELECT
        LIG_ID,
        MISAFIR AS TAKIM_ADI,
        TARIH,
        MIS_GOL AS ATILAN_GOL,
        EV_GOL AS YENILEN_GOL,
        'DEPLASMAN' AS MAC_TIPI,
        ROW_NUMBER() OVER (PARTITION BY LIG_ID, MISAFIR ORDER BY TARIH DESC) AS MAC_SIRASI
    FROM MacSkorlari
),
LigOrtalamalari AS (
    -- Lig bazında ortalama gol hesapla
    SELECT
        LIG_ID,
        COUNT(*) AS TOPLAM_MAC,
        AVG(CAST(EV_GOL + MIS_GOL AS FLOAT)) AS MAC_BASINA_GOL,
        AVG(CAST(EV_GOL AS FLOAT)) AS EV_GOL_ORT,
        AVG(CAST(MIS_GOL AS FLOAT)) AS DEPLASMAN_GOL_ORT
    FROM MacSkorlari
    GROUP BY LIG_ID
),
TakimIstatistikleri AS (
    -- Takım bazında son 10 maç istatistikleri
    SELECT
        tm.LIG_ID,
        tm.TAKIM_ADI,
        COUNT(*) AS MAC_SAYISI,
        AVG(CAST(tm.ATILAN_GOL AS FLOAT)) AS ATILAN_GOL_ORT,
        AVG(CAST(tm.YENILEN_GOL AS FLOAT)) AS YENILEN_GOL_ORT,
        SUM(tm.ATILAN_GOL) AS TOPLAM_ATILAN,
        SUM(tm.YENILEN_GOL) AS TOPLAM_YENILEN,
        -- Son 5 maç formu
        SUM(CASE WHEN tm.MAC_SIRASI <= 5 THEN tm.ATILAN_GOL ELSE 0 END) AS SON5_ATILAN,
        SUM(CASE WHEN tm.MAC_SIRASI <= 5 THEN tm.YENILEN_GOL ELSE 0 END) AS SON5_YENILEN,
        -- Ev/Deplasman ayrımı
        AVG(CASE WHEN tm.MAC_TIPI = 'EV' THEN CAST(tm.ATILAN_GOL AS FLOAT) END) AS EV_ATILAN_ORT,
        AVG(CASE WHEN tm.MAC_TIPI = 'DEPLASMAN' THEN CAST(tm.ATILAN_GOL AS FLOAT) END) AS DEP_ATILAN_ORT,
        AVG(CASE WHEN tm.MAC_TIPI = 'EV' THEN CAST(tm.YENILEN_GOL AS FLOAT) END) AS EV_YENILEN_ORT,
        AVG(CASE WHEN tm.MAC_TIPI = 'DEPLASMAN' THEN CAST(tm.YENILEN_GOL AS FLOAT) END) AS DEP_YENILEN_ORT,
        MAX(tm.TARIH) AS SON_MAC_TARIHI
    FROM TakimMaclari tm
    WHERE tm.MAC_SIRASI <= 10  -- Son 10 maç
    GROUP BY tm.LIG_ID, tm.TAKIM_ADI
    HAVING COUNT(*) >= 3  -- En az 3 maç oynamış olmalı
)
SELECT
    ts.LIG_ID,
    l.LIG_ADI,
    ts.TAKIM_ADI,
    ts.MAC_SAYISI,
    ts.TOPLAM_ATILAN,
    ts.TOPLAM_YENILEN,
    ROUND(ts.ATILAN_GOL_ORT, 3) AS ATILAN_GOL_ORT,
    ROUND(ts.YENILEN_GOL_ORT, 3) AS YENILEN_GOL_ORT,

    -- =============================================
    -- SALDIRI GÜCÜ (Attack Strength)
    -- Takımın gol ortalaması / Lig ortalaması
    -- > 1 = Ortalama üstü hücum
    -- =============================================
    ROUND(ts.ATILAN_GOL_ORT / NULLIF(lo.MAC_BASINA_GOL / 2, 0), 3) AS SALDIRI_GUCU,

    -- =============================================
    -- SAVUNMA GÜCÜ (Defense Strength)
    -- Takımın yediği gol ort / Lig ort
    -- < 1 = İyi savunma (az gol yiyor)
    -- =============================================
    ROUND(ts.YENILEN_GOL_ORT / NULLIF(lo.MAC_BASINA_GOL / 2, 0), 3) AS SAVUNMA_GUCU,

    -- Ev sahibi güç faktörleri
    ROUND(ts.EV_ATILAN_ORT / NULLIF(lo.EV_GOL_ORT, 0), 3) AS EV_SALDIRI_GUCU,
    ROUND(ts.EV_YENILEN_ORT / NULLIF(lo.DEPLASMAN_GOL_ORT, 0), 3) AS EV_SAVUNMA_GUCU,

    -- Deplasman güç faktörleri
    ROUND(ts.DEP_ATILAN_ORT / NULLIF(lo.DEPLASMAN_GOL_ORT, 0), 3) AS DEP_SALDIRI_GUCU,
    ROUND(ts.DEP_YENILEN_ORT / NULLIF(lo.EV_GOL_ORT, 0), 3) AS DEP_SAVUNMA_GUCU,

    -- Son 5 maç form bilgisi
    ts.SON5_ATILAN,
    ts.SON5_YENILEN,
    ROUND(CAST(ts.SON5_ATILAN AS FLOAT) / 5, 2) AS SON5_ATILAN_ORT,
    ROUND(CAST(ts.SON5_YENILEN AS FLOAT) / 5, 2) AS SON5_YENILEN_ORT,

    -- Lig ortalamaları (referans için)
    ROUND(lo.MAC_BASINA_GOL, 3) AS LIG_GOL_ORT,
    ROUND(lo.EV_GOL_ORT, 3) AS LIG_EV_GOL_ORT,
    ROUND(lo.DEPLASMAN_GOL_ORT, 3) AS LIG_DEP_GOL_ORT,
    lo.TOPLAM_MAC AS LIG_MAC_SAYISI,

    ts.SON_MAC_TARIHI

FROM TakimIstatistikleri ts
JOIN TANIM.LIG l ON ts.LIG_ID = l.LIG_ID
JOIN LigOrtalamalari lo ON ts.LIG_ID = lo.LIG_ID
GO

-- =============================================
-- ÖRNEK SORGULAR
-- =============================================

-- Tüm takımların güç analizi
-- SELECT * FROM TAHMIN.v_Takim_Guc_Analizi ORDER BY SALDIRI_GUCU DESC

-- Premier League takımları
-- SELECT * FROM TAHMIN.v_Takim_Guc_Analizi WHERE LIG_ID = 6 ORDER BY SALDIRI_GUCU DESC

-- En güçlü hücum
-- SELECT TOP 10 TAKIM_ADI, LIG_ADI, SALDIRI_GUCU, ATILAN_GOL_ORT
-- FROM TAHMIN.v_Takim_Guc_Analizi ORDER BY SALDIRI_GUCU DESC

-- En iyi savunma (düşük savunma gücü = daha az gol yiyor)
-- SELECT TOP 10 TAKIM_ADI, LIG_ADI, SAVUNMA_GUCU, YENILEN_GOL_ORT
-- FROM TAHMIN.v_Takim_Guc_Analizi ORDER BY SAVUNMA_GUCU ASC
