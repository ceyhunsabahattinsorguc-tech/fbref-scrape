-- =============================================
-- TAHMIN.v_Takim_Motivasyon
-- Şampiyonluk/Düşme yarışı motivasyon bonusu
-- Olusturma: 2025-12-09
-- =============================================

IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'TAHMIN.v_Takim_Motivasyon'))
    DROP VIEW TAHMIN.v_Takim_Motivasyon
GO

CREATE VIEW TAHMIN.v_Takim_Motivasyon AS
WITH LigBilgi AS (
    -- Her lig için toplam takım sayısı
    SELECT
        LIG_ID,
        COUNT(*) AS TAKIM_SAYISI,
        MAX(PUAN) AS LIDER_PUAN
    FROM FIKSTUR.PUAN_DURUMU
    GROUP BY LIG_ID
),
TakimDurumu AS (
    SELECT
        pd.LIG_ID,
        pd.TAKIM_ADI,
        pd.SIRA,
        pd.PUAN,
        pd.OYNANAN,
        pd.GALIBIYET,
        pd.BERABERLIK,
        pd.MAGLUBIYET,
        pd.ATILAN_GOL,
        pd.YENILEN_GOL,
        pd.AVERAJ,
        lb.TAKIM_SAYISI,
        lb.LIDER_PUAN,

        -- Lider ile puan farkı
        lb.LIDER_PUAN - pd.PUAN AS LIDER_FARK,

        -- Düşme hattına uzaklık (son 3 takım tehlike bölgesi)
        pd.SIRA - (lb.TAKIM_SAYISI - 2) AS DUSME_MESAFE,

        -- Pozisyon kategorisi
        CASE
            WHEN pd.SIRA <= 3 THEN 'SAMPIYONLUK'          -- İlk 3
            WHEN pd.SIRA <= 6 THEN 'AVRUPA'               -- 4-6
            WHEN pd.SIRA >= lb.TAKIM_SAYISI - 2 THEN 'DUSME'  -- Son 3
            ELSE 'ORTA'
        END AS POZISYON_KATEGORISI

    FROM FIKSTUR.PUAN_DURUMU pd
    JOIN LigBilgi lb ON pd.LIG_ID = lb.LIG_ID
)
SELECT
    td.*,
    l.LIG_ADI,

    -- =============================================
    -- MOTİVASYON PUANI HESAPLAMA (Max 15 puan)
    -- =============================================
    CASE
        -- Şampiyonluk yarışı (lider ile 6 puan fark)
        WHEN td.POZISYON_KATEGORISI = 'SAMPIYONLUK' AND td.LIDER_FARK <= 6 THEN 15

        -- Şampiyonluk yarışı (lider ile 10 puan fark)
        WHEN td.POZISYON_KATEGORISI = 'SAMPIYONLUK' AND td.LIDER_FARK <= 10 THEN 12

        -- Avrupa kupası yarışı
        WHEN td.POZISYON_KATEGORISI = 'AVRUPA' THEN 10

        -- Düşme tehlikesi (son 3)
        WHEN td.POZISYON_KATEGORISI = 'DUSME' THEN 15

        -- Düşme hattına yakın (4-5 puan)
        WHEN td.DUSME_MESAFE <= 3 AND td.DUSME_MESAFE > 0 THEN 12

        -- Normal durumda
        ELSE 5
    END AS MOTIVASYON_PUANI,

    -- =============================================
    -- MOTİVASYON ÇARPANI
    -- =============================================
    CASE
        WHEN td.POZISYON_KATEGORISI IN ('SAMPIYONLUK', 'DUSME') THEN 1.15
        WHEN td.POZISYON_KATEGORISI = 'AVRUPA' THEN 1.10
        WHEN td.DUSME_MESAFE <= 3 AND td.DUSME_MESAFE > 0 THEN 1.10
        ELSE 1.0
    END AS MOTIVASYON_CARPANI,

    -- Açıklama
    CASE
        WHEN td.POZISYON_KATEGORISI = 'SAMPIYONLUK' AND td.LIDER_FARK <= 6
            THEN 'Şampiyonluk yarışı (kritik)'
        WHEN td.POZISYON_KATEGORISI = 'SAMPIYONLUK' AND td.LIDER_FARK <= 10
            THEN 'Şampiyonluk yarışı'
        WHEN td.POZISYON_KATEGORISI = 'AVRUPA'
            THEN 'Avrupa kupası yarışı'
        WHEN td.POZISYON_KATEGORISI = 'DUSME'
            THEN 'Düşme tehlikesi!'
        WHEN td.DUSME_MESAFE <= 3 AND td.DUSME_MESAFE > 0
            THEN 'Düşme hattına yakın'
        ELSE 'Normal'
    END AS MOTIVASYON_ACIKLAMA

FROM TakimDurumu td
JOIN TANIM.LIG l ON td.LIG_ID = l.LIG_ID
GO

-- =============================================
-- ÖRNEK SORGU
-- =============================================

-- Motivasyonu yüksek takımlar
-- SELECT LIG_ADI, TAKIM_ADI, SIRA, PUAN, MOTIVASYON_PUANI, MOTIVASYON_ACIKLAMA
-- FROM TAHMIN.v_Takim_Motivasyon
-- WHERE MOTIVASYON_PUANI >= 10
-- ORDER BY MOTIVASYON_PUANI DESC, LIG_ADI, SIRA
