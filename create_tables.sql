-- =====================================================
-- FBREF VERITABANI TABLO YAPISI
-- =====================================================

-- 1. TAKIM TABLOSU
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'TAKIM' AND schema_id = SCHEMA_ID('TANIM'))
BEGIN
    CREATE TABLE [TANIM].[TAKIM](
        [TAKIM_ID] [int] IDENTITY(1,1) NOT NULL,
        [TAKIM_ADI] [nvarchar](100) NULL,
        [URL] [nvarchar](500) NULL,
        [ULKE] [nvarchar](50) NULL,
        [KAYIT_TARIHI] [datetime] NULL,
        [DEGISIKLIK_TARIHI] [datetime] NULL,
        CONSTRAINT [PK_TAKIM] PRIMARY KEY CLUSTERED ([TAKIM_ID] ASC)
    )
END
GO

-- 2. OYUNCU TABLOSU
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'OYUNCU' AND schema_id = SCHEMA_ID('TANIM'))
BEGIN
    CREATE TABLE [TANIM].[OYUNCU](
        [OYUNCU_ID] [int] IDENTITY(1,1) NOT NULL,
        [OYUNCU_ADI] [nvarchar](150) NULL,
        [URL] [nvarchar](500) NULL,
        [ULKE] [nvarchar](50) NULL,
        [POZISYON] [nvarchar](20) NULL,
        [KAYIT_TARIHI] [datetime] NULL,
        [DEGISIKLIK_TARIHI] [datetime] NULL,
        CONSTRAINT [PK_OYUNCU] PRIMARY KEY CLUSTERED ([OYUNCU_ID] ASC)
    )
END
GO

-- 3. PERFORMANS TABLOSU (Guncellenmis - Tum istatistikler)
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'PERFORMANS' AND schema_id = SCHEMA_ID('FIKSTUR'))
BEGIN
    DROP TABLE [FIKSTUR].[PERFORMANS]
END
GO

CREATE TABLE [FIKSTUR].[PERFORMANS](
    [PERFORMANS_ID] [int] IDENTITY(1,1) NOT NULL,
    [FIKSTURID] [int] NULL,
    [OYUNCU_ID] [int] NULL,
    [TAKIM_ID] [int] NULL,

    -- Temel Bilgiler
    [FORMA_NO] [int] NULL,
    [POZISYON] [nvarchar](10) NULL,
    [YAS] [nvarchar](10) NULL,
    [SURE] [int] NULL,

    -- Performance (Performans)
    [GOL] [int] NULL,
    [ASIST] [int] NULL,
    [PENALTI_GOL] [int] NULL,
    [PENALTI_ATISI] [int] NULL,
    [SUT] [int] NULL,
    [ISABETLI_SUT] [int] NULL,
    [SARI_KART] [int] NULL,
    [KIRMIZI_KART] [int] NULL,

    -- Diger
    [TEMAS] [int] NULL,
    [TOP_KAPMA] [int] NULL,
    [MUDAHALE] [int] NULL,
    [BLOK] [int] NULL,

    -- Expected (Beklenen)
    [BEKLENEN_GOL] [decimal](5,2) NULL,
    [PENALTISIZ_XG] [decimal](5,2) NULL,
    [BEKLENEN_ASIST] [decimal](5,2) NULL,

    -- SCA (Sut Yaratan Aksiyonlar)
    [SUT_YARATAN_AKSIYON] [int] NULL,
    [GOL_YARATAN_AKSIYON] [int] NULL,

    -- Passes (Paslar)
    [BASARILI_PAS] [int] NULL,
    [PAS_DENEMESI] [int] NULL,
    [PAS_ISABET] [decimal](5,2) NULL,
    [ILERIYE_PAS] [int] NULL,

    -- Carries (Top Tasima)
    [TOP_TASIMA] [int] NULL,
    [ILERIYE_TASIMA] [int] NULL,

    -- Take-Ons (Carpisma/Dribling)
    [CARPISMA_GIRISIMI] [int] NULL,
    [BASARILI_CARPISMA] [int] NULL,

    -- Kayit Bilgileri
    [KAYIT_TARIHI] [datetime] NULL,
    [DEGISIKLIK_TARIHI] [datetime] NULL,

    CONSTRAINT [PK_PERFORMANS] PRIMARY KEY CLUSTERED ([PERFORMANS_ID] ASC)
)
GO

-- 4. KALECI PERFORMANS TABLOSU
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'KALECI_PERFORMANS' AND schema_id = SCHEMA_ID('FIKSTUR'))
BEGIN
    CREATE TABLE [FIKSTUR].[KALECI_PERFORMANS](
        [KALECI_PERFORMANS_ID] [int] IDENTITY(1,1) NOT NULL,
        [FIKSTURID] [int] NULL,
        [OYUNCU_ID] [int] NULL,
        [TAKIM_ID] [int] NULL,

        -- Temel Bilgiler
        [FORMA_NO] [int] NULL,
        [YAS] [nvarchar](10) NULL,
        [SURE] [int] NULL,

        -- Shot Stopping (Kurtaris)
        [KALEYE_SUT] [int] NULL,           -- SoTA
        [YENILEN_GOL] [int] NULL,          -- GA
        [KURTARIS] [int] NULL,             -- Saves
        [KURTARIS_YUZDESI] [decimal](5,2) NULL,  -- Save%
        [BEKLENEN_GOL_KURTARIS] [decimal](5,2) NULL,  -- PSxG

        -- Launched (Uzun Paslar)
        [UZUN_PAS_BASARILI] [int] NULL,    -- Cmp
        [UZUN_PAS_DENEME] [int] NULL,      -- Att
        [UZUN_PAS_YUZDE] [decimal](5,2) NULL,  -- Cmp%

        -- Passes (Paslar)
        [PAS_DENEME] [int] NULL,           -- Att (GK)
        [ELLE_PAS] [int] NULL,             -- Thr
        [UZUN_PAS_YUZDESI] [decimal](5,2) NULL,  -- Launch%
        [PAS_UZUNLUK] [decimal](5,2) NULL, -- AvgLen

        -- Goal Kicks (Kale Vuruslari)
        [KALE_VURUSU_DENEME] [int] NULL,   -- Att
        [KALE_VURUSU_UZUN_YUZDE] [decimal](5,2) NULL,  -- Launch%
        [KALE_VURUSU_UZUNLUK] [decimal](5,2) NULL,  -- AvgLen

        -- Crosses (Ortalar)
        [KARSILANAN_ORTA] [int] NULL,      -- Opp
        [DURDURULAN_ORTA] [int] NULL,      -- Stp
        [ORTA_DURDURMA_YUZDE] [decimal](5,2) NULL,  -- Stp%

        -- Sweeper (Cikis)
        [CEZA_DISI_AKSIYON] [int] NULL,    -- #OPA
        [ORT_CIKIS_MESAFE] [decimal](5,2) NULL,  -- AvgDist

        -- Kayit Bilgileri
        [KAYIT_TARIHI] [datetime] NULL,
        [DEGISIKLIK_TARIHI] [datetime] NULL,

        CONSTRAINT [PK_KALECI_PERFORMANS] PRIMARY KEY CLUSTERED ([KALECI_PERFORMANS_ID] ASC)
    )
END
GO

-- Foreign Key'ler
ALTER TABLE [FIKSTUR].[PERFORMANS] ADD CONSTRAINT [FK_PERFORMANS_FIKSTUR]
    FOREIGN KEY([FIKSTURID]) REFERENCES [FIKSTUR].[FIKSTUR] ([FIKSTURID])
GO

ALTER TABLE [FIKSTUR].[PERFORMANS] ADD CONSTRAINT [FK_PERFORMANS_OYUNCU]
    FOREIGN KEY([OYUNCU_ID]) REFERENCES [TANIM].[OYUNCU] ([OYUNCU_ID])
GO

ALTER TABLE [FIKSTUR].[PERFORMANS] ADD CONSTRAINT [FK_PERFORMANS_TAKIM]
    FOREIGN KEY([TAKIM_ID]) REFERENCES [TANIM].[TAKIM] ([TAKIM_ID])
GO

ALTER TABLE [FIKSTUR].[KALECI_PERFORMANS] ADD CONSTRAINT [FK_KALECI_PERFORMANS_FIKSTUR]
    FOREIGN KEY([FIKSTURID]) REFERENCES [FIKSTUR].[FIKSTUR] ([FIKSTURID])
GO

ALTER TABLE [FIKSTUR].[KALECI_PERFORMANS] ADD CONSTRAINT [FK_KALECI_PERFORMANS_OYUNCU]
    FOREIGN KEY([OYUNCU_ID]) REFERENCES [TANIM].[OYUNCU] ([OYUNCU_ID])
GO

ALTER TABLE [FIKSTUR].[KALECI_PERFORMANS] ADD CONSTRAINT [FK_KALECI_PERFORMANS_TAKIM]
    FOREIGN KEY([TAKIM_ID]) REFERENCES [TANIM].[TAKIM] ([TAKIM_ID])
GO

PRINT 'Tum tablolar olusturuldu!'
