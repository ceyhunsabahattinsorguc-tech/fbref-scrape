-- =============================================
-- BAHIS Schema ve Tablolar
-- Bahis oranları veri tabloları
-- Oluşturma: 2025-12-08
-- =============================================

-- BAHIS schema yoksa oluştur
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'BAHIS')
    EXEC('CREATE SCHEMA BAHIS')
GO

-- =============================================
-- 1. MAC_ORANLARI - Maç bahis oranları (1X2, O/U)
-- =============================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'MAC_ORANLARI' AND schema_id = SCHEMA_ID('BAHIS'))
CREATE TABLE BAHIS.MAC_ORANLARI (
    ORAN_ID INT IDENTITY(1,1) PRIMARY KEY,
    FIKSTURID INT NULL,                    -- FK to FIKSTUR.FIKSTUR
    LIG_ID INT NOT NULL,
    EV_SAHIBI NVARCHAR(100) NOT NULL,
    MISAFIR NVARCHAR(100) NOT NULL,
    MAC_TARIHI DATE NOT NULL,

    -- 1X2 Oranları
    ORAN_1 DECIMAL(5,2) NULL,              -- Ev sahibi kazanır
    ORAN_X DECIMAL(5,2) NULL,              -- Berabere
    ORAN_2 DECIMAL(5,2) NULL,              -- Misafir kazanır

    -- İma edilen olasılıklar (hesaplanmış)
    OLASILIK_1 DECIMAL(5,2) NULL,          -- 1/ORAN_1 * 100
    OLASILIK_X DECIMAL(5,2) NULL,
    OLASILIK_2 DECIMAL(5,2) NULL,

    -- Over/Under 2.5
    ORAN_UST_25 DECIMAL(5,2) NULL,
    ORAN_ALT_25 DECIMAL(5,2) NULL,
    OLASILIK_UST_25 DECIMAL(5,2) NULL,
    OLASILIK_ALT_25 DECIMAL(5,2) NULL,

    -- Overround (bahisçi marjı)
    OVERROUND_1X2 DECIMAL(5,2) NULL,       -- Toplam olasılık - 100
    OVERROUND_OU DECIMAL(5,2) NULL,

    KAYNAK NVARCHAR(50) DEFAULT 'OddsPortal',
    KAYIT_TARIHI DATETIME DEFAULT GETDATE(),
    GUNCELLEME_TARIHI DATETIME NULL
)
GO

-- =============================================
-- 2. GOLCU_ORANLARI - Golcü bahis oranları
-- =============================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'GOLCU_ORANLARI' AND schema_id = SCHEMA_ID('BAHIS'))
CREATE TABLE BAHIS.GOLCU_ORANLARI (
    GOLCU_ORAN_ID INT IDENTITY(1,1) PRIMARY KEY,
    FIKSTURID INT NULL,                    -- FK to FIKSTUR.FIKSTUR
    OYUNCU_ID INT NULL,                    -- FK to TANIM.OYUNCU
    OYUNCU_ADI NVARCHAR(100) NOT NULL,
    TAKIM_ADI NVARCHAR(100) NOT NULL,
    MAC_TARIHI DATE NOT NULL,

    -- Golcü oranları
    ORAN_HER_AN DECIMAL(5,2) NULL,         -- Maçta gol atar
    ORAN_ILK_GOL DECIMAL(5,2) NULL,        -- İlk golü atar
    ORAN_SON_GOL DECIMAL(5,2) NULL,        -- Son golü atar
    ORAN_2_GOL DECIMAL(5,2) NULL,          -- 2+ gol atar
    ORAN_HAT_TRICK DECIMAL(5,2) NULL,      -- 3+ gol atar

    -- İma edilen olasılıklar
    OLASILIK_GOL DECIMAL(5,2) NULL,        -- 1/ORAN_HER_AN * 100

    KAYNAK NVARCHAR(50) DEFAULT 'Tipico',
    KAYIT_TARIHI DATETIME DEFAULT GETDATE(),
    GUNCELLEME_TARIHI DATETIME NULL
)
GO

-- =============================================
-- Index'ler
-- =============================================
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_MAC_ORANLARI_TARIH')
    CREATE INDEX IX_MAC_ORANLARI_TARIH ON BAHIS.MAC_ORANLARI(MAC_TARIHI)
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_MAC_ORANLARI_FIKSTUR')
    CREATE INDEX IX_MAC_ORANLARI_FIKSTUR ON BAHIS.MAC_ORANLARI(FIKSTURID)
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_GOLCU_ORANLARI_TARIH')
    CREATE INDEX IX_GOLCU_ORANLARI_TARIH ON BAHIS.GOLCU_ORANLARI(MAC_TARIHI)
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_GOLCU_ORANLARI_OYUNCU')
    CREATE INDEX IX_GOLCU_ORANLARI_OYUNCU ON BAHIS.GOLCU_ORANLARI(OYUNCU_ID)
GO

PRINT 'BAHIS schema ve tablolar oluşturuldu.'
GO
