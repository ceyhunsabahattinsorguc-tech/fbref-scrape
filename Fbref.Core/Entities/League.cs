using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Fbref.Core.Entities;

[Table("LIG", Schema = "TANIM")]
public class League
{
    [Key]
    [Column("LIG_ID")]
    public int Id { get; set; }

    [Column("LIG_ADI")]
    public string? Name { get; set; }      // "Super Lig"

    [Column("URL")]
    public string? Url { get; set; }       // fikstür url'i

    [Column("ULKE")]
    public string? Country { get; set; }   // "TÜRKİYE"

    [Column("SON_ISLEM_ZAMANI")]
    public DateTime? LastUpdatedAt { get; set; }

    [Column("LEVEL_")]
    public byte? Level { get; set; }

    [Column("SEZON")]
    public string? SeasonName { get; set; }

    [Column("FIKSTUR_TABLO_ID")]
    public string? FixtureTableId { get; set; } // sched_2024-2025_26_1 gibi

    [Column("DURUM")]
    public int Status { get; set; } // 1 = aktif

    [Column("SEZON_ID")]
    public int? SeasonId { get; set; }

    [Column("SEZON_URL")]
    public string? SeasonUrl { get; set; }

    public Season? Season { get; set; }
}
