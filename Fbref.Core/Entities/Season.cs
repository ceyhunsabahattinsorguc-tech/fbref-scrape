using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Fbref.Core.Entities;

[Table("SEZON", Schema = "FIKSTUR")]
public class Season
{
    [Key]
    [Column("SEZON_ID")]
    public int Id { get; set; }

    [Column("SEZON")]
    public string? Name { get; set; }  // "2024-2025"

    [Column("DURUM")]
    public int Status { get; set; }    // 1 = aktif

    [Column("SON_ISLEM_ZAMANI")]
    public DateTime? LastUpdatedAt { get; set; }

    public ICollection<League> Leagues { get; set; } = new List<League>();
}
