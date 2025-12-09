using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Fbref.Core.Entities;

[Table("FIKSTUR", Schema = "FIKSTUR")]
public class Fixture
{
    [Key]
    [Column("FIKSTURID")]
    public int Id { get; set; }

    [Column("HAFTA")]
    public int? Week { get; set; }

    [Column("GUN")]
    public string? DayShort { get; set; }  // "Cum", "Cts" vs.

    [Column("TARIH")]
    public DateTime? MatchDate { get; set; }

    [Column("EVSAHIBI")]
    public string? HomeTeamName { get; set; }

    [Column("SKOR")]
    public string? ScoreText { get; set; } // "2–1" gibi, sonra pars edebiliriz

    [Column("MISAFIR")]
    public string? AwayTeamName { get; set; }

    [Column("SEYIRCI")]
    public int? Attendance { get; set; }

    [Column("STADYUM")]
    public string? Stadium { get; set; }

    [Column("HAKEM")]
    public string? Referee { get; set; }

    [Column("URL")]
    public string? MatchUrl { get; set; }  // maç sayfası

    [Column("KAYIT_TARIHI")]
    public DateTime? CreatedAt { get; set; }

    [Column("DEGISIKLIK_TARIHI")]
    public DateTime? UpdatedAt { get; set; }

    [Column("DURUM")]
    public int? Status { get; set; }

    [Column("NOTLAR")]
    public string? Notes { get; set; }

    [Column("LIG_ID")]
    public int? LeagueId { get; set; }

    public League? League { get; set; }
}
