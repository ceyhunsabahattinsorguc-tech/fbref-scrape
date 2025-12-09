using Fbref.Core.Entities;
using Microsoft.EntityFrameworkCore;

namespace Fbref.Infrastructure;

public class FbrefDbContext : DbContext
{
    public FbrefDbContext(DbContextOptions<FbrefDbContext> options) : base(options)
    {
    }

    public DbSet<Season> Seasons => Set<Season>();
    public DbSet<League> Leagues => Set<League>();
    public DbSet<Fixture> Fixtures => Set<Fixture>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Season>()
            .HasMany(s => s.Leagues)
            .WithOne(l => l.Season)
            .HasForeignKey(l => l.SeasonId);

        modelBuilder.Entity<League>()
            .HasMany<Fixture>()
            .WithOne(f => f.League!)
            .HasForeignKey(f => f.LeagueId);

        base.OnModelCreating(modelBuilder);
    }
}
