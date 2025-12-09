using Fbref.Infrastructure;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<FbrefDbContext>(options =>
    options.UseSqlServer(builder.Configuration.GetConnectionString("FbrefDb")));

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Scraper servisini birazdan ekleyeceğiz:
builder.Services.AddScoped<FbrefSuperLigScraper>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.MapGet("/api/seasons/active", async (FbrefDbContext db) =>
{
    // Aktif sezon (DURUM = 1)
    var season = await db.Seasons.FirstOrDefaultAsync(s => s.Status == 1);
    return season is null ? Results.NotFound() : Results.Ok(season);
});

app.MapGet("/api/leagues/superlig", async (FbrefDbContext db) =>
{
    var superLig = await db.Leagues
        .Where(l => l.Name == "Super Lig" || l.Name == "Süper Lig")
        .ToListAsync();

    return Results.Ok(superLig);
});

// Birazdan gerçek scraping'i koyacağımız endpoint:
app.MapPost("/api/scrape/superlig/fixtures", async (FbrefSuperLigScraper scraper) =>
{
    await scraper.UpdateSuperLigFixturesAsync();
    return Results.Ok(new { message = "Super Lig fikstür güncelleme tamamlandı (veya denendi)." });
});

app.Run();
