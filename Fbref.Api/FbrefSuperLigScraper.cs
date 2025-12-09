using HtmlAgilityPack;
using Microsoft.EntityFrameworkCore;
using PuppeteerSharp;
using Fbref.Infrastructure;
using Fbref.Core.Entities;

public class FbrefSuperLigScraper
{
    private readonly FbrefDbContext _db;

    public FbrefSuperLigScraper(FbrefDbContext db)
    {
        _db = db;
    }

    public async Task UpdateSuperLigFixturesAsync()
    {
        var season = await _db.Seasons.FirstOrDefaultAsync(s => s.Status == 1);
        if (season == null)
            throw new Exception("Aktif sezon bulunamadı.");

        var league = await _db.Leagues.FirstOrDefaultAsync(l =>
            l.SeasonId == season.Id &&
           (l.Name == "Süper Lig" || l.Name == "Super Lig"));

        if (league == null)
            throw new Exception("Süper Lig bulunamadı.");

        string html = await GetHtmlAsync(league.Url);

        await ParseFixtureTableAsync(html, league);

        await _db.SaveChangesAsync();
    }

   public async Task<string> GetHtmlAsync(string url)
{
    // Chromium mevcut mu? Değilse indir
    var fetcher = new BrowserFetcher();
    await fetcher.DownloadAsync();

    var browser = await Puppeteer.LaunchAsync(new LaunchOptions
    {
        Headless = false, // Headful aç (Cloudflare blocking çözülür)
        Timeout = 0,
        Args = new[]
        {
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-blink-features=AutomationControlled"
        }
    });

    using var page = await browser.NewPageAsync();

    // Bot görünümünü azalt
    await page.EvaluateFunctionOnNewDocumentAsync(@"() => {
        Object.defineProperty(navigator, 'webdriver', { get: () => false });
    }");

    // Timeout'u artır
    await page.GoToAsync(url, new NavigationOptions
    {
        Timeout = 90000, // 90 saniye
        WaitUntil = new[] { WaitUntilNavigation.Networkidle2 }
    });

    var html = await page.GetContentAsync();

    await browser.CloseAsync();

    return html;
}

    private async Task ParseFixtureTableAsync(string html, League league)
    {
        var doc = new HtmlDocument();
        doc.LoadHtml(html);

        var table = doc.DocumentNode.SelectSingleNode($"//table[@id='{league.FixtureTableId}']");
        if (table == null)
            throw new Exception($"Tablo bulunamadı: {league.FixtureTableId}");

        var rows = table.SelectNodes(".//tbody/tr");
        if (rows == null)
            return;

        foreach (var row in rows)
        {
            var cells = row.SelectNodes("./td");
            if (cells == null || cells.Count < 7)
                continue;

            // ✔ DOĞRU INDEXLER
            string weekText  = cells[0].InnerText.Trim();
            string dayText   = cells[2].InnerText.Trim();
            string dateText  = cells[1].InnerText.Trim();
            string homeTeam  = cells[4].InnerText.Trim();
            string scoreText = cells[5].InnerText.Trim();
            string awayTeam  = cells[6].InnerText.Trim();

            var linkNode = cells[5].SelectSingleNode(".//a[@href]");
            string matchUrl = linkNode != null
                ? "https://fbref.com" + linkNode.GetAttributeValue("href", "")
                : null;

            int? week = null;
            if (int.TryParse(weekText, out var w))
                week = w;

            DateTime? matchDate = null;
            if (DateTime.TryParse(dateText, out var d))
                matchDate = d;

            // DB kontrol
            var existing = await _db.Fixtures.FirstOrDefaultAsync(f =>
                f.LeagueId == league.Id &&
                f.MatchDate == matchDate &&
                f.HomeTeamName == homeTeam &&
                f.AwayTeamName == awayTeam
            );

            if (existing == null)
            {
                existing = new Fixture
                {
                    LeagueId = league.Id,
                    CreatedAt = DateTime.Now
                };
                _db.Fixtures.Add(existing);
            }

            // Güncelleme
            existing.Week         = week;
            existing.DayShort     = dayText;
            existing.MatchDate    = matchDate;
            existing.HomeTeamName = homeTeam;
            existing.AwayTeamName = awayTeam;
            existing.ScoreText    = scoreText;
            existing.MatchUrl     = matchUrl;
            existing.UpdatedAt    = DateTime.Now;
        }
    }
}
