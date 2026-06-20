namespace Core.Specifications;

public class ProductSpecParams
{
    private const int MaxPageSize = 50;
    public int PageIndex { get; set; } = 1;
    private int _pageSize = 6;
    public int PageSize
    {
        get => _pageSize;
        set => _pageSize = (value > MaxPageSize) ? MaxPageSize : value;
    }

    private List<string> _genres = [];
    public List<string> Genres
    {
        get => _genres;
        set => _genres = value.SelectMany(g => g.Split(',', StringSplitOptions.RemoveEmptyEntries).Select(s => s.Trim())).ToList();
    }

    private List<string> _labels = [];
    public List<string> Labels
    {
        get => _labels;
        set => _labels = value.SelectMany(l => l.Split(',', StringSplitOptions.RemoveEmptyEntries).Select(s => s.Trim())).ToList();
    }

    private List<string> _artists = [];
    public List<string> Artists
    {
        get => _artists;
        set => _artists = value.SelectMany(a => a.Split(',', StringSplitOptions.RemoveEmptyEntries).Select(s => s.Trim())).ToList();
    }

    public string? Sort { get; set; }

    private string? _search;
    public string Search
    {
        get => _search ?? string.Empty;
        set => _search = value.ToLower();
    }
}