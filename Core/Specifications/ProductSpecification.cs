using Core.Entities;

namespace Core.Specifications;

public class ProductSpecification : BaseSpecification<Product>
{
    public ProductSpecification(string? genre, string? label, string? artist, string? sort) : base(x =>
            (string.IsNullOrWhiteSpace(genre) || x.Genre == genre) &&
            (string.IsNullOrWhiteSpace(label) || x.Label == label) &&
            (string.IsNullOrWhiteSpace(artist) || x.Artist == artist)
            )
    {
        switch (sort)
        {
            case "priceAsc":
                AddOrderBy(p => p.Price);
                break;
            case "priceDesc":
                AddOrderByDescending(p => p.Price);
                break;
            default:
                AddOrderBy(p => p.Name);
                break;
        }
    }
}