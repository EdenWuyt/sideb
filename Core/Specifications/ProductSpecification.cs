using Core.Entities;

namespace Core.Specifications;

public class ProductSpecification : BaseSpecification<Product>
{
    public ProductSpecification(ProductSpecParams specParams) : base(x =>
            (specParams.Genres.Count == 0 || specParams.Genres.Contains(x.Genre)) &&
            (specParams.Labels.Count == 0 || specParams.Labels.Contains(x.Label)) &&
            (specParams.Artists.Count == 0 || specParams.Artists.Contains(x.Artist)) &&
            (string.IsNullOrEmpty(specParams.Search) || x.Name.ToLower().Contains(specParams.Search) || x.Artist.ToLower().Contains(specParams.Search))
    )
    {
        ApplyPaging((specParams.PageIndex - 1) * specParams.PageSize, specParams.PageSize);

        switch (specParams.Sort)
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