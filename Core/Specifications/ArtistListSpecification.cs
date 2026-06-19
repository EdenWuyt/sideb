using Core.Entities;

namespace Core.Specifications;

public class ArtistListSpecification : BaseSpecification<Product, string>
{
    public ArtistListSpecification()
    {
        AddSelect(p => p.Artist);
        ApplyDistinct();
    }
}