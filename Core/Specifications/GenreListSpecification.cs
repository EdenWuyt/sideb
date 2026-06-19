using Core.Entities;

namespace Core.Specifications;

public class GenreListSpecification : BaseSpecification<Product, string>
{
    public GenreListSpecification()
    {
        AddSelect(p => p.Genre);
        ApplyDistinct();
    }
}