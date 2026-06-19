using Core.Entities;

namespace Core.Specifications;

public class LabelListSpecification : BaseSpecification<Product, string>
{
    public LabelListSpecification()
    {
        AddSelect(p => p.Label);
        ApplyDistinct();
    }
}