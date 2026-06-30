namespace Core.Entities.OrderAggregate;

public class OrderItem: BaseEntity
{
    public ProductItemOrdered ItemOrdered { get; set; } = null!;    // owned entity
    public decimal Price { get; set; }
    public int Quantity { get; set; }
}