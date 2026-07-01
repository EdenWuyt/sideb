using Core.Entities.OrderAggregate;

namespace Core.Specifications;

public class OrderSpecification : BaseSpecification<Order>
{
    public OrderSpecification(string buyerEmail) : base(o => o.BuyerEmail == buyerEmail)
    {
        AddInclude(o => o.OrderItems);  // Or AddIncludeString("OrderItems");
        AddInclude(o => o.DeliveryMethod);  // Or AddIncludeString("DeliveryMethod");
        AddOrderByDescending(o => o.OrderDate);
    }

    public OrderSpecification(int id, string buyerEmail) : base(o => o.Id == id && o.BuyerEmail == buyerEmail)
    {
        AddInclude(o => o.OrderItems);  // Or AddIncludeString("OrderItems");
        AddInclude(o => o.DeliveryMethod);  // Or AddIncludeString("DeliveryMethod
    }

    public OrderSpecification(string paymentIntentId, bool isPaymentIntent) : base(o => o.PaymentIntentId == paymentIntentId)
    {
        AddInclude(o => o.OrderItems);  // Or AddIncludeString("OrderItems");
        AddInclude(o => o.DeliveryMethod);  // Or AddIncludeString("DeliveryMethod
    }
}