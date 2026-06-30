namespace Core.Entities.OrderAggregate;

public enum OrderStatus
{
    Pending,
    PaymentReceived,
    PaymentFailed,
    Shipped,    // unused in this project, but could be used in a real-world scenario
    Cancelled   // unused in this project, but could be used in a real-world scenario
}