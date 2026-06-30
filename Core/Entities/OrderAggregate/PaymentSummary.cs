namespace Core.Entities.OrderAggregate;

public class PaymentSummary
{
    public int Last4Digits { get; set; }
    public required string Brand { get; set; }
    public int ExpMonth { get; set; }
    public int ExpYear { get; set; }
}