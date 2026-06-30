using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Core.Entities.OrderAggregate;

namespace Infrastructure.Config;

public class OrderConfiguration : IEntityTypeConfiguration<Order>
{
    public void Configure(EntityTypeBuilder<Order> builder)
    {
        builder.OwnsOne(o => o.ShippingAddress, o => o.WithOwner());
        builder.OwnsOne(o => o.PaymentSummary, o => o.WithOwner());

        builder.Property(o => o.Status).HasConversion(
            os => os.ToString(),
            os => Enum.Parse<OrderStatus>(os)
        );
        builder.Property(o => o.OrderDate).HasConversion(
            od => od.ToUniversalTime(),
            od => DateTime.SpecifyKind(od, DateTimeKind.Utc)
        );
        builder.Property(o => o.Subtotal).HasColumnType("decimal(18,2)");
        builder.HasMany(o => o.OrderItems).WithOne().OnDelete(DeleteBehavior.Cascade);
    }
}