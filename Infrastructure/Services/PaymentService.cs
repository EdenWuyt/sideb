using Core.Entities;
using Core.Interfaces;
using Microsoft.Extensions.Configuration;
using Stripe;

namespace Infrastructure.Services;

public class PaymentService(
    IConfiguration config,
    ICartService cartService,
    IUnitOfWork unitOfWork) : IPaymentService
{
    public async Task<ShoppingCart?> CreateOrUpdatePaymentIntent(string cartId)
    {
        StripeConfiguration.ApiKey = config["StripeSettings:SecretKey"];

        // get the cart from redis
        var cart = await cartService.GetCartAsync(cartId) ?? throw new Exception("Cart not found");

        // get the shipping price
        var shippingPrice = await GetShippingPriceAsync(cart);

        // get the correct price for each item in the cart and update it if necessary
        cart = await ValidateCartItemsInCartAsync(cart);

        var subtotal = CalculateSubtotal(cart);

        subtotal = await ApplyDiscountAsync(cart, subtotal);

        var total = subtotal + shippingPrice;

        // create or update the payment intent
        cart = await CreateUpdatePaymentIntentAsync(cart, total);

        await cartService.SetCartAsync(cart);

        return cart;
    }

    private async Task<ShoppingCart> CreateUpdatePaymentIntentAsync(ShoppingCart cart, decimal total)
    {
        var service = new PaymentIntentService();

        if (string.IsNullOrEmpty(cart.PaymentIntentId))
        {
            var options = new PaymentIntentCreateOptions
            {
                Amount = ToStripeAmount(total),
                Currency = "aud",
                PaymentMethodTypes = ["card"]
            };
            var intent = await service.CreateAsync(options);
            cart.PaymentIntentId = intent.Id;
            cart.ClientSecret = intent.ClientSecret;
        }
        else
        {
            var options = new PaymentIntentUpdateOptions
            {
                Amount = ToStripeAmount(total)
            };
            await service.UpdateAsync(cart.PaymentIntentId, options);
        }
        return cart;
    }

    private async Task<decimal> ApplyDiscountAsync(ShoppingCart cart, decimal subtotal)
    {
        var discount = 0m;
        var couponService = new Stripe.CouponService();

        if (cart.Coupon == null) return subtotal;

        var coupon = await couponService.GetAsync(cart.Coupon.CouponId);

        if (coupon == null || !coupon.Valid)
        {
            cart.Coupon = null;
            return subtotal;
        }
        else
        {
            cart.Coupon = new Core.Entities.Coupon
            {
                Name = coupon.Name,
                AmountOff = coupon.AmountOff,
                PercentOff = coupon.PercentOff,
                PromotionCode = cart.Coupon.PromotionCode,
                CouponId = coupon.Id
            };
            if (coupon.AmountOff.HasValue) discount = coupon.AmountOff.Value / 100m;
            else if (coupon.PercentOff.HasValue) discount = subtotal * (coupon.PercentOff.Value / 100m);
        }

        // ensure that the discount does not exceed the subtotal
        discount = RoundCurrency(discount) > subtotal ? subtotal : RoundCurrency(discount);
        return subtotal - discount;
    }

    private decimal CalculateSubtotal(ShoppingCart cart)
    {
        return cart.Items.Sum(i => i.Quantity * i.Price);
    }

    private async Task<ShoppingCart> ValidateCartItemsInCartAsync(ShoppingCart cart)
    {
        foreach (var item in cart.Items)
        {
            var product = await unitOfWork.Repository<Core.Entities.Product>().GetByIdAsync(item.ProductId)
                ?? throw new Exception("Problem getting product in cart");
            if (item.Price != product.Price) item.Price = product.Price;
        }
        return cart;
    }

    private async Task<decimal> GetShippingPriceAsync(ShoppingCart cart)
    {
        var shippingPrice = 0m;

        if (cart.DeliveryMethodId.HasValue)
        {
            var deliveryMethod = await unitOfWork.Repository<DeliveryMethod>().GetByIdAsync(cart.DeliveryMethodId.Value)
                ?? throw new Exception("Delivery method not found");
            shippingPrice = deliveryMethod.Price;
        }

        return shippingPrice;
    }

    private static decimal RoundCurrency(decimal amount)
    {
        return Math.Round(amount, 2, MidpointRounding.AwayFromZero);
    }

    private static long ToStripeAmount(decimal amount)
    {
        return (long)Math.Round(RoundCurrency(amount) * 100, 0, MidpointRounding.AwayFromZero);
    }
}
