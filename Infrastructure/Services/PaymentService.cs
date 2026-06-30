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
        var cart = await cartService.GetCartAsync(cartId);
        if (cart == null) return null;

        // get the shipping price
        var shippingPrice = 0m;

        if (cart.DeliveryMethodId.HasValue)
        {
            var deliveryMethod = await unitOfWork.Repository<DeliveryMethod>().GetByIdAsync(cart.DeliveryMethodId.Value);
            if (deliveryMethod == null) return null;
            shippingPrice = deliveryMethod.Price;
        }

        // get the correct price for each item in the cart and update it if necessary
        foreach (var item in cart.Items)
        {
            var product = await unitOfWork.Repository<Core.Entities.Product>().GetByIdAsync(item.ProductId);
            if (product == null) return null;

            if (item.Price != product.Price) item.Price = product.Price;
        }

        // create or update the payment intent
        var service = new PaymentIntentService();
        PaymentIntent? intent = null;

        if (string.IsNullOrEmpty(cart.PaymentIntentId))
        {
            var options = new PaymentIntentCreateOptions
            {
                Amount = (long)cart.Items.Sum(i => i.Quantity * (i.Price * 100)) + (long)(shippingPrice * 100),
                Currency = "aud",
                PaymentMethodTypes = ["card"]
            };
            intent = await service.CreateAsync(options);
            cart.PaymentIntentId = intent.Id;
            cart.ClientSecret = intent.ClientSecret;
        }
        else
        {
            var options = new PaymentIntentUpdateOptions
            {
                Amount = (long)cart.Items.Sum(i => i.Quantity * (i.Price * 100)) + (long)(shippingPrice * 100)
            };
            intent = await service.UpdateAsync(cart.PaymentIntentId, options);
        }

        // update the cart in redis
        await cartService.SetCartAsync(cart);

        return cart;
    }
}