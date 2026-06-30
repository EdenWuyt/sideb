using API.DTOs;
using Core.Entities;

namespace API.Extensions;

public static class ShoppingCartMappingExtensions
{
    public static ShoppingCart ToEntity(this ShoppingCartDto cartDto)
    {
        if (cartDto == null) throw new ArgumentNullException(nameof(cartDto));
        return new ShoppingCart
        {
            Id = cartDto.Id,
            Items = cartDto.Items.Select(i => i.ToEntity()).ToList(),
            DeliveryMethodId = cartDto.DeliveryMethodId,
            ClientSecret = cartDto.ClientSecret,
            PaymentIntentId = cartDto.PaymentIntentId
        };
    }
}