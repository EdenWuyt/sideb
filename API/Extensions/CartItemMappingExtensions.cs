using API.DTOs;
using Core.Entities;

namespace API.Extensions;

public static class CartItemMappingExtensions
{
    public static CartItem ToEntity(this CartItemDto cartItemDto)
    {
        if (cartItemDto == null) throw new ArgumentNullException(nameof(cartItemDto));
        return new CartItem
        {
            ProductId = cartItemDto.ProductId,
            ProductName = cartItemDto.ProductName,
            Price = cartItemDto.Price,
            Quantity = cartItemDto.Quantity,
            PictureUrl = cartItemDto.PictureUrl,
            Artist = cartItemDto.Artist,
            Genre = cartItemDto.Genre,
            Label = cartItemDto.Label
        };
    }       
}