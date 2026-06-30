using Core.Interfaces;
using Core.Entities;
using API.DTOs;
using API.Extensions;
using Microsoft.AspNetCore.Mvc;

namespace API.Controllers;

public class CartController(ICartService cartService) : BaseApiController
{
    private readonly ICartService _cartService = cartService;

    [HttpGet]
    public async Task<ActionResult<ShoppingCart>> GetCartById(string id)
    {
        var cart = await _cartService.GetCartAsync(id);
        return Ok(cart ?? new ShoppingCart{Id = id});
    }

    [HttpPost]
    public async Task<ActionResult<ShoppingCart>> UpdateCart(ShoppingCartDto cartDto)
    {
        var cart = cartDto.ToEntity();
        var updatedCart = await _cartService.SetCartAsync(cart);
        if (updatedCart == null) return BadRequest("Problem updating the cart");
        return Ok(updatedCart);
    }

    [HttpDelete]
    public async Task<IActionResult> DeleteCart(string id)
    {
        var deleted = await _cartService.DeleteCartAsync(id);
        if (!deleted) return BadRequest("Problem deleting the cart");
        return NoContent();
    }
}