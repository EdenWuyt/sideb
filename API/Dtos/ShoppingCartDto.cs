using System.ComponentModel.DataAnnotations;

namespace API.DTOs;

public class ShoppingCartDto
{
    [Required]
    public string Id { get; set; } = string.Empty;

    public List<CartItemDto> Items { get; set; } = [];
    
    public int? DeliveryMethodId { get; set; }
    
    public string? ClientSecret { get; set; }
    
    public string? PaymentIntentId { get; set; }
}
