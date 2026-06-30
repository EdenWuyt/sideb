using System.ComponentModel.DataAnnotations;

namespace API.DTOs;

public class CartItemDto
{
    public int ProductId { get; set; }

    [Required]
    public string ProductName { get; set; } = string.Empty;

    public decimal Price { get; set; }

    [Range(0, int.MaxValue, ErrorMessage = "Quantity must be a non-negative integer")]
    public int Quantity { get; set; }

    [Required]
    public string PictureUrl { get; set; } = string.Empty;

    [Required]
    public string Artist { get; set; } = string.Empty;

    [Required]
    public string Genre { get; set; } = string.Empty;

    [Required]
    public string Label { get; set; } = string.Empty;
}
