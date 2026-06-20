using System.ComponentModel.DataAnnotations;

namespace API.DTOs;

public class CreateProductDto
{
    [Required]
    public string Name { get; set; } = string.Empty;

    [Required]
    public string Artist { get; set; } = string.Empty;

    [Required]
    public string Description { get; set; } = string.Empty;

    [Range(0.01, double.MaxValue, ErrorMessage = "Price must be greater than 0")]
    public decimal Price { get; set; }

    [Required]
    public string PictureUrl { get; set; } = string.Empty;

    [Required]
    public string Genre { get; set; } = string.Empty;
    
    [Required]
    public string Label { get; set; } = string.Empty;

    [Range(0, int.MaxValue, ErrorMessage = "Stock must be a non-negative integer")]
    public int Stock { get; set; }
}