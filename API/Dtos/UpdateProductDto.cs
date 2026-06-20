using System.ComponentModel.DataAnnotations;
namespace API.DTOs;

public class UpdateProductDto
{
    public string? Name { get; set; }
    public string? Artist { get; set; }
    public string? Description { get; set; }

    [Range(0.01, double.MaxValue, ErrorMessage = "Price must be greater than 0")]
    public decimal? Price { get; set; }
    
    public string? PictureUrl { get; set; }
    public string? Genre { get; set; }
    public string? Label { get; set; }

    [Range(0, int.MaxValue, ErrorMessage = "Stock must be a non-negative integer")]
    public int? Stock { get; set; }
}