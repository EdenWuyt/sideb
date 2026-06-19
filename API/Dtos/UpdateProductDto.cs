namespace API.Dtos;

public class UpdateProductDto
{
    public string? Name { get; set; }
    public string? Artist { get; set; }
    public string? Description { get; set; }
    public decimal? Price { get; set; }
    public string? PictureUrl { get; set; }
    public string? Genre { get; set; }
    public string? Label { get; set; }
    public int? Stock { get; set; }
}