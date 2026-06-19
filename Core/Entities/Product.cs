namespace Core.Entities;

public class Product: BaseEntity
{
    public required string Name { get; set; }
    public required string Artist { get; set; }
    public required string Description { get; set; }
    public decimal Price { get; set; }
    public required string PictureUrl { get; set; }
    public required string Genre { get; set; }
    public required string Label { get; set; }
    public int Stock { get; set; }
}
