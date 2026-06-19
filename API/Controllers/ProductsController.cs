using Core.Entities;
using Core.Interfaces;
using Infrastructure.Data;
using API.Dtos;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace API.Controllers;

[ApiController]
[Route("api/[controller]")]
public class ProductsController(IProductRepository repo) : ControllerBase
{
    private readonly IProductRepository _repo = repo;

    [HttpGet]
    public async Task<ActionResult<IReadOnlyList<Product>>> GetProducts(string? genre, string? label, string? artist, string? sort)
    {
        var products = await _repo.GetProductsAsync(genre, label, artist, sort);
        return Ok(products);
    }

    [HttpGet("{id:int}")]
    public async Task<ActionResult<Product>> GetProduct(int id)
    {
        var product = await _repo.GetProductByIdAsync(id);
        if (product == null) return NotFound();
        return Ok(product);
    }

    [HttpPost]
    public async Task<ActionResult<Product>> CreateProduct(Product product)
    {
        _repo.AddProduct(product);
        if (await _repo.SaveChangesAsync())
        {
            return CreatedAtAction(nameof(GetProduct), new { id = product.Id }, product);
        }

        return BadRequest("Problem creating this product");
    }

    [HttpPut("{id:int}")]
    public async Task<ActionResult> UpdateProduct(int id, Product product)
    {
        if (id != product.Id || !ProductExists(id)) return BadRequest("Failed to update this product");

        _repo.UpdateProduct(product);

        if (await _repo.SaveChangesAsync())
        {
            return NoContent();
        }

        return BadRequest("Problem updating this product");
    }

    [HttpPatch("{id:int}")]
    public async Task<ActionResult> PatchProduct(int id, UpdateProductDto dto)
    {
        var product = await _repo.GetProductByIdAsync(id);
        if (product == null) return NotFound();

        if (dto == null) return BadRequest("Invalid patch data");

        if (dto.Name is not null) product.Name = dto.Name;
        if (dto.Artist is not null) product.Artist = dto.Artist;
        if (dto.Description is not null) product.Description = dto.Description;
        if (dto.Price.HasValue) product.Price = dto.Price.Value;
        if (dto.PictureUrl is not null) product.PictureUrl = dto.PictureUrl;
        if (dto.Genre is not null) product.Genre = dto.Genre;
        if (dto.Label is not null) product.Label = dto.Label;
        if (dto.Stock.HasValue) product.Stock = dto.Stock.Value;
        
        if (await _repo.SaveChangesAsync())
        {
            return NoContent();
        }

        return BadRequest("Problem updating this product");
    }

    [HttpDelete("{id:int}")]
    public async Task<ActionResult> DeleteProduct(int id)
    {
        var product = await _repo.GetProductByIdAsync(id);
        if (product == null) return NotFound();

        _repo.DeleteProduct(product);

        if (await _repo.SaveChangesAsync())
        {
            return NoContent();
        }

        return BadRequest("Problem deleting this product");
    }

    [HttpGet("genres")]
    public async Task<ActionResult<IReadOnlyList<string>>> GetGenres()
    {
        var genres = await _repo.GetGenresAsync();
            return Ok(genres);
        } 

    [HttpGet("labels")]
    public async Task<ActionResult<IReadOnlyList<string>>> GetLabels()
    {
        var labels = await _repo.GetLabelsAsync();
        return Ok(labels);
    }

    [HttpGet("artists")]
    public async Task<ActionResult<IReadOnlyList<string>>> GetArtists()
    {
        var artists = await _repo.GetArtistsAsync();
        return Ok(artists);
    }

    private bool ProductExists(int id)
    {
        return _repo.ProductExists(id);
    }
}