using Core.Entities;
using Core.Interfaces;
using Core.Specifications;
using API.DTOs;
using API.RequestHelpers;
using Microsoft.AspNetCore.Mvc;

namespace API.Controllers;

public class ProductsController(IGenericRepository<Product> repo) : BaseApiController
{
    private readonly IGenericRepository<Product> _repo = repo;

    [HttpGet]
    public async Task<ActionResult<IReadOnlyList<Pagination<Product>>>> GetProducts([FromQuery]ProductSpecParams specParams)
    {
            var spec = new ProductSpecification(specParams);

        return await CreatePagedResult(_repo, spec, specParams.PageIndex, specParams.PageSize);
    }

    [HttpGet("{id:int}")]
    public async Task<ActionResult<Product>> GetProduct(int id)
    {
        var product = await _repo.GetByIdAsync(id);
        if (product == null) return NotFound();
        return Ok(product);
    }

    [HttpPost]
    public async Task<ActionResult<Product>> CreateProduct(Product product)
    {
        _repo.Add(product);
        if (await _repo.SaveAllAsync())
        {
            return CreatedAtAction(nameof(GetProduct), new { id = product.Id }, product);
        }

        return BadRequest("Problem creating this product");
    }

    [HttpPut("{id:int}")]
    public async Task<ActionResult> UpdateProduct(int id, Product product)
    {
        if (id != product.Id || !ProductExists(id)) return BadRequest("Failed to update this product");

        _repo.Update(product);

        if (await _repo.SaveAllAsync())
        {
            return NoContent();
        }

        return BadRequest("Problem updating this product");
    }

    [HttpPatch("{id:int}")]
    public async Task<ActionResult> PatchProduct(int id, UpdateProductDto dto)
    {
        var product = await _repo.GetByIdAsync(id);
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
        
        if (await _repo.SaveAllAsync())
        {
            return NoContent();
        }

        return BadRequest("Problem updating this product");
    }

    [HttpDelete("{id:int}")]
    public async Task<ActionResult> DeleteProduct(int id)
    {
        var product = await _repo.GetByIdAsync(id);
        if (product == null) return NotFound();

        _repo.Delete(product);

        if (await _repo.SaveAllAsync())
        {
            return NoContent();
        }

        return BadRequest("Problem deleting this product");
    }

    [HttpGet("genres")]
    public async Task<ActionResult<IReadOnlyList<string>>> GetGenres()
    {
        var spec = new GenreListSpecification();
        var genres = await _repo.ListAsync(spec);
        return Ok(genres);
    } 

    [HttpGet("labels")]
    public async Task<ActionResult<IReadOnlyList<string>>> GetLabels()
    {
        var spec = new LabelListSpecification();
        var labels = await _repo.ListAsync(spec);
        return Ok(labels);
    }

    [HttpGet("artists")]
    public async Task<ActionResult<IReadOnlyList<string>>> GetArtists()
    {
        var spec = new ArtistListSpecification();
        var artists = await _repo.ListAsync(spec);
        return Ok(artists);
    }

    private bool ProductExists(int id)
    {
        return _repo.Exists(id);
    }
}