using Core.Entities;
using Core.Interfaces;
using Microsoft.EntityFrameworkCore;

namespace Infrastructure.Data;

public class ProductRepository(StoreContext context) : IProductRepository
{
    private readonly StoreContext _context = context;

    public async Task<IReadOnlyList<Product>> GetProductsAsync(string? genre, string? label, string? artist, string? sort)
    {
        var query = _context.Products.AsQueryable();

        if (!string.IsNullOrWhiteSpace(genre))
        {
            query = query.Where(p => p.Genre == genre);
        }

        if (!string.IsNullOrWhiteSpace(label))
        {
            query = query.Where(p => p.Label == label);
        }

        if (!string.IsNullOrWhiteSpace(artist))
        {
            query = query.Where(p => p.Artist == artist);
        }

        query = sort switch
        {
            "priceAsc" => query.OrderBy(p => p.Price),
            "priceDesc" => query.OrderByDescending(p => p.Price),
            _ => query.OrderBy(p => p.Name)
        };
        

        return await query.ToListAsync();
    }

    public async Task<Product?> GetProductByIdAsync(int id)
    {
        return await _context.Products.FindAsync(id);
    }

    public async Task<IReadOnlyList<string>> GetGenresAsync()
    {
        return await _context.Products.Select(p => p.Genre).Distinct().ToListAsync();
    }

    public async Task<IReadOnlyList<string>> GetLabelsAsync()
    {
        return await _context.Products.Select(p => p.Label).Distinct().ToListAsync();
    }

    public async Task<IReadOnlyList<string>> GetArtistsAsync()
    {
        return await _context.Products.Select(p => p.Artist).Distinct().ToListAsync();
    }

    public void AddProduct(Product product)
    {
        _context.Products.Add(product);
    }

    public void UpdateProduct(Product product)
    {
        _context.Entry(product).State = EntityState.Modified;
    }

    public void DeleteProduct(Product product)
    {
        _context.Products.Remove(product);
    }

    public bool ProductExists(int id)
    {
        return _context.Products.Any(p => p.Id == id);
    }

    public async Task<bool> SaveChangesAsync()
    {
        return await _context.SaveChangesAsync() > 0;
    }

}
