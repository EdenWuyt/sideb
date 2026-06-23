using Core.Interfaces;
using Core.Entities;
using StackExchange.Redis;
using System.Text.Json;

namespace Infrastructure.Services;

public class CartService(IConnectionMultiplexer redis) : ICartService
{
    private readonly IDatabase _database = redis.GetDatabase();

    public async Task<ShoppingCart?> GetCartAsync(string cartId)
    {
        var data = await _database.StringGetAsync(cartId);
        if (data.IsNullOrEmpty) return null;
        return JsonSerializer.Deserialize<ShoppingCart>((byte[])data!); 
    }

    public async Task<ShoppingCart?> SetCartAsync(ShoppingCart cart)
    {
        var updated = await _database.StringSetAsync(cart.Id, JsonSerializer.Serialize(cart), TimeSpan.FromDays(30));
        if (!updated) return null;
        return await GetCartAsync(cart.Id);
    }

    public async Task<bool> DeleteCartAsync(string cartId)
    {
        return await _database.KeyDeleteAsync(cartId);
    }
}