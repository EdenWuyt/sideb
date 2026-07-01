using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.Authorization;
using System.Collections.Concurrent;
using API.Extensions;

namespace API.SignalR;

[Authorize]
public class NotificationHub (): Hub
{
    private static readonly ConcurrentDictionary<string, HashSet<string>> _userConnections = new();

    public override Task OnConnectedAsync()
    {
        var email = Context.User?.GetEmail();
        if (!string.IsNullOrEmpty(email))
        {
            var connections = _userConnections.GetOrAdd(email, _ => new HashSet<string>());
            lock (connections)
            {
                connections.Add(Context.ConnectionId);
            }
        }
        return base.OnConnectedAsync();
    }

    public override Task OnDisconnectedAsync(Exception? exception)
    {
        var email = Context.User?.GetEmail();
        if (!string.IsNullOrEmpty(email) && _userConnections.TryGetValue(email, out var connections))
        {
            lock (connections)
            {
                connections.Remove(Context.ConnectionId);
                if (connections.Count == 0)
                {
                    _userConnections.TryRemove(email, out _);
                }
            }
        }
        return base.OnDisconnectedAsync(exception);
    }

    public static List<string> GetConnectionIdsByEmail(string email)
    {
        if (_userConnections.TryGetValue(email, out var connections))
        {
            lock (connections)
            {
                return connections.ToList();
            }
        }
        return [];
    }
}