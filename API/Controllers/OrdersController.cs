using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Core.Entities.OrderAggregate;
using Core.Entities;
using Core.Interfaces;
using Core.Specifications;
using API.DTOs;
using API.Extensions;

namespace API.Controllers;

[Authorize]
public class OrdersController(ICartService cartService, IUnitOfWork unitOfWork) : BaseApiController
{
    [HttpPost]
    public async Task<ActionResult<Order>> CreateOrder(CreateOrderDto orderDto)
    {
        var email = User.GetEmail();

        var cart = await cartService.GetCartAsync(orderDto.CartId);
        if (cart == null) return BadRequest("Cart not found");
        if (cart.PaymentIntentId == null) return BadRequest("Payment intent not found");

        var items = new List<OrderItem>();
        foreach (var item in cart.Items)
        {
            var product = await unitOfWork.Repository<Product>().GetByIdAsync(item.ProductId);
            if (product == null) return BadRequest($"Problem with the order");

            var itemOrdered = new ProductItemOrdered { ProductId = product.Id, ProductName = product.Name, PictureUrl = product.PictureUrl };
            var orderItem = new OrderItem { ItemOrdered = itemOrdered, Price = product.Price, Quantity = item.Quantity };
            items.Add(orderItem);
        }

        var deliveryMethod = await unitOfWork.Repository<DeliveryMethod>().GetByIdAsync(orderDto.DeliveryMethodId);
        if (deliveryMethod == null) return BadRequest("Delivery method not found");

        var order = new Order
        {
            BuyerEmail = email,
            OrderItems = items,
            DeliveryMethod = deliveryMethod,
            ShippingAddress = orderDto.ShippingAddress.ToEntity(),
            PaymentSummary = orderDto.PaymentSummary.ToEntity(),
            Subtotal = items.Sum(item => item.Price * item.Quantity),
            Discount = orderDto.Discount,
            PaymentIntentId = cart.PaymentIntentId
        };

        unitOfWork.Repository<Order>().Add(order);
        if (await unitOfWork.Complete())
        {
            return Ok(order);
        }
        return BadRequest("Problem creating order");
    }

    [HttpGet]
    public async Task<ActionResult<IReadOnlyList<OrderDto>>> GetOrdersForUser()
    {
        var email = User.GetEmail();
        var spec = new OrderSpecification(email);
        var orders = await unitOfWork.Repository<Order>().ListAsync(spec);
        var ordersToReturn = orders.Select(order => order.ToDto()).ToList();
        return Ok(ordersToReturn);
    }

    [HttpGet("{id:int}")]
    public async Task<ActionResult<OrderDto>> GetOrderById(int id)
    {
        var email = User.GetEmail();
        var spec = new OrderSpecification(id, email);
        var order = await unitOfWork.Repository<Order>().GetBySpecAsync(spec);
        if (order == null) return NotFound();
        return Ok(order.ToDto());
    }
}