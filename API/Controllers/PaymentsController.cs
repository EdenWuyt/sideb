using Core.Interfaces;
using Core.Entities;
using Core.Entities.OrderAggregate;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Authorization;
using Stripe;
using Core.Specifications;
using Microsoft.AspNetCore.SignalR;
using API.SignalR;
using API.Extensions;

namespace API.Controllers;

public class PaymentsController(
    IPaymentService paymentService,
    IUnitOfWork unitOfWork,
    ILogger<PaymentsController> logger,
    IConfiguration configuration,
    IHubContext<NotificationHub> notificationHubContext) : BaseApiController
{
    private readonly string _whSecret = configuration["StripeSettings:WhSecret"]!;

    [Authorize]
    [HttpPost("{cartId}")]
    public async Task<ActionResult<ShoppingCart>> CreateOrUpdatePaymentIntent(string cartId)
    {
        var cart = await paymentService.CreateOrUpdatePaymentIntent(cartId);
        if (cart == null) return BadRequest("Problem with your cart");
        return Ok(cart);
    }

    [HttpGet("delivery-methods")]
    public async Task<ActionResult<IReadOnlyList<DeliveryMethod>>> GetDeliveryMethods()
    {
        var deliveryMethods = await unitOfWork.Repository<DeliveryMethod>().ListAllAsync();
        return Ok(deliveryMethods);
    }

    [HttpPost("webhook")]
    public async Task<IActionResult> StripeWebhook()
    {
        var json = await new StreamReader(Request.Body).ReadToEndAsync();   // Raw json payload from Stripe
        try
        {
            var stripeEvent = ConstructStripeEvent(json);
            if (stripeEvent.Data.Object is not PaymentIntent paymentIntent) return BadRequest("Invalid event data");
            await HandlePaymentIntentSucceeded(paymentIntent);
            return Ok();
        }
        catch (StripeException ex)
        {
            logger.LogError(ex, "Stripe webhook error");
            return StatusCode(StatusCodes.Status500InternalServerError, "Stripe webhook error");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "An unexpected error occurred");
            return StatusCode(StatusCodes.Status500InternalServerError, "An unexpected error occurred");
        }
    }

    private async Task HandlePaymentIntentSucceeded(PaymentIntent paymentIntent)
    {
        if (paymentIntent.Status == "succeeded")
        {
            Core.Entities.OrderAggregate.Order? order = null;
            // Retry logic to handle potential delays in order creation
            for (int i = 0; i < 5; i++)
            {
                var orderSpec = new OrderSpecification(paymentIntent.Id, true);
                order = await unitOfWork.Repository<Core.Entities.OrderAggregate.Order>().GetBySpecAsync(orderSpec);
                if (order != null) break;
                await Task.Delay(500); // Wait for 500ms before retrying
            }
            if (order == null)
            {
                logger.LogError("Order not found for PaymentIntent ID: {PaymentIntentId}", paymentIntent.Id);
                throw new Exception("Order not found");
            }
            var orderTotalInCents = (long)Math.Round(order.GetTotal() * 100, MidpointRounding.AwayFromZero);
            if (orderTotalInCents != paymentIntent.Amount)
            {
                order.Status = OrderStatus.PaymentMismatch;
            }
            else
            {
                order.Status = OrderStatus.PaymentReceived;
            }
            await unitOfWork.Complete();

            // Notify the user about the payment status (signalR)
            var connectionIds = NotificationHub.GetConnectionIdsByEmail(order.BuyerEmail);
            if (connectionIds.Count != 0)
            {
                await notificationHubContext.Clients.Clients(connectionIds).SendAsync("OrderCompletedNotification", order.ToDto());
            }
        }
    }

    private Event ConstructStripeEvent(string json)
    {
        try
        {
            return EventUtility.ConstructEvent(
                json,
                Request.Headers["Stripe-Signature"],
                _whSecret,
                throwOnApiVersionMismatch: false    // Set to false to avoid exceptions for API version mismatches locally
                );
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error constructing Stripe event");
            throw new StripeException("Invalid signature");
        }
    }
}