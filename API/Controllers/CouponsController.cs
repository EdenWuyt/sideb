using Core.Interfaces;
using Core.Entities;
using Microsoft.AspNetCore.Mvc;

namespace API.Controllers;

public class CouponsController(ICouponService couponService) : BaseApiController
{
    [HttpGet("{code}")]
    public async Task<ActionResult<Coupon>> ValidateCoupon(string code)
    {
        var coupon = await couponService.GetCouponFromPromoCodeAsync(code);
        if (coupon == null) return BadRequest("Invalid coupon code");
        return Ok(coupon);
    }
}