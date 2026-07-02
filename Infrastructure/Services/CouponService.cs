using Core.Interfaces;
using Microsoft.Extensions.Configuration;
using Stripe;

namespace Infrastructure.Services;

public class CouponService : ICouponService
{
    public CouponService(IConfiguration config)
    {
        StripeConfiguration.ApiKey = config["StripeSettings:SecretKey"];
    }

    public async Task<Core.Entities.Coupon?> GetCouponFromPromoCodeAsync(string code)
    {
        var promotionCodeService = new PromotionCodeService();
        var couponService = new Stripe.CouponService();

        // Retrieve the promotion code from Stripe
        var options = new PromotionCodeListOptions
        {
            Code = code,
            Limit = 1
        };
        var promotionCodeResult = await promotionCodeService.ListAsync(options);
        var promotionCode = promotionCodeResult.Data.FirstOrDefault();

        if (promotionCode == null) return null;

        // subject to Stripe API changes
        var couponId = promotionCode.Promotion.CouponId;
        var coupon = await couponService.GetAsync(couponId);

        return new Core.Entities.Coupon
        {
            Name = coupon.Name,
            AmountOff = coupon.AmountOff,
            PercentOff = coupon.PercentOff,
            PromotionCode = promotionCode.Code,
            CouponId = coupon.Id
        };
    }
}