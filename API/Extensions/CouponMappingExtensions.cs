using API.DTOs;
using Core.Entities;

namespace API.Extensions;

public static class CouponMappingExtensions
{
    public static Coupon ToEntity(this CouponDto couponDto)
    {
        if (couponDto == null) throw new ArgumentNullException(nameof(couponDto));
        return new Coupon
        {
            Name = couponDto.Name,
            AmountOff = couponDto.AmountOff,
            PercentOff = couponDto.PercentOff,
            PromotionCode = couponDto.PromotionCode,
            CouponId = couponDto.CouponId
        };
    }
}