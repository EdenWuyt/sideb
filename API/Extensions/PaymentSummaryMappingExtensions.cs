using API.DTOs;
using Core.Entities.OrderAggregate;

namespace API.Extensions;

public static class PaymentSummaryMappingExtensions
{
    public static PaymentSummary ToEntity(this PaymentSummaryDto paymentSummaryDto)
    {
        if (paymentSummaryDto == null) throw new ArgumentNullException(nameof(paymentSummaryDto));
        return new PaymentSummary   
        {
            Brand = paymentSummaryDto.Brand,
            Last4Digits = paymentSummaryDto.Last4Digits,
            ExpMonth = paymentSummaryDto.ExpMonth,
            ExpYear = paymentSummaryDto.ExpYear,
        };
    }
}   