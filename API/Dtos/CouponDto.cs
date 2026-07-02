using System.ComponentModel.DataAnnotations;

namespace API.DTOs;

public class CouponDto
{
    [Required]
    public string Name { get; set; } = string.Empty;

    public decimal? AmountOff { get; set; }
    
    [Range(0, 100)]
    public decimal? PercentOff { get; set; }
    
    [Required]
    public string PromotionCode { get; set; } = string.Empty;
    
    [Required]
    public string CouponId { get; set; } = string.Empty;
}