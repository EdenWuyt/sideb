using System.ComponentModel.DataAnnotations;

namespace API.DTOs;

public class PaymentSummaryDto
{
    [Range(0, 9999, ErrorMessage = "Last 4 digits must be a 4-digit number")]
    public int Last4Digits { get; set; }
    [Required]
    public string Brand { get; set; } = string.Empty;
    [Range(1, 12, ErrorMessage = "Expiration month must be between 1 and 12")]
    public int ExpMonth { get; set; }
    [Range(2024, 2100, ErrorMessage = "Expiration year must be between 2024 and 2100")]
    public int ExpYear { get; set; }
}





