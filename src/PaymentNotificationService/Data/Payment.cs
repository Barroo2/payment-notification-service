namespace PaymentNotificationService.Data;

public class Payment
{
    public Guid Id { get; set; }
    public Guid BookingId { get; set; }
    public Guid BookingCreatedEventId { get; set; }

    // Fest gespeichert, damit ein erneut gesendetes PaymentCompleted dieselbe EventId hat
    public Guid PaymentCompletedEventId { get; set; }

    public decimal Amount { get; set; }
    public string? CustomerEmail { get; set; }
    public required string CorrelationId { get; set; }
    public DateTimeOffset PaidAtUtc { get; set; }
}
