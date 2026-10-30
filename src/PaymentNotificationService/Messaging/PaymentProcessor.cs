using Microsoft.EntityFrameworkCore;
using Npgsql;
using PaymentNotificationService.Data;

namespace PaymentNotificationService.Messaging;

public sealed class PaymentProcessor(
    PaymentDbContext db,
    IConfiguration configuration,
    ILogger<PaymentProcessor> logger)
{
    // Liefert die Zahlung zur Buchung: neu angelegt oder, bei doppelten Nachrichten, die bestehende
    public async Task<Payment> ProcessAsync(BookingCreatedV1 message, BookingCreatedData data, string correlationId, CancellationToken ct)
    {
        var existing = await db.Payments.AsNoTracking().FirstOrDefaultAsync(p => p.BookingId == data.BookingId, ct);
        if (existing is not null)
        {
            logger.LogInformation(
                "Buchung {BookingId} ist schon bezahlt (doppelte Nachricht {EventId}), keine zweite Zahlung.",
                data.BookingId, message.EventId);
            return existing;
        }

        // Zahlung simulieren
        await Task.Delay(configuration.GetValue("Payment:SimulatedDelayMs", 1000), ct);

        var payment = new Payment
        {
            Id = Guid.NewGuid(),
            BookingId = data.BookingId,
            BookingCreatedEventId = message.EventId,
            PaymentCompletedEventId = Guid.NewGuid(),
            Amount = data.TotalPrice,
            CustomerEmail = data.CustomerEmail,
            CorrelationId = correlationId,
            PaidAtUtc = DateTimeOffset.UtcNow
        };
        db.Payments.Add(payment);

        try
        {
            await db.SaveChangesAsync(ct);
        }
        catch (DbUpdateException ex) when (ex.InnerException is PostgresException { SqlState: PostgresErrorCodes.UniqueViolation })
        {
            // Eine andere Instanz hat dieselbe Buchung gleichzeitig verarbeitet
            logger.LogInformation("Buchung {BookingId} wurde parallel von einer anderen Instanz bezahlt.", data.BookingId);
            db.ChangeTracker.Clear();
            return await db.Payments.AsNoTracking().FirstAsync(p => p.BookingId == data.BookingId, ct);
        }

        logger.LogInformation(
            "Zahlung {PaymentId} über {Amount} für Buchung {BookingId} erfolgreich.",
            payment.Id, payment.Amount, payment.BookingId);

        // Notification simulieren
        logger.LogInformation(
            "Notification: Buchungsbestätigung für {BookingId} gesendet an {Recipient}.",
            payment.BookingId, payment.CustomerEmail ?? "(keine E-Mail im Event)");

        return payment;
    }
}
