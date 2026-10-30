namespace PaymentNotificationService.Messaging;

// Tolerant Reader: nur die Felder, die dieser Service braucht. Unbekannte Felder werden ignoriert,
// fehlende optionale Felder (z.B. customerEmail) sind null.
public sealed record BookingCreatedV1(Guid EventId, string? CorrelationId, BookingCreatedData? Data);

public sealed record BookingCreatedData(
    Guid BookingId,
    Guid ShowtimeId,
    decimal TotalPrice,
    List<string>? Seats,
    string? CustomerEmail);

// Gleicher Umschlag wie BookingCreatedV1 im booking-service
public sealed record PaymentCompletedV1(
    Guid EventId,
    string EventType,
    int SchemaVersion,
    DateTimeOffset OccurredAtUtc,
    string CorrelationId,
    string Producer,
    PaymentCompletedData Data);

public sealed record PaymentCompletedData(Guid BookingId, Guid PaymentId, decimal Amount);
