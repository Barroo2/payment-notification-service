using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace PaymentNotificationService.Messaging;

public sealed class BookingCreatedConsumer(
    IServiceScopeFactory scopeFactory,
    IConfiguration configuration,
    ILogger<BookingCreatedConsumer> logger) : BackgroundService
{
    // Queue-Namen und Deklaration wie im booking-service (durable, nicht exclusive, kein auto-delete)
    private const string BookingCreatedQueue = "booking.created.v1";
    private const string PaymentCompletedQueue = "payment.completed.v1";

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private IConnection? connection;
    private IChannel? consumeChannel;
    private IChannel? publishChannel;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // RabbitMQ ist beim Start evtl. noch nicht da: so lange versuchen, bis die Verbindung steht.
        // Danach stellt RabbitMQ.Client Verbindung und Consumer bei Ausfällen selbst wieder her.
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ConnectAsync(stoppingToken);
                return;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning("RabbitMQ nicht erreichbar ({Error}), neuer Versuch in 5 Sekunden.", ex.Message);
                await DisposeRabbitAsync();
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }
    }

    private async Task ConnectAsync(CancellationToken ct)
    {
        var factory = new ConnectionFactory
        {
            HostName = configuration["RabbitMq:Host"] ?? throw new InvalidOperationException("RabbitMq:Host is required."),
            Port = configuration.GetValue("RabbitMq:Port", 5672),
            UserName = configuration["RabbitMq:User"] ?? throw new InvalidOperationException("RabbitMq:User is required."),
            Password = configuration["RabbitMq:Password"] ?? throw new InvalidOperationException("RabbitMq:Password is required."),
            ClientProvidedName = "payment-notification-service"
        };

        connection = await factory.CreateConnectionAsync(ct);
        publishChannel = await connection.CreateChannelAsync(
            new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true), ct);
        consumeChannel = await connection.CreateChannelAsync(cancellationToken: ct);

        foreach (var queue in new[] { BookingCreatedQueue, PaymentCompletedQueue })
        {
            await consumeChannel.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: false, arguments: null, cancellationToken: ct);
        }

        // Jede Instanz holt nur 1 Nachricht auf einmal: mehrere Instanzen teilen sich die Arbeit
        await consumeChannel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false, ct);

        var consumer = new AsyncEventingBasicConsumer(consumeChannel);
        consumer.ReceivedAsync += OnMessageAsync;
        await consumeChannel.BasicConsumeAsync(BookingCreatedQueue, autoAck: false, consumer, ct);

        logger.LogInformation("Verbunden mit RabbitMQ, warte auf Nachrichten in {Queue}.", BookingCreatedQueue);
    }

    private async Task OnMessageAsync(object sender, BasicDeliverEventArgs ea)
    {
        var channel = ((AsyncEventingBasicConsumer)sender).Channel;
        var ct = ea.CancellationToken;

        BookingCreatedV1? message;
        try
        {
            message = JsonSerializer.Deserialize<BookingCreatedV1>(ea.Body.Span, JsonOptions);
        }
        catch (JsonException)
        {
            message = null;
        }

        if (message?.Data is not { } data || data.BookingId == Guid.Empty)
        {
            // Nicht lesbare Nachricht: verwerfen statt endlos neu zustellen
            logger.LogError("Ungültige Nachricht {MessageId} in {Queue} wird verworfen.", ea.BasicProperties.MessageId, BookingCreatedQueue);
            await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false, ct);
            return;
        }

        var correlationId = message.CorrelationId ?? ea.BasicProperties.CorrelationId ?? Guid.NewGuid().ToString("N");

        using var _ = logger.BeginScope(
            "CorrelationId={CorrelationId} BookingId={BookingId} EventId={EventId}",
            correlationId, data.BookingId, message.EventId);

        try
        {
            using var scope = scopeFactory.CreateScope();
            var processor = scope.ServiceProvider.GetRequiredService<PaymentProcessor>();
            var payment = await processor.ProcessAsync(message, data, correlationId, ct);

            // Auch bei Duplikaten erneut senden: falls der Service nach dem Speichern, aber vor dem
            // Senden abgestürzt ist, geht das Event so nicht verloren. Gleiche EventId → Empfänger kann deduplizieren.
            await PublishPaymentCompletedAsync(payment, ct);

            // Erst bestätigen, wenn gespeichert und weitergeleitet ist
            await channel.BasicAckAsync(ea.DeliveryTag, multiple: false, ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogError(ex, "Verarbeitung fehlgeschlagen, Nachricht wird erneut zugestellt.");
            await Task.Delay(TimeSpan.FromSeconds(2), ct);
            await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: true, ct);
        }
    }

    private async Task PublishPaymentCompletedAsync(Data.Payment payment, CancellationToken ct)
    {
        var evt = new PaymentCompletedV1(
            EventId: payment.PaymentCompletedEventId,
            EventType: "payment.completed",
            SchemaVersion: 1,
            OccurredAtUtc: payment.PaidAtUtc,
            CorrelationId: payment.CorrelationId,
            Producer: "payment-notification-service",
            Data: new PaymentCompletedData(payment.BookingId, payment.Id, payment.Amount));

        var properties = new BasicProperties
        {
            Persistent = true,
            ContentType = "application/json",
            MessageId = evt.EventId.ToString(),
            CorrelationId = evt.CorrelationId,
            Type = "payment.completed.v1"
        };

        await publishChannel!.BasicPublishAsync(
            exchange: string.Empty,
            routingKey: PaymentCompletedQueue,
            mandatory: true,
            basicProperties: properties,
            body: JsonSerializer.SerializeToUtf8Bytes(evt, JsonOptions),
            cancellationToken: ct);

        logger.LogInformation("PaymentCompleted {EventId} nach {Queue} gesendet.", evt.EventId, PaymentCompletedQueue);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);
        await DisposeRabbitAsync();
    }

    private async Task DisposeRabbitAsync()
    {
        if (consumeChannel is not null) await consumeChannel.DisposeAsync();
        if (publishChannel is not null) await publishChannel.DisposeAsync();
        if (connection is not null) await connection.DisposeAsync();
        consumeChannel = null;
        publishChannel = null;
        connection = null;
    }
}
