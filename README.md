# payment-notification-service

Payment & Notification Service des Kino-Ticketsystems (Modul 321, Microservices).

## Was der Service macht

Ein Worker ohne HTTP-API, nur über RabbitMQ angebunden:

1. Konsumiert `BookingCreated` aus der Queue `booking.created.v1` (publiziert vom booking-service).
2. Simuliert die Zahlung (`Payment:SimulatedDelayMs`, Standard 1 s) und speichert sie in `payment_db`.
3. Simuliert die Notification (Log-Eintrag "Buchungsbestätigung gesendet an ...").
4. Publiziert `PaymentCompleted` in die Queue `payment.completed.v1` (für den booking-service → Status `Confirmed`).

Der Contract beider Events steht in `cinema-deploy/contracts/asyncapi.yaml`.

## Zuverlässigkeit

- **Idempotenz:** `payments.BookingId` ist unique. Kommt eine Nachricht doppelt, wird nicht zweimal bezahlt.
  Auch zwei Instanzen, die gleichzeitig dieselbe Buchung bekommen, erzeugen nur eine Zahlung.
- **Kein Nachrichtenverlust:** Manuelles Ack erst nach Speichern und Weiterleiten. Stürzt der Service vorher ab,
  stellt RabbitMQ die Nachricht erneut zu. Bei Duplikaten wird `PaymentCompleted` mit derselben `eventId` nochmals
  gesendet, damit es nicht verloren geht (der Empfänger dedupliziert über `eventId`).
- **Ungültige Nachrichten** (kein JSON, keine `bookingId`) werden verworfen statt endlos neu zugestellt.
- **Tolerant Reader:** Es werden nur die benötigten Felder gelesen, unbekannte ignoriert. `customerEmail` ist optional.
- **Ausfall:** Ist der Service gestoppt, nimmt der booking-service weiter Buchungen an. Die Nachrichten warten in
  `booking.created.v1` und werden nach dem Start nachgeholt. Ist RabbitMQ beim Start nicht da, versucht der
  Service alle 5 s erneut zu verbinden.
- **Skalierung:** Prefetch 1, d.h. mehrere Instanzen teilen sich die Nachrichten (Competing Consumers).

## Logging

Einheitliches JSON-Format auf der Konsole. Jeder Log-Eintrag zu einer Nachricht enthält
`CorrelationId`, `BookingId` und `EventId`. Die `correlationId` aus `BookingCreated` wird in `PaymentCompleted`
übernommen.

## Starten

Voraussetzungen: .NET SDK 10, Docker.

1. Infrastruktur aus dem Repo `cinema-deploy` starten (Postgres mit `payment_db`, RabbitMQ):
   ```bash
   docker compose up -d
   ```
2. Service starten:
   ```bash
   dotnet run --project src/PaymentNotificationService
   ```
   Beim Start werden die Migrationen angewendet.

Skalierung lokal zeigen: denselben Befehl in einem zweiten Terminal starten.

Konfiguration (`src/PaymentNotificationService/appsettings.Development.json`):

| Key | Wert lokal |
|---|---|
| `ConnectionStrings:PaymentDb` | `payment_db`, User `payment_user` / `payment_dev` |
| `RabbitMq:Host` / `Port` / `User` / `Password` | `localhost` / `5672` / `cinema_dev` / `cinema_dev_password` |
| `Payment:SimulatedDelayMs` | `1000` (in `appsettings.json`) |

Neue Migration anlegen:

```bash
dotnet tool restore
dotnet ef migrations add <Name> --project src/PaymentNotificationService --output-dir Data/Migrations
```

## Testen ohne booking-service

Im RabbitMQ Management UI (http://localhost:15672) unter Queues → `booking.created.v1` → "Publish message":

```json
{
  "eventId": "aaaaaaaa-0000-0000-0000-000000000001",
  "correlationId": "test-123",
  "data": {
    "bookingId": "bbbbbbbb-0000-0000-0000-000000000001",
    "showtimeId": "33333333-3333-3333-3333-000000000001",
    "seats": ["A1"],
    "totalPrice": 18.50
  }
}
```

Dieselbe Nachricht zweimal senden: Im Log erscheint "ist schon bezahlt (doppelte Nachricht ...)" und
in `payments` steht nur eine Zeile.
