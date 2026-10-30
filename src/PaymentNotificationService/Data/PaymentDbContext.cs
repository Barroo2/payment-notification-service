using Microsoft.EntityFrameworkCore;

namespace PaymentNotificationService.Data;

public class PaymentDbContext(DbContextOptions<PaymentDbContext> options) : DbContext(options)
{
    public DbSet<Payment> Payments => Set<Payment>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Payment>(e =>
        {
            e.ToTable("payments");
            e.Property(p => p.Amount).HasPrecision(10, 2);
            e.Property(p => p.CustomerEmail).HasMaxLength(320);
            e.Property(p => p.CorrelationId).HasMaxLength(100);

            // Idempotenz: pro Buchung genau eine Zahlung, auch bei mehreren Instanzen
            e.HasIndex(p => p.BookingId).IsUnique();
        });
    }
}
