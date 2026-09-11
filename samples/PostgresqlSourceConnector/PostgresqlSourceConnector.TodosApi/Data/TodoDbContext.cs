using Microsoft.EntityFrameworkCore;
using PostgresqlSourceConnector.TodosApi.Models;

namespace PostgresqlSourceConnector.TodosApi.Data;

public class TodoDbContext(DbContextOptions<TodoDbContext> options) : DbContext(options)
{
    public DbSet<Todo> Todos => Set<Todo>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<Todo>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Title).IsRequired().HasMaxLength(200);
            entity.Property(e => e.CreatedAt).HasColumnType("timestamp with time zone");
            entity.Property(e => e.UpdatedAt).HasColumnType("timestamp with time zone");
        });
    }
}
