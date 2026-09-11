using Microsoft.EntityFrameworkCore;
using PostgresqlSourceConnector.TodosApi.Data;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceDefaults();
builder.Services.AddOpenApi();

// Swagger UI
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Register TodoDbContext with Aspire-managed PostgreSQL connection
// The connection string name "todos" matches the database name from AppHost.cs
builder.AddNpgsqlDbContext<TodoDbContext>("todos");

var app = builder.Build();

// Enable Swagger UI middleware
app.UseSwagger();
app.UseSwaggerUI();

// --- CRUD Endpoints ---

// GET /todos - List all todos
app.MapGet("/todos", async (TodoDbContext db) => await db.Todos.ToListAsync());

// GET /todos/{id} - Get a single todo by ID
app.MapGet(
    "/todos/{id}",
    async (int id, TodoDbContext db) =>
    {
        var todo = await db.Todos.FindAsync(id);
        return todo is not null ? Results.Ok(todo) : Results.NotFound();
    }
);

// POST /todos - Create a new todo
app.MapPost(
    "/todos",
    async (TodoCreateRequest request, TodoDbContext db) =>
    {
        var todo = new PostgresqlSourceConnector.TodosApi.Models.Todo
        {
            Title = request.Title,
            Completed = false,
            CreatedAt = DateTime.UtcNow,
        };

        db.Todos.Add(todo);
        await db.SaveChangesAsync();

        return Results.Created($"/todos/{todo.Id}", todo);
    }
);

// PUT /todos/{id} - Update an existing todo
app.MapPut(
    "/todos/{id}",
    async (int id, TodoUpdateRequest request, TodoDbContext db) =>
    {
        var todo = await db.Todos.FindAsync(id);
        if (todo is null)
            return Results.NotFound();

        todo.Title = request.Title;
        todo.Completed = request.Completed;
        todo.UpdatedAt = DateTime.UtcNow;

        await db.SaveChangesAsync();
        return Results.Ok(todo);
    }
);

// PATCH /todos/{id}/toggle - Toggle completed status
app.MapPatch(
    "/todos/{id}/toggle",
    async (int id, TodoDbContext db) =>
    {
        var todo = await db.Todos.FindAsync(id);
        if (todo is null)
            return Results.NotFound();

        todo.Completed = !todo.Completed;
        todo.UpdatedAt = DateTime.UtcNow;

        await db.SaveChangesAsync();
        return Results.Ok(todo);
    }
);

// DELETE /todos/{id} - Delete a todo
app.MapDelete(
    "/todos/{id}",
    async (int id, TodoDbContext db) =>
    {
        var todo = await db.Todos.FindAsync(id);
        if (todo is null)
            return Results.NotFound();

        db.Todos.Remove(todo);
        await db.SaveChangesAsync();
        return Results.NoContent();
    }
);

app.MapGet("/", () => "Todos API - use /todos endpoint");

app.MapOpenApi();
app.UseSwaggerUI(options => options.SwaggerEndpoint("/openapi/v1.json", "v1"));

app.Run();

// --- Request DTOs ---

public record TodoCreateRequest(string Title);

public record TodoUpdateRequest(string Title, bool Completed);
