namespace PostgresqlSourceConnector.TodosApi.Models;

public class Todo
{
    public int Id { get; set; }
    public string Title { get; set; } = string.Empty;
    public bool Completed { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime? UpdatedAt { get; set; }
}
