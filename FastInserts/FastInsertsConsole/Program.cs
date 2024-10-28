using FastInsertsConsole;

int targetInsertCount = 2_000_000;
string connectionString = "Server=localhost;Database=ForTests;Trusted_Connection=True;Max Pool Size=200;";

var settings = AskForSettings(connectionString);
if (!settings.HasValue)
{
    Console.WriteLine("Invalid input");
    Console.ReadLine();
    return;
}

// Prepare DB
using (var prepareInserter = settings.Value.Inserter())
    await prepareInserter.PrepareAsync();

// Do inserts
List<Task> workers = new();
var startedAt = DateTime.UtcNow;
var remainingCounter = new RemainingCounter(targetInsertCount);
for (int i = 0; i < settings.Value.WorkerCount; i++)
    workers.Add(InsertAsync(settings.Value.Inserter, remainingCounter));

while (remainingCounter.Remaining > 0)
{
    Console.WriteLine($"{DateTime.UtcNow:dd.MM.yyyy HH:mm:ss} Inserted: {targetInsertCount - remainingCounter.Remaining}, per thread: {(int)(targetInsertCount / (DateTime.UtcNow - startedAt).TotalSeconds / settings.Value.WorkerCount)}");
    await Task.Delay(1000);
}

await Task.WhenAll(workers);
var completedAt = DateTime.UtcNow;

// Print summary
Console.WriteLine($"""
    Elapsed time in seconds: {(int)(completedAt - startedAt).TotalSeconds}
    Inserts per second: {targetInsertCount / (completedAt - startedAt).TotalSeconds}
    Inserts per second per thread: {targetInsertCount / (completedAt - startedAt).TotalSeconds / settings.Value.WorkerCount}
    """);
Console.ReadLine();


async Task InsertAsync(Func<IInserter> inserterCreator, RemainingCounter counter)
{
    using var inserter = inserterCreator();
    while (counter.TryDecrement())
    {
        try
        {
            await inserter.InsertAsync();
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex);
            throw;
        }
    }
}

(int WorkerCount, Func<IInserter> Inserter)? AskForSettings(string connectionString)
{
    Console.WriteLine("""
    Select inserter and thread count([1,256]), ex. "B 256":
        - B - autoicnremented id
        - O - autoicnremented id + optimize for sequential identity
        - G - guid as id
        - S - id from sequence
    """);

    var inputParts = (Console.ReadLine() ?? string.Empty).Split(' ');
    if (inputParts.Length != 2 || !int.TryParse(inputParts[1], out var workerCount) || workerCount <= 0 || workerCount > 256)
        return null;

    Func<IInserter>? creator = inputParts[0].ToUpperInvariant() switch
    {
        "B" => () => new BasicInserter(connectionString),
        "O" => () => new OptimizeForSequentialIdInserter(connectionString),
        "G" => () => new GuidIdInserter(connectionString),
        "S" => () => new IdFromSequenceInserter(connectionString),
        _ => null
    };
    if (creator == null)
        return null;
    return (workerCount, creator);
}

class RemainingCounter
{
    private object _lock = new();
    private int _counter;

    public int Remaining => _counter;

    public RemainingCounter(int counter)
    {
        _counter = counter;
    }

    public bool TryDecrement()
    {
        lock (_lock)
        {
            if (_counter <= 0)
                return false;
            _counter--;
            return true;
        }
    }
}