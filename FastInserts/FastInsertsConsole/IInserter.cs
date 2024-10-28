namespace FastInsertsConsole;

internal interface IInserter : IDisposable
{
    Task PrepareAsync();
    Task InsertAsync();
}
