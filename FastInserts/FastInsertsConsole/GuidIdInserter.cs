﻿using System.Data.SqlClient;

namespace FastInsertsConsole;

internal class GuidIdInserter : IInserter
{
    private SqlConnection _sqlConnection;
    private SqlCommand? _insertCommand;

    public GuidIdInserter(string connectionString)
    {
        _sqlConnection = new SqlConnection(connectionString);
    }

    public async Task PrepareAsync()
    {
        await EnsureConnectionOpenedAsync();

        await using var dropCommand = _sqlConnection.CreateCommand();
        dropCommand.CommandText = "DROP TABLE IF EXISTS [dbo].[TestAutoIncrement]";
        await dropCommand.ExecuteNonQueryAsync();

        await using var createCommand = _sqlConnection.CreateCommand();
        createCommand.CommandText = """
            CREATE TABLE [dbo].[TestAutoIncrement](
            	[Id] [UniqueIdentifier] NOT NULL,
            	[SomeData] [nvarchar](100) NOT NULL,
            	CONSTRAINT [PK_TestAutoIncrement] PRIMARY KEY CLUSTERED ([Id] ASC)
            )
            """;
        await createCommand.ExecuteNonQueryAsync();
    }

    public async Task InsertAsync()
    {
        await EnsureConnectionOpenedAsync();
        if (_insertCommand == null)
        {
            _insertCommand = _sqlConnection.CreateCommand();
            _insertCommand.CommandText = "INSERT INTO [dbo].[TestAutoIncrement] (Id, [SomeData]) VALUES (@Id, @SomeData)";
            _insertCommand.Parameters.Add(new SqlParameter("@SomeData", System.Data.SqlDbType.NVarChar));
            _insertCommand.Parameters.Add(new SqlParameter("@Id", System.Data.SqlDbType.UniqueIdentifier));
            _insertCommand.CommandTimeout = 300;
        }

        _insertCommand.Parameters["@SomeData"].Value = Helpers.GenerateRandomString(20, 100);
        _insertCommand.Parameters["@Id"].Value = Guid.NewGuid();
        await _insertCommand.ExecuteNonQueryAsync();
    }

    public void Dispose()
    {
        _sqlConnection.Dispose();
    }

    private async Task EnsureConnectionOpenedAsync()
    {
        if (_sqlConnection.State != System.Data.ConnectionState.Open)
            await _sqlConnection.OpenAsync();
    }

}
