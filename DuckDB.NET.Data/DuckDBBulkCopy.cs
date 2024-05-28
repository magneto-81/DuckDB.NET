using System;
using System.Collections.Generic;
using System.Data;

namespace DuckDB.NET.Data;

public sealed class DuckDBBulkCopy : IDisposable {

    private readonly DuckDBConnection connection;
    private readonly DuckDBBulkCopyOptions bulkCopyOptions;

    private DuckDBTransaction? transaction;

    private DuckDBRowsCopiedEventHandler? rowsCopiedEventHandler;

    private string destinationTableName;
    
    private bool disposed;


    /// <summary>
    /// 
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="bulkCopyOptions"></param>
    internal DuckDBBulkCopy(DuckDBConnection connection, DuckDBBulkCopyOptions bulkCopyOptions) 
    { 
        this.connection = connection!;
        this.bulkCopyOptions = bulkCopyOptions;
    }

    internal DuckDBBulkCopy(DuckDBConnection connection, DuckDBBulkCopyOptions bulkCopyOptions, DuckDBTransaction transaction) : this(connection, bulkCopyOptions)
    {
        if (transaction != null && bulkCopyOptions.HasFlag(DuckDBBulkCopyOptions.UseInternalTransaction))
            throw new ArgumentException("Must not specify DuckDBBulkCopyOption.UseInternalTransaction and pass an external Transaction at the same time.");

        this.transaction = transaction ?? throw new ArgumentNullException(nameof(transaction));
    }

    #region Public Properties

    public string? DestinationTableName 
    {
        get => destinationTableName;

        set 
        {
            if (value == null)
                throw new ArgumentNullException(nameof(DestinationTableName));
            else if (value.Length == 0)
                throw new ArgumentOutOfRangeException(nameof(DestinationTableName));

            destinationTableName = value;
        }
    }

    static (string? SchemaName, string TableName) GetDestinationCoords(string value) {

        var startQuoting = "\"";
        var endTableQuoting = "\".";

        if (value.StartsWith(startQuoting)) 
        {
            var endTableQuotingIndex = value.IndexOf(endTableQuoting);

            if (endTableQuotingIndex == -1) 
            {
                return (null, 
                        value.Substring(1, value.Length - 1));
            }

            var schemaName = value.Substring(1, endTableQuotingIndex - 1);
            var tableName = value.Substring(endTableQuotingIndex + endTableQuoting.Length, value.Length - schemaName.Length - endTableQuoting.Length-1);

            return (schemaName, tableName.Trim(['"']));

        } 
        else 
        {
            return (null, value);
        }
    }


    public int NotifyAfter { get; set; }

    public DuckDBBulkCopyColumnMappingCollection ColumnMappings { get; } = [];

    public event DuckDBRowsCopiedEventHandler DuckDBRowsCopied 
    {
        add => rowsCopiedEventHandler += value;
        remove => rowsCopiedEventHandler -= value;
    }

    #endregion

    #region Private Members

    private void OnRowsCopied(DuckDBRowsCopiedEventArgs value) 
    {
        rowsCopiedEventHandler?.Invoke(this, value);
    }

    private void Dispose(bool disposing) {
        if (!disposed) 
        {
            if (disposing) 
            {
                if (bulkCopyOptions.HasFlag(DuckDBBulkCopyOptions.UseInternalTransaction) &&
                    connection.Transaction != null) 
                {
                    connection.Transaction.Commit();
                    connection.Transaction.Dispose();

                    connection.Transaction = null;
                }
            }

            // TODO: free unmanaged resources (unmanaged objects) and override finalizer
            // TODO: set large fields to null
            disposed = true;
        }
    }

    private void NormalizeColumnMappings(IDataReader dataReader, (string? SchemaName, string TableName) coords) 
    {
        var destinationMap = GetDestinationTableColumnMetadata(coords);

        foreach (DuckDBBulkCopyColumnMapping columnMapping in ColumnMappings) 
        {
            // Working only with column ordinal
            if (!string.IsNullOrEmpty(columnMapping.SourceColumn))
                columnMapping.SourceOrdinal = dataReader.GetOrdinal(columnMapping.SourceColumn);

            if (!string.IsNullOrEmpty(columnMapping.DestinationColumn))
                columnMapping.DestinationOrdinal = destinationMap[columnMapping.DestinationColumn];
        }
    }

    private SortedList<string, int> GetDestinationTableColumnMetadata((string? SchemaName, string TableName) coords) 
    {
        SortedList<string, int> destinationTableMapping = new(10);

        using var command = connection.CreateCommand();

        command.CommandText = !string.IsNullOrEmpty(coords.SchemaName)
            ? $"SELECT column_name, column_index FROM duckdb_columns WHERE schema_name='{coords.SchemaName}' AND table_name='{coords.TableName}'"
            : $"SELECT column_name, column_index FROM duckdb_columns WHERE table_name='{coords.TableName}'";

        using var reader = command.ExecuteReader();

        while (reader.Read()) {
            var columnName = reader.IsDBNull(0) ? string.Empty : reader.GetString(0);
            var columnOrdinal = reader.IsDBNull(1) ? -1 : (reader.GetInt32(1)) - 1;

            destinationTableMapping.Add(columnName, columnOrdinal);
        }

        return destinationTableMapping;

    }

    private void WriteToServerInternal(IDataReader dataReader) 
    {
        if (bulkCopyOptions.HasFlag(DuckDBBulkCopyOptions.UseInternalTransaction))
            transaction = connection.BeginTransaction();

        try 
        {
            int rowsCopied = 0;
            int rowsUntilNotification = NotifyAfter;

            var coords = GetDestinationCoords(destinationTableName);

            if (ColumnMappings.Count == 0) 
            {
                ColumnMappings.CreateDefaultMapping(dataReader.FieldCount);
            }
            else 
            {
                NormalizeColumnMappings(dataReader, coords);
            }

            using (var appender = connection.CreateAppender(coords.SchemaName, coords.TableName))
            {
                while (dataReader.Read()) 
                {
                    var row = appender.CreateRow();

                    object[] values = new object[dataReader.FieldCount];

                    dataReader.GetValues(values);

                    foreach(DuckDBBulkCopyColumnMapping columnMapping in ColumnMappings) 
                    {
                        row.InsertValue(values[columnMapping.SourceOrdinal], columnMapping.DestinationOrdinal);
                    }

                    row.EndRow();

                    rowsCopied++;

                    if(NotifyAfter > 0) 
                    {
                        if (rowsUntilNotification > 0)
                            if (--rowsUntilNotification == 0) 
                            {
                                OnRowsCopied(new(rowsCopied));
                                rowsUntilNotification = NotifyAfter;
                            }                                
                    }
                }
            }

            transaction?.Commit();
        }
        catch
        { 
            transaction?.Rollback();
            throw;
        }
    }

#endregion

    #region Public Members

    public void WriteToServer(IDataReader dataReader) => WriteToServerInternal(dataReader);

    public void WriteToServer(DataTable tableData) 
    {
        using var dataReader = tableData.CreateDataReader();
        WriteToServerInternal(dataReader);
    }

    #endregion

    #region IDisposable Members

    public void Dispose() 
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        Dispose(disposing: true);
        GC.SuppressFinalize(this);    
    }

    #endregion
}