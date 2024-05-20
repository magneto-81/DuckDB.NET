using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;

namespace DuckDB.NET.Data;

public sealed class DuckDBBulkCopy : IDisposable {

    private DuckDBConnection connection;
    private DuckDBTransaction transaction;
    private DuckDBBulkCopyOptions bulkCopyOptions;
    private bool disposed;

    /// <summary>
    /// 
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="bulkCopyOptions"></param>
    internal DuckDBBulkCopy(DuckDBConnection connection, DuckDBBulkCopyOptions bulkCopyOptions) 
    { 
        this.connection = connection;
        this.bulkCopyOptions = bulkCopyOptions;
    }

    internal DuckDBBulkCopy(DuckDBConnection connection, DuckDBBulkCopyOptions bulkCopyOptions, DuckDBTransaction transaction) : this(connection, bulkCopyOptions)
    {
        this.transaction = transaction;
    }

    #region Public Properties

    public string DestinationTableName { get; set; }

    public DuckDBBulkCopyColumnMappingCollection ColumnMappings { get; } = [];

    #endregion

    #region Private Members

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

    private string SystemTypeToDuckDBType(string systemType, byte numericScale, byte numericPrcision) 
    {
        return systemType switch {
            "System.Boolean" => "BOOLEAN",
            "System.Int16" => "SMALLINT",
            "System.Int32" => "INTEGER",
            "System.Int64" => "BIGINT",
            "System.String" => "VARCHAR",
            
            _ => throw new ArgumentOutOfRangeException(systemType),
        };
    }

    private void CreateSchemaOnDestination(IDataReader dataReader) 
    {
        StringBuilder statementBuilder = new(500);
        ArrayList keyList = [];
        

        using var sourceSchemaTable = dataReader.GetSchemaTable();

        var allColumnsTable = ColumnMappings.Count == 0;

        statementBuilder.AppendFormat("CREATE TABLE {0} (\n", DestinationTableName);

        foreach (DataRow row in sourceSchemaTable.Rows) {
            var columnName = row["ColumnName"].ToString();
            var columnOrdinal = row["ColumnOrdinal"].ToString();
            var columnSize = row["ColumnSize"].ToString();
            var numericScale = Convert.ToByte(row["NumericScale"].ToString());
            var numericPrecision = Convert.ToByte(row["NumericPrecision"].ToString());
            var isKey = row["IsKey"] != DBNull.Value && Convert.ToBoolean(row["IsKey"].ToString());
            var dataType = row["DataType"].ToString();
            var allowDbNull = row["AllowDBNull"] != DBNull.Value && Convert.ToBoolean(row["AllowDBNull"].ToString());
            var isIdentity = row["IsIdentity"] != DBNull.Value && Convert.ToBoolean(row["IsIdentity"].ToString());
            var isAutoIncrement = row["IsAutoIncrement"] != DBNull.Value && Convert.ToBoolean(row["IsAutoIncrement"].ToString());

            if (allColumnsTable) 
            {
                statementBuilder.AppendFormat(",\t{0} {1} {2}\n", 
                    columnName, 
                    SystemTypeToDuckDBType(dataType, numericScale, numericPrecision), 
                    allowDbNull ? "NULL" : "NOT NULL");

                if (isKey)
                    keyList.Add(columnName);
            }
            else 
            { 

            }
        }

        statementBuilder.AppendLine(")");

        using var command = connection.CreateCommand();
        command.CommandText = statementBuilder.ToString();
        command.CommandType = CommandType.Text;
        command.Transaction = transaction;

        command.ExecuteNonQuery();
    }

    private void WriteToServerInternal(IDataReader dataReader) 
    { 
        try 
        {
            if (bulkCopyOptions.HasFlag(DuckDBBulkCopyOptions.UseInternalTransaction)) 
            {
                transaction = connection.BeginTransaction();
            }

            CreateSchemaOnDestination(dataReader);
            




            transaction?.Commit();
        }
        catch 
        { 
            transaction?.Rollback();
        }
        finally 
        { 
            
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

    // // TODO: override finalizer only if 'Dispose(bool disposing)' has code to free unmanaged resources
    // ~DuckDBBulkCopy()
    // {
    //     // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
    //     Dispose(disposing: false);
    // }

    #region IDisposable Members

    public void Dispose() 
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        Dispose(disposing: true);
        GC.SuppressFinalize(this);    
    }

    #endregion
}