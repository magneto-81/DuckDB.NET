using System;

namespace DuckDB.NET.Data;

//
// Summary:
//     Represents the set of arguments passed to the System.Data.SqlClient.SqlRowsCopiedEventHandler.
public class DuckDBRowsCopiedEventArgs : EventArgs {

    /// <summary>
    /// Creates a new instance of the DuckDBRowsCopiedEventArgs object.
    /// </summary>
    /// <param name="rowsCopied">An System.Int64 that indicates the number of rows copied during the current bulk copy operation.</param>
    public DuckDBRowsCopiedEventArgs(long rowsCopied) 
    { 
        RowsCopied = rowsCopied;
    }

    /// <summary>
    /// Gets or sets a value that indicates whether the bulk copy operation should be aborted.
    /// </summary>
    /// <returns>true if the bulk copy operation should be aborted; otherwise false.</returns>
    public bool Abort { get; set; }

    /// <summary>
    /// Gets a value that returns the number of rows copied during the current bulk copy operations.
    /// </summary>
    /// <returns>int that returns the number of rows copied.</returns>
    public long RowsCopied { get; }
}