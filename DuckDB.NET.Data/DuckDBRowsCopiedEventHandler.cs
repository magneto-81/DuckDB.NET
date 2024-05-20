namespace DuckDB.NET.Data;

/// <summary>
/// Represents the method that handles the DuckDBBulkCopy.DuckDBRowsCopied event of a DuckDBBulkCopy.
/// </summary>
/// <param name="sender">The source of the event.</param>
/// <param name="e">A DuckDBRowsCopiedEventArgs object that contains the event data.</param>
public delegate void DuckDBRowsCopiedEventHandler(object sender, DuckDBRowsCopiedEventArgs e);