using System;

namespace DuckDB.NET.Data;

/// <summary>
/// Bitwise flag that specifies one or more options to use with an instance of DuckDBBulkCopy
/// </summary>
[Flags]
public enum DuckDBBulkCopyOptions
{
    /// <summary>
    /// Use the default values for all options.
    /// </summary>
    Default = 0,

    /// <summary>
    /// Preserve source identity values. When not specified, identity values are assigned by the destination.
    /// </summary>
    KeepIdentity = 1,

    /// <summary>
    /// Preserve null values in the destination table regardless of the settings for default values. 
    /// When not specified, null values are replaced by default values where applicable.
    /// </summary>
    KeepNulls = 2,

    /// <summary>
    /// When specified, each batch of the bulk-copy operation will occur within a transaction. 
    /// If you indicate this option and also provide a DuckDBTransaction object to the constructor, an System.ArgumentException occurs.
    /// </summary>
    UseInternalTransaction = 4

}
