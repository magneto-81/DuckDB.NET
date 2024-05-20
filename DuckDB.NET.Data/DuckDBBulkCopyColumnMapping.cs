using System;

namespace DuckDB.NET.Data;

/// <summary>
/// This class helps allows the user to create association between source- and targetcolumns
/// </summary>
public sealed class DuckDBBulkCopyColumnMapping {

    internal string? destinationColumnName;
    internal int destinationColumnOrdinal;
    internal string? sourceColumnName;
    internal int sourceColumnOrdinal;

    // devnote: we don't want the user to detect the columnordinal after WriteToServer call.
    // _sourceColumnOrdinal(s) will be copied to _internalSourceColumnOrdinal when WriteToServer executes.
    internal int _internalDestinationColumnOrdinal;
    internal int _internalSourceColumnOrdinal;   // -1 indicates an undetermined value

    public DuckDBBulkCopyColumnMapping() {
        _internalSourceColumnOrdinal = -1;
    }

    public DuckDBBulkCopyColumnMapping(string sourceColumn, string destinationColumn) {
        SourceColumn = sourceColumn;
        DestinationColumn = destinationColumn;
    }

    public DuckDBBulkCopyColumnMapping(int sourceColumnOrdinal, string destinationColumn) {
        SourceOrdinal = sourceColumnOrdinal;
        DestinationColumn = destinationColumn;
    }

    public DuckDBBulkCopyColumnMapping(string sourceColumn, int destinationOrdinal) {
        SourceColumn = sourceColumn;
        DestinationOrdinal = destinationOrdinal;
    }

    public DuckDBBulkCopyColumnMapping(int sourceColumnOrdinal, int destinationOrdinal) {
        SourceOrdinal = sourceColumnOrdinal;
        DestinationOrdinal = destinationOrdinal;
    }

    public string DestinationColumn {
        get => destinationColumnName ?? string.Empty;

        set 
        {
            destinationColumnOrdinal = _internalDestinationColumnOrdinal = -1;
            destinationColumnName = value;
        }
    }

    public int DestinationOrdinal {
        get => destinationColumnOrdinal;

        set 
        {
            if (value >= 0) {
                destinationColumnName = null;
                destinationColumnOrdinal = _internalDestinationColumnOrdinal = value;
            } else
                throw new IndexOutOfRangeException();
        }
    }

    public string SourceColumn {
        get => sourceColumnName ?? string.Empty;

        set 
        {
            sourceColumnOrdinal = _internalSourceColumnOrdinal = -1;
            sourceColumnName = value;
        }
    }

    public int SourceOrdinal {
        get => sourceColumnOrdinal;

        set 
        {
            if (value >= 0) 
            {
                sourceColumnName = null;
                sourceColumnOrdinal = _internalSourceColumnOrdinal = value;
            } else 
                throw new IndexOutOfRangeException();
        }
    }


}
