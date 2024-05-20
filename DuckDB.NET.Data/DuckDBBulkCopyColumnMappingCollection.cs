using System;
using System.Collections;
using System.Diagnostics;

namespace DuckDB.NET.Data;

public sealed class DuckDBBulkCopyColumnMappingCollection : CollectionBase {

    private enum MappingSchema {
        Undefined = 0,
        NamesNames = 1,
        NemesOrdinals = 2,
        OrdinalsNames = 3,
        OrdinalsOrdinals = 4,
    }

    private MappingSchema mappingSchema = MappingSchema.Undefined;

    internal DuckDBBulkCopyColumnMappingCollection() 
    {

    }

    public DuckDBBulkCopyColumnMapping this[int index] => (DuckDBBulkCopyColumnMapping)List[index];

    internal bool ReadOnly { get; set; }


    public DuckDBBulkCopyColumnMapping Add(DuckDBBulkCopyColumnMapping bulkCopyColumnMapping) {
        AssertWriteAccess();
        
        Debug.Assert(string.IsNullOrEmpty(bulkCopyColumnMapping.SourceColumn) || bulkCopyColumnMapping._internalSourceColumnOrdinal == -1, "BulkLoadAmbigousSourceColumn");

        if ((string.IsNullOrEmpty(bulkCopyColumnMapping.SourceColumn) && (bulkCopyColumnMapping.SourceOrdinal == -1)) ||
            (string.IsNullOrEmpty(bulkCopyColumnMapping.DestinationColumn) && (bulkCopyColumnMapping.DestinationOrdinal == -1)))
            throw new InvalidOperationException("Bulk column non matching column mapping.");

        InnerList.Add(bulkCopyColumnMapping);

        return bulkCopyColumnMapping;
    }

    public DuckDBBulkCopyColumnMapping Add(string sourceColumn, string destinationColumn) 
    {
        AssertWriteAccess();
        return Add(new(sourceColumn, destinationColumn));
    }

    public DuckDBBulkCopyColumnMapping Add(int sourceColumnIndex, string destinationColumn) 
    {
        AssertWriteAccess();
        return Add(new(sourceColumnIndex, destinationColumn));
    }

    public DuckDBBulkCopyColumnMapping Add(string sourceColumn, int destinationColumnIndex) 
    {
        AssertWriteAccess();
        return Add(new(sourceColumn, destinationColumnIndex));
    }

    public DuckDBBulkCopyColumnMapping Add(int sourceColumnIndex, int destinationColumnIndex) 
    {
        AssertWriteAccess();
        return Add(new(sourceColumnIndex, destinationColumnIndex));
    }

    private void AssertWriteAccess() 
    {
        if (ReadOnly)
            throw new InvalidOperationException("Bulk column mapping is inaccessible.");
    }

    new public void Clear() {
        AssertWriteAccess();
        base.Clear();
    }

    public bool Contains(DuckDBBulkCopyColumnMapping value) => -1 != InnerList.IndexOf(value);

    public void CopyTo(DuckDBBulkCopyColumnMapping[] array, int index) => InnerList.CopyTo(array, index);

    internal void CreateDefaultMapping(int columnCount) 
    {
        for (int i = 0; i < columnCount; i++)
            InnerList.Add(new DuckDBBulkCopyColumnMapping(i, i));
    }

    public int IndexOf(DuckDBBulkCopyColumnMapping value) => InnerList.IndexOf(value);

    public void Insert(int index, DuckDBBulkCopyColumnMapping value) 
    {
        AssertWriteAccess();
        InnerList.Insert(index, value);
    }

    public void Remove(DuckDBBulkCopyColumnMapping value) 
    {
        AssertWriteAccess();
        InnerList.Remove(value);
    }

    new public void RemoveAt(int index) 
    {
        AssertWriteAccess();
        base.RemoveAt(index);
    }

    internal void ValidateCollection() {
        MappingSchema mappingSchema;

        foreach (DuckDBBulkCopyColumnMapping columnMapping in this) 
        {
            if (columnMapping.SourceOrdinal != -1) 
            {
                mappingSchema = columnMapping.DestinationOrdinal != -1 ? MappingSchema.OrdinalsOrdinals : MappingSchema.OrdinalsNames;
            } 
            else 
            {
                mappingSchema = columnMapping.DestinationOrdinal != -1 ? MappingSchema.NemesOrdinals : MappingSchema.NamesNames;
            }

            if (this.mappingSchema == MappingSchema.Undefined) 
            {
                this.mappingSchema = mappingSchema;
            } 
            else 
            {
                if (this.mappingSchema != mappingSchema)
                    throw new InvalidOperationException("Bulk column mapping must be Names or Ordinals only.");
            }
        }
    }
}
