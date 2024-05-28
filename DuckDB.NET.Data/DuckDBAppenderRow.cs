using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Data.Internal.Writer;
using DuckDB.NET.Native;
using System;
using System.Collections;
using System.Data;
using System.Numerics;

namespace DuckDB.NET.Data;

public class DuckDBAppenderRow
{
    private int columnIndex = 0;
    private readonly string qualifiedTableName;
    private readonly VectorDataWriterBase[] vectorWriters;
    private readonly int rowIndex;

    private BitArray columnCompiledArray;

    private bool insertMode = true;

    internal DuckDBAppenderRow(string qualifiedTableName, VectorDataWriterBase[] vectorWriters, ulong rowIndex)
    {
        this.qualifiedTableName = qualifiedTableName;
        this.vectorWriters = vectorWriters;
        this.rowIndex = (int)rowIndex;

        this.columnCompiledArray = new(vectorWriters.Length, false);
    }

    public void EndRow()
    {
        if (insertMode) 
        {
            for (int ordinal = 0; ordinal < vectorWriters.Length; ordinal++)
                if (!columnCompiledArray[ordinal])
                    InsertValue(null, ordinal);
        } 
        else 
        {
            if (columnIndex < vectorWriters.Length) 
            {
                throw new InvalidOperationException($"The table {qualifiedTableName} has {vectorWriters.Length} columns but you specified only {columnIndex} values");
            }
        }
    }

    public DuckDBAppenderRow InsertValue(object? value, int columnOrdinal) {

        if (value.IsNull())
            InsertValueInternal((int?)null, columnOrdinal);
        else if (value is bool)
            InsertValueInternal((bool)value, columnOrdinal);
        else if (value is BigInteger) 
            InsertValueInternal((BigInteger)value, columnOrdinal);
        else if (value is long)
            InsertValueInternal((long)value, columnOrdinal);
        else if (value is int)
            InsertValueInternal((int)value, columnOrdinal);
        else if (value is short)
            InsertValueInternal((short)value, columnOrdinal);
        else if (value is ulong)
            InsertValueInternal((ulong)value, columnOrdinal);
        else if (value is uint)
            InsertValueInternal((uint)value, columnOrdinal);
        else if (value is ushort)
            InsertValueInternal((ushort)value, columnOrdinal);
        else if (value is byte)
            InsertValueInternal((byte)value, columnOrdinal);
        else if (value is byte[])
#if NET6_0_OR_GREATER
            InsertValueInternal((byte[])value, columnOrdinal);
#else
            throw new NotSupportedException();
#endif
        else if (value is string)
            InsertValueInternal((string)value, columnOrdinal);
#if NET6_0_OR_GREATER
        else if (value is DateOnly)
            InsertValueInternal((DateOnly)value, columnOrdinal);
        else if (value is TimeOnly)
            InsertValueInternal((TimeOnly)value, columnOrdinal);
#endif
        else if (value is DuckDBDateOnly)
            InsertValueInternal((DuckDBDateOnly)value, columnOrdinal);
        else if (value is DuckDBTimeOnly)
            InsertValueInternal((DuckDBTimeOnly)value, columnOrdinal);
        else if (value is DateTime)
            InsertValueInternal((DateTime)value, columnOrdinal);
        else if (value is DateTimeOffset)
            InsertValueInternal((DateTimeOffset)value, columnOrdinal);
        else if (value is TimeSpan)
            InsertValueInternal((TimeSpan)value, columnOrdinal);
        else if (value is decimal)
            InsertValueInternal((decimal)value, columnOrdinal);
        else if (value is double)
            InsertValueInternal((double)value, columnOrdinal);
        else if (value is float)
            InsertValueInternal((float)value, columnOrdinal);
        else if (value is Guid)
            InsertValueInternal((Guid)value, columnOrdinal);
        else
            throw new NotImplementedException(value?.GetType().FullName);

        return this;
    }

    public DuckDBAppenderRow AppendNullValue() => AppendValueInternal<int?>(null); //Doesn't matter what type T we pass to Append when passing null.

    public DuckDBAppenderRow AppendValue(bool? value) => AppendValueInternal(value);

#if NET6_0_OR_GREATER

    public DuckDBAppenderRow AppendValue(byte[]? value) => AppendSpan(value);

    public DuckDBAppenderRow AppendValue(Span<byte> value) => AppendSpan(value);
#endif

    public DuckDBAppenderRow AppendValue(string? value) => AppendValueInternal(value);

    public DuckDBAppenderRow AppendValue(decimal? value) => AppendValueInternal(value);

    public DuckDBAppenderRow AppendValue(Guid? value) => AppendValueInternal(value);

    public DuckDBAppenderRow AppendValue(BigInteger? value) => AppendValueInternal(value);

    #region Append Signed Int

    public DuckDBAppenderRow AppendValue(sbyte? value) => AppendValueInternal(value);

    public DuckDBAppenderRow AppendValue(short? value) => AppendValueInternal(value);

    public DuckDBAppenderRow AppendValue(int? value) => AppendValueInternal(value);

    public DuckDBAppenderRow AppendValue(long? value) => AppendValueInternal(value);

    #endregion

    #region Append Unsigned Int

    public DuckDBAppenderRow AppendValue(byte? value) => AppendValueInternal(value);

    public DuckDBAppenderRow AppendValue(ushort? value) => AppendValueInternal(value);

    public DuckDBAppenderRow AppendValue(uint? value) => AppendValueInternal(value);

    public DuckDBAppenderRow AppendValue(ulong? value) => AppendValueInternal(value);

    #endregion

    #region Append Float

    public DuckDBAppenderRow AppendValue(float? value) => AppendValueInternal(value);

    public DuckDBAppenderRow AppendValue(double? value) => AppendValueInternal(value);

    #endregion

    #region Append Temporal
#if NET6_0_OR_GREATER
    public DuckDBAppenderRow AppendValue(DateOnly? value) => AppendValueInternal(value);

    public DuckDBAppenderRow AppendValue(TimeOnly? value) => AppendValueInternal(value);
#endif

    public DuckDBAppenderRow AppendValue(DuckDBDateOnly? value) => AppendValueInternal(value);

    public DuckDBAppenderRow AppendValue(DuckDBTimeOnly? value) => AppendValueInternal(value);


    public DuckDBAppenderRow AppendValue(DateTime? value) => AppendValueInternal(value);

    public DuckDBAppenderRow AppendValue(DateTimeOffset? value) => AppendValueInternal(value);

    public DuckDBAppenderRow AppendValue(TimeSpan? value)
    {
        return AppendValueInternal(value);
    }

    #endregion

    private DuckDBAppenderRow AppendValueInternal<T>(T? value)
    {
        CheckColumnAccess();

        vectorWriters[columnIndex].AppendValue(value, rowIndex);

        columnIndex++;

        return this;
    }

    private DuckDBAppenderRow InsertValueInternal<T>(T? value, int columnOrdinal) 
    {
        insertMode = true;

        CheckColumnAccess(columnOrdinal);

        vectorWriters[columnOrdinal].AppendValue(value, rowIndex);

        columnCompiledArray[columnOrdinal] = true;

        return this;
    }

#if NET6_0_OR_GREATER
    private unsafe DuckDBAppenderRow AppendSpan(Span<byte> val)
    {
        if (val == null)
        {
            return AppendNullValue();
        }

        CheckColumnAccess();

        fixed (byte* pSource = val)
        {
            vectorWriters[columnIndex].AppendBlob(pSource, val.Length, rowIndex);
        }

        columnIndex++;
        return this;
    }

#endif

    private void CheckColumnAccess()
    {
        if (columnIndex >= vectorWriters.Length)
        {
            throw new IndexOutOfRangeException($"The table {qualifiedTableName} has {vectorWriters.Length} columns but you are trying to append value for column {columnIndex + 1}");
        }
    }

    private void CheckColumnAccess(int columnOrdinal) 
    {
        if (columnOrdinal >= vectorWriters.Length) 
        {
            throw new IndexOutOfRangeException($"The table {qualifiedTableName} has {vectorWriters.Length} columns but you are trying to append value for column {columnOrdinal + 1}");
        }
    }
}
