﻿using DuckDB.NET.Data.Common;
using DuckDB.NET.Data.DataChunk.Writer;
using DuckDB.NET.Native;
using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace DuckDB.NET.Data;

public class DuckDBAppender : IDisposable
{
    private bool closed;
    private readonly Native.DuckDBAppender nativeAppender;
    private readonly string qualifiedTableName;

    private ulong rowCount;

    private readonly DuckDBLogicalType[] logicalTypes;
    private readonly DuckDBDataChunk dataChunk;
    private readonly VectorDataWriterBase[] vectorWriters;

    internal DuckDBAppender(Native.DuckDBAppender appender, string qualifiedTableName)
    {
        nativeAppender = appender;
        this.qualifiedTableName = qualifiedTableName;

        var columnCount = NativeMethods.Appender.DuckDBAppenderColumnCount(nativeAppender);

        vectorWriters = new VectorDataWriterBase[columnCount];
        logicalTypes = new DuckDBLogicalType[columnCount];
        var logicalTypeHandles = new IntPtr[columnCount];

        for (ulong index = 0; index < columnCount; index++)
        {
            logicalTypes[index] = NativeMethods.Appender.DuckDBAppenderColumnType(nativeAppender, index);
            logicalTypeHandles[index] = logicalTypes[index].DangerousGetHandle();
        }

        dataChunk = NativeMethods.DataChunks.DuckDBCreateDataChunk(logicalTypeHandles, columnCount);
    }

    public IDuckDBAppenderRow CreateRow()
    {
        if (closed)
        {
            throw new InvalidOperationException("Appender is already closed");
        }

        if (rowCount % DuckDBGlobalData.VectorSize == 0)
        {
            AppendDataChunk();

            InitVectorWriters();

            rowCount = 0;
        }

        rowCount++;
        return new DuckDBAppenderRow(qualifiedTableName, vectorWriters, rowCount - 1, dataChunk, nativeAppender);
    }

    public void Close()
    {
        closed = true;

        try
        {
            AppendDataChunk();

            foreach (var logicalType in logicalTypes)
            {
                logicalType.Dispose();
            }

            foreach (var writer in vectorWriters)
            {
                writer?.Dispose();
            }

            dataChunk.Dispose();

            var state = NativeMethods.Appender.DuckDBAppenderClose(nativeAppender);
            if (!state.IsSuccess())
            {
                ThrowLastError(nativeAppender);
            }
        }
        finally
        {
            nativeAppender.Close();
        }
    }

    public void Dispose()
    {
        if (!closed)
        {
            Close();
        }
    }

    private void InitVectorWriters()
    {
        for (long index = 0; index < vectorWriters.LongLength; index++)
        {
            var vector = NativeMethods.DataChunks.DuckDBDataChunkGetVector(dataChunk, index);

            vectorWriters[index]?.Dispose();
            vectorWriters[index] = VectorDataWriterFactory.CreateWriter(vector, logicalTypes[index]);
        }
    }

    private void AppendDataChunk()
    {
        NativeMethods.DataChunks.DuckDBDataChunkSetSize(dataChunk, rowCount);
        var state = NativeMethods.Appender.DuckDBAppendDataChunk(nativeAppender, dataChunk);

        if (!state.IsSuccess())
        {
            ThrowLastError(nativeAppender);
        }

        NativeMethods.DataChunks.DuckDBDataChunkReset(dataChunk);
    }

    [DoesNotReturn]
    [StackTraceHidden]
    internal static void ThrowLastError(Native.DuckDBAppender appender)
    {
        var errorMessage = NativeMethods.Appender.DuckDBAppenderError(appender).ToManagedString(false);

        throw new DuckDBException(errorMessage);
    }
}
