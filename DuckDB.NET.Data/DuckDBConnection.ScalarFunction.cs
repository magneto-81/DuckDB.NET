﻿using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Data.Internal;
using DuckDB.NET.Data.Internal.Reader;
using DuckDB.NET.Data.Internal.Writer;
using DuckDB.NET.Data.Reader;
using DuckDB.NET.Data.Writer;
using DuckDB.NET.Native;
using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Data;

partial class DuckDBConnection
{
#if NET6_0_OR_GREATER
    public unsafe void RegisterScalarFunction<T, TResult>(string name, Action<IDuckDBDataReader[], IDuckDBDataWriter, int> action, bool varargs = false)
    {
        RegisterScalarMethod(name, action, DuckDBTypeMap.GetLogicalType<TResult>(), varargs: varargs, DuckDBTypeMap.GetLogicalType<T>());
    }

    public unsafe void RegisterScalarFunction<T1, T2, TResult>(string name, Action<IDuckDBDataReader[], IDuckDBDataWriter, int> action)
    {
        RegisterScalarMethod(name, action, DuckDBTypeMap.GetLogicalType<TResult>(), false, DuckDBTypeMap.GetLogicalType<T1>(), DuckDBTypeMap.GetLogicalType<T2>());
    }

    public unsafe void RegisterScalarFunction<T1, T2, T3, TResult>(string name, Action<IDuckDBDataReader[], IDuckDBDataWriter, int> action)
    {
        RegisterScalarMethod(name, action, DuckDBTypeMap.GetLogicalType<TResult>(), false,
                       DuckDBTypeMap.GetLogicalType<T1>(),
                              DuckDBTypeMap.GetLogicalType<T2>(),
                              DuckDBTypeMap.GetLogicalType<T3>());
    }

    public unsafe void RegisterScalarFunction<T1, T2, T3, T4, TResult>(string name, Action<IDuckDBDataReader[], IDuckDBDataWriter, int> action)
    {
        RegisterScalarMethod(name, action, DuckDBTypeMap.GetLogicalType<TResult>(), varargs: false,
                              DuckDBTypeMap.GetLogicalType<T1>(),
                              DuckDBTypeMap.GetLogicalType<T2>(),
                              DuckDBTypeMap.GetLogicalType<T3>(),
                              DuckDBTypeMap.GetLogicalType<T4>());
    }

    private unsafe void RegisterScalarMethod(string name, Action<IDuckDBDataReader[], IDuckDBDataWriter, int> action, DuckDBLogicalType returnType, bool varargs, params DuckDBLogicalType[] parameterTypes)
    {
        var function = NativeMethods.ScalarFunction.DuckDBCreateScalarFunction();
        NativeMethods.ScalarFunction.DuckDBScalarFunctionSetName(function, name.ToUnmanagedString());

        if (varargs)
        {
            if (parameterTypes.Length != 1)
            {
                throw new InvalidOperationException("Cannot use varargs with multiple parameters");
            }

            NativeMethods.ScalarFunction.DuckDBScalarFunctionSetVarargs(function, parameterTypes[0]);
        }
        else
        {
            foreach (var type in parameterTypes)
            {
                NativeMethods.ScalarFunction.DuckDBScalarFunctionAddParameter(function, type);
            }
        }

        NativeMethods.ScalarFunction.DuckDBScalarFunctionSetReturnType(function, returnType);
        NativeMethods.ScalarFunction.DuckDBScalarFunctionSetFunction(function, &ScalarFunctionCallback);

        var info = new ScalarFunctionInfo(parameterTypes, returnType, action, varargs);

        NativeMethods.ScalarFunction.DuckDBScalarFunctionSetExtraInfo(function, info.ToHandle(), &DestroyExtraInfo);

        var state = NativeMethods.ScalarFunction.DuckDBRegisterScalarFunction(NativeConnection, function);

        NativeMethods.ScalarFunction.DuckDBDestroyScalarFunction(out function);

        if (!state.IsSuccess())
        {
            throw new InvalidOperationException("Error registering user defined scalar function");
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void ScalarFunctionCallback(IntPtr info, IntPtr chunk, IntPtr vector)
    {
        var dataChunk = new DuckDBDataChunk(chunk);

        var chunkSize = (int)NativeMethods.DataChunks.DuckDBDataChunkGetSize(dataChunk);
        var handle = GCHandle.FromIntPtr(NativeMethods.ScalarFunction.DuckDBScalarFunctionGetExtraInfo(info));

        if (handle.Target is not ScalarFunctionInfo functionInfo)
        {
            throw new InvalidOperationException("User defined scalar function execution failed. Function extra info is null");
        }

        VectorDataReaderBase[] readers;

        if (functionInfo.Varargs)
        {
            var columnCount = NativeMethods.DataChunks.DuckDBDataChunkGetColumnCount(dataChunk);
            readers = new VectorDataReaderBase[columnCount];
            
            for (var index = 0; index < columnCount; index++)
            {
                readers[index] = VectorDataReaderFactory.CreateReader(NativeMethods.DataChunks.DuckDBDataChunkGetVector(dataChunk, index), functionInfo.ParameterTypes[0], "");
            }
        }
        else
        {
            readers = functionInfo.ParameterTypes.Select((type, index) => VectorDataReaderFactory.CreateReader(NativeMethods.DataChunks.DuckDBDataChunkGetVector(dataChunk, index), type, "")).ToArray();
        }

        var writer = VectorDataWriterFactory.CreateWriter(vector, functionInfo.ReturnType);

        functionInfo.Action(readers, writer, chunkSize);
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static unsafe void DestroyExtraInfo(IntPtr pointer) => pointer.FreeHandle();
#endif
}