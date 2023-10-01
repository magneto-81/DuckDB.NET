﻿using System;
using System.Collections.Generic;
using System.Linq;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test;

public class DuckDBDataReaderListTests
{
    [Fact]
    public void ReadListOfIntegers()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        using var duckDbCommand = connection.CreateCommand();
        duckDbCommand.CommandText = "SELECT [1, 2, 3];";
        using var reader = duckDbCommand.ExecuteReader();
        reader.Read();
        var list = reader.GetFieldValue<List<int>>(0);
        list.Should().BeEquivalentTo(new List<int> { 1, 2, 3 });
    }

    [Fact]
    public void ReadMultipleListOfIntegers()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        using var duckDbCommand = connection.CreateCommand();
        duckDbCommand.CommandText = "Select * from ( SELECT [1, 2, 3] Union Select [4, 5] Union Select []) order by 1";
        using var reader = duckDbCommand.ExecuteReader();
        reader.Read();
        var list = reader.GetFieldValue<List<int>>(0);
        list.Should().BeEquivalentTo(new List<int>());

        reader.Read();
        list = reader.GetFieldValue<List<int>>(0);
        list.Should().BeEquivalentTo(new List<int> { 1, 2, 3 });

        reader.Read();
        list = reader.GetFieldValue<List<int>>(0);
        list.Should().BeEquivalentTo(new List<int> { 4, 5 });
    }

    [Fact]
    public void ReadListOfIntegersWithNulls()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        using var duckDbCommand = connection.CreateCommand();
        duckDbCommand.CommandText = "Select * from ( SELECT [1, 2, NULL, 3, NULL] Union Select [NULL, NULL, 4, 5] Union Select null) order by 1";
        using var reader = duckDbCommand.ExecuteReader();
        reader.Read();
        var list = reader.GetFieldValue<List<int?>>(0);
        list.Should().BeEquivalentTo(new List<int?> { 1, 2, null, 3, null });

        reader.Read();
        reader.Invoking(rd => rd.GetFieldValue<List<int>>(0)).Should().Throw<NullReferenceException>();

        reader.Read();
        reader.IsDBNull(0).Should().BeTrue();
    }

    [Fact]
    public void ReadMultipleListOfDoubles()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        using var duckDbCommand = connection.CreateCommand();
        duckDbCommand.CommandText = "Select * from ( SELECT [1/2, 3/2, 5/2] Union Select [4, 5] Union Select []) order by 1";
        using var reader = duckDbCommand.ExecuteReader();
        reader.Read();
        var list = reader.GetFieldValue<List<double>>(0);
        list.Should().BeEquivalentTo(new List<double>());

        reader.Read();
        list = reader.GetFieldValue<List<double>>(0);
        list.Should().BeEquivalentTo(new List<double> { 0.5, 1.5, 2.5 });

        reader.Read();
        list = reader.GetFieldValue<List<double>>(0);
        list.Should().BeEquivalentTo(new List<double> { 4, 5 });
    }

    [Fact]
    public void ReadMultipleListOfStrings()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        using var duckDbCommand = connection.CreateCommand();
        duckDbCommand.CommandText = "Select * from ( SELECT ['hello', 'world'] Union Select ['from DuckDB.Net', 'client'] Union Select []) order by 1";
        using var reader = duckDbCommand.ExecuteReader();
        reader.Read();
        var list = reader.GetFieldValue<List<string>>(0);
        list.Should().BeEquivalentTo(new List<string>());

        reader.Read();
        list = reader.GetFieldValue<List<string>>(0);
        list.Should().BeEquivalentTo(new List<string> { "from DuckDB.Net", "client" });

        reader.Read();
        list = reader.GetFieldValue<List<string>>(0);
        list.Should().BeEquivalentTo(new List<string> { "hello", "world" });
    }

    [Fact]
    public void ReadMultipleListOfDecimals()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        using var duckDbCommand = connection.CreateCommand();
        duckDbCommand.CommandText = "Select * from ( SELECT [1.1, 2.3456, NULL] Union Select [73.56725, 264387.632673487236]) order by 1";
        var reader = duckDbCommand.ExecuteReader();

        reader.Read();
        var list = reader.GetFieldValue<List<decimal?>>(0);
        list.Should().BeEquivalentTo(new List<decimal?> { 1.1m, 2.3456m, null });

        reader.Read();
        var value = reader.GetValue(0);
        value.Should().BeEquivalentTo(new List<decimal?> { 73.56725m, 264387.632673487236m });
        reader.Dispose();

        duckDbCommand.CommandText = "SELECT [1.1, 2.34] ";
        reader = duckDbCommand.ExecuteReader();

        reader.Read();
        list = reader.GetFieldValue<List<decimal?>>(0);
        list.Should().BeEquivalentTo(new List<decimal?> { 1.1m, 2.34m });
        reader.Dispose();
    }

    [Fact]
    public void ReadListOfTimeStamps()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        using var duckDbCommand = connection.CreateCommand();
        duckDbCommand.CommandText = "SELECT range(date '1992-01-01', date '1992-08-01', interval '1' month);";

        using var reader = duckDbCommand.ExecuteReader();
        reader.Read();

        var list = reader.GetFieldValue<List<DateTime>>(0);
        list.Should().BeEquivalentTo(Enumerable.Range(0, 7).Select(m => new DateTime(1992, 1, 1).AddMonths(m)));
    }

    [Fact]
    public void ReadListOfDates()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        using var duckDbCommand = connection.CreateCommand();
        duckDbCommand.CommandText = "SELECT [Date '2002-04-06', Date '2008-10-12']";

        using var reader = duckDbCommand.ExecuteReader();
        reader.Read();

        var list = reader.GetFieldValue<List<DateTime>>(0);
        list.Should().BeEquivalentTo(new List<DateTime> { new(2002, 4, 6), new(2008, 10, 12) });

        var dateList = reader.GetFieldValue<List<DateOnly>>(0);
        dateList.Should().BeEquivalentTo(new List<DateOnly> { new(2002, 4, 6), new(2008, 10, 12) });
    }

    [Fact]
    public void ReadListOfTimes()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        using var duckDbCommand = connection.CreateCommand();
        duckDbCommand.CommandText = "SELECT [Time '12:14:16', Time '18:10:12']";

        using var reader = duckDbCommand.ExecuteReader();
        reader.Read();

        var list = reader.GetFieldValue<List<TimeOnly>>(0);
        list.Should().BeEquivalentTo(new List<TimeOnly> { new(12, 14, 16), new(18, 10, 12) });
    }
}