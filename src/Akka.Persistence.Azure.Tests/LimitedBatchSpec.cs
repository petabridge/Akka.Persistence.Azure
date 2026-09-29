// -----------------------------------------------------------------------
//  <copyright file="LimitedBatchSpec.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2022 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka.Persistence.Azure.Tests.Helper;
using Azure.Data.Tables;
using Xunit;

namespace Akka.Persistence.Azure.Tests
{
    [Collection("AzureSpecs")]
    public class LimitedBatchSpec: IAsyncLifetime
    {
        private readonly TableClient _tableClient;

        public LimitedBatchSpec(AzuriteFixture fixture, ITestOutputHelper output)
        {
            // Generate unique table name to avoid conflicts when running in collection
            var tableName = $"testtable{Guid.NewGuid().ToString("N")[^8..]}";
            _tableClient = new TableClient(fixture.ConnectionString, tableName);
        }

        public async ValueTask InitializeAsync()
        {
            await _tableClient.CreateAsync();
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }

        [Fact(DisplayName = "Limited batch with 0 entries should return empty list")]
        public async Task ZeroEntriesTest()
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
            cts.CancelAfter(TimeSpan.FromSeconds(3));
            var result = await _tableClient.ExecuteBatchAsLimitedBatches(new List<TableTransactionAction>(), cts.Token);
            Assert.Empty(result);

            var entities = await _tableClient.QueryAsync<TableEntity>("PartitionKey eq 'test'", null, null, cts.Token)
                .ToListAsync(cts.Token);
            Assert.Empty(entities);
        }
        
        [Fact(DisplayName = "Limited batch with less than 100 entries should work")]
        public async Task FewEntriesTest()
        {
            var entries = Enumerable.Range(1, 50)
                .Select(i => new TableTransactionAction(TableTransactionActionType.Add, new TableEntity
                {
                    PartitionKey = "test",
                    RowKey = i.ToString("D8")
                })).ToList();
            
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
            var result = await _tableClient.ExecuteBatchAsLimitedBatches(entries, cts.Token);
            Assert.Equal(50, result.Count);

            var entities = await _tableClient.QueryAsync<TableEntity>("PartitionKey eq 'test'", null, null, cts.Token)
                .ToListAsync(cts.Token);
            Assert.Equal(50, entities.Count);
            Assert.Equal(Enumerable.Range(1, 50).ToList(), entities.Select(e => int.Parse(e.RowKey.TrimStart('0'))).ToList());
        }
        
        [Fact(DisplayName = "Limited batch with more than 100 entries should work")]
        public async Task LotsEntriesTest()
        {
            var entries = Enumerable.Range(1, 505)
                .Select(i => new TableTransactionAction(TableTransactionActionType.Add, new TableEntity
                {
                    PartitionKey = "test",
                    RowKey = i.ToString("D8")
                })).ToList();
            
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
            var result = await _tableClient.ExecuteBatchAsLimitedBatches(entries, cts.Token);
            Assert.Equal(505, result.Count);

            var entities = await _tableClient.QueryAsync<TableEntity>("PartitionKey eq 'test'", null, null, cts.Token)
                .ToListAsync(cts.Token);
            Assert.Equal(505, entities.Count);
            Assert.Equal(Enumerable.Range(1, 505).ToList(), entities.Select(e => int.Parse(e.RowKey.TrimStart('0'))).ToList());
        }
    }
}