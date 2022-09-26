// -----------------------------------------------------------------------
//  <copyright file="LimitedBatchSpec.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2022 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka.Persistence.Azure.Tests.Helper;
using Azure.Data.Tables;
using FluentAssertions;
using FluentAssertions.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Azure.Tests
{
    public class LimitedBatchSpec: IAsyncLifetime
    {
        private readonly TableClient _tableClient;
        
        public LimitedBatchSpec(ITestOutputHelper output)
        {
            _tableClient = new TableClient("UseDevelopmentStorage=true", "testtable");
        }
        
        public async Task InitializeAsync()
        {
            await DbUtils.CleanupCloudTable("UseDevelopmentStorage=true");
            await _tableClient.CreateAsync();
        }

        public Task DisposeAsync()
        {
            return Task.CompletedTask;
        }

        [Fact(DisplayName = "Limited batch with 0 entries should return empty list")]
        public async Task ZeroEntriesTest()
        {
            using var cts = new CancellationTokenSource(3.Seconds());
            var result = await _tableClient.ExecuteBatchAsLimitedBatches(new List<TableTransactionAction>(), cts.Token);
            result.Count.Should().Be(0);

            var entities = await _tableClient.QueryAsync<TableEntity>("PartitionKey eq 'test'", null, null, cts.Token)
                .ToListAsync(cts.Token);
            entities.Count.Should().Be(0);
        }
        
        [Fact(DisplayName = "Limited batch with less than 100 entries should work")]
        public async Task FewEntriesTest()
        {
            var entries = Enumerable.Range(0, 50)
                .Select(i => new TableTransactionAction(TableTransactionActionType.Add, new TableEntity
                {
                    PartitionKey = "test",
                    RowKey = i.ToString("D8")
                })).ToList();
            
            using var cts = new CancellationTokenSource(3.Seconds());
            var result = await _tableClient.ExecuteBatchAsLimitedBatches(entries, cts.Token);
            result.Count.Should().Be(50);

            var entities = await _tableClient.QueryAsync<TableEntity>("PartitionKey eq 'test'", null, null, cts.Token)
                .ToListAsync(cts.Token);
            entities.Count.Should().Be(50);
            entities.Select(e => int.Parse(e.RowKey)).Should().BeEquivalentTo(Enumerable.Range(0, 50));
        }
        
        [Fact(DisplayName = "Limited batch with more than 100 entries should work")]
        public async Task LotsEntriesTest()
        {
            var entries = Enumerable.Range(0, 505)
                .Select(i => new TableTransactionAction(TableTransactionActionType.Add, new TableEntity
                {
                    PartitionKey = "test",
                    RowKey = i.ToString("D8")
                })).ToList();
            
            using var cts = new CancellationTokenSource(3.Seconds());
            var result = await _tableClient.ExecuteBatchAsLimitedBatches(entries, cts.Token);
            result.Count.Should().Be(505);

            var entities = await _tableClient.QueryAsync<TableEntity>("PartitionKey eq 'test'", null, null, cts.Token)
                .ToListAsync(cts.Token);
            entities.Count.Should().Be(505);
            entities.Select(e => int.Parse(e.RowKey)).Should().BeEquivalentTo(Enumerable.Range(0, 505));
        }
    }
}