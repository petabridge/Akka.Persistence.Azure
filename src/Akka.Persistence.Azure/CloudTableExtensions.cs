// -----------------------------------------------------------------------
// <copyright file="CloudTableExtensions.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2022 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading.Tasks;
using Azure;
using Azure.Data.Tables;

namespace Akka.Persistence.Azure
{
    public static class CloudTableExtensions
    {
        private const int MaxBatchSize = 100;
        
        public static async Task<IReadOnlyList<Response>> ExecuteBatchAsLimitedBatches(
            this TableClient table,
            List<TableTransactionAction> batch)
        {
            if (batch.Count < 1)
                return ImmutableList<Response>.Empty;
            
            if (batch.Count <= MaxBatchSize)
                return (await table.SubmitTransactionAsync(batch)).Value;

            var result = new List<Response>();
            var limitedBatchOperationLists = batch.ChunkBy(MaxBatchSize);
            
            foreach (var limitedBatchOperationList in limitedBatchOperationLists)
            {
                var limitedBatchResponse = await table.SubmitTransactionAsync(limitedBatchOperationList);
                result.AddRange(limitedBatchResponse.Value);
            }

            return result;
        }
    }
}