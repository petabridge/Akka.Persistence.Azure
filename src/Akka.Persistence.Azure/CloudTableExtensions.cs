// -----------------------------------------------------------------------
// <copyright file="CloudTableExtensions.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2023 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Data.Tables;

namespace Akka.Persistence.Azure
{
    public static class CloudTableExtensions
    {
        private const int MaxBatchSize = 100;
        
        /// <summary>
        /// <para>
        /// Execute a batch transaction to the service. This method automatically chunks the batch request into chunks
        /// of 100 items if the batch size is greater than 100.
        /// </para>
        /// <b>NOTE</b>: This does mean that sending more than 100 items will break atomicity, there is no guarantee
        /// that all items in the batch will be executed successfully.
        /// </summary>
        /// <param name="table">The Azure table client</param>
        /// <param name="batch">The list of <see cref="TableTransactionAction"/> items to be sent to the service</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>List of <see cref="Response"/> for each items</returns>
        // TODO Replace this with real transactional execution if Azure Table Storage supports it in the future.
        public static async Task<IReadOnlyList<Response>> ExecuteBatchAsLimitedBatches(
            this TableClient table,
            List<TableTransactionAction> batch, 
            CancellationToken token)
        {
            if (batch.Count < 1)
                return ImmutableList<Response>.Empty;
            
            if (batch.Count <= MaxBatchSize)
                return (await table.SubmitTransactionAsync(batch, token)).Value;

            var result = new List<Response>();
            var limitedBatchOperationLists = batch.ChunkBy(MaxBatchSize);
            
            foreach (var limitedBatchOperationList in limitedBatchOperationLists)
            {
                var limitedBatchResponse = await table.SubmitTransactionAsync(limitedBatchOperationList, token);
                result.AddRange(limitedBatchResponse.Value);
            }

            return result;
        }
    }
}