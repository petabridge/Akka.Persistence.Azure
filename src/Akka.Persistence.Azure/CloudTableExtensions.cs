// -----------------------------------------------------------------------
// <copyright file="CloudTableExtensions.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2023 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Akka.Annotations;
using Akka.Persistence.Azure.Journal;
using Akka.Persistence.Azure.TableEntities;
using Azure;
using Azure.Data.Tables;

#nullable enable
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
        [InternalApi]
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

            for (var i = 0; i < limitedBatchOperationLists.Count; i++)
            {
                try
                {
                    var limitedBatchOperationList = limitedBatchOperationLists[i];
                    var limitedBatchResponse = await table.SubmitTransactionAsync(limitedBatchOperationList, token);
                    result.AddRange(limitedBatchResponse.Value);
                }
                catch (Exception ex)
                {
                    var failedBatch = limitedBatchOperationLists[i].ToArray();
                    var sb = new StringBuilder("Failed to execute transaction batch operation");
                    
                    TableTransactionAction? failedAction;
                    if (failedBatch.Length == 1)
                        failedAction = failedBatch[0];
                    else if (ex is TableTransactionFailedException { FailedTransactionActionIndex: not null } transactionEx)
                    {
                        var batchIndex = transactionEx.FailedTransactionActionIndex.Value;
                        failedAction = failedBatch[batchIndex];
                        
                        sb.Append($" while processing batch index {batchIndex}");
                    }
                    else
                        failedAction = null;

                    if (failedAction is null) 
                        throw new DatabaseOperationException(sb.ToString(), ex);
                    
                    sb.Append($", action type: {failedAction.ActionType}");
                    var entity = (TableEntity)failedAction.Entity;
                    sb.Append($", persistence id: {entity.PartitionKey}");
                    sb.Append($", row key: {entity.RowKey}");
                    
                    if (entity.RowKey == HighestSequenceNrEntry.RowKeyValue)
                        sb.Append($", {HighestSequenceNrEntry.HighestSequenceNrKey}: {entity[HighestSequenceNrEntry.HighestSequenceNrKey]}");
                    
                    if(entity.ContainsKey(PersistentJournalEntry.SeqNoKeyName))
                        sb.Append($", sequence number: {entity.GetInt64(PersistentJournalEntry.SeqNoKeyName)}");
                    
                    if(entity.ContainsKey(PersistentJournalEntry.ManifestKeyName))
                        sb.Append($", manifest: {entity.GetString(PersistentJournalEntry.ManifestKeyName)}");
                    
                    throw new DatabaseOperationException(sb.ToString(), ex);
                }
            }

            return result;
        }
    }
}