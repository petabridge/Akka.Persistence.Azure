using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace Akka.Persistence.Azure
{
    public static class CloudTableExtensions
    {
        private const int MaxBatchSize = 100;
        
        public static async Task<IList<TableResult>> ExecuteBatchAsLimitedBatches(
            this CloudTable table,
            TableBatchOperation batch)
        {
            if (batch.Count < 1)
                return new List<TableResult>();
            
            if (batch.Count <= MaxBatchSize)
                return await table.ExecuteBatchAsync(batch);

            var result = new List<TableResult>();
            var limitedBatchOperationLists = batch.ChunkBy(MaxBatchSize);
            
            foreach (var limitedBatchOperationList in limitedBatchOperationLists)
            {
                var limitedBatch = CreateLimitedTableBatchOperation(limitedBatchOperationList);
                var limitedBatchResult = await table.ExecuteBatchAsync(limitedBatch);
                result.AddRange(limitedBatchResult);
            }

            return result;
        }

        private static TableBatchOperation CreateLimitedTableBatchOperation(
            IEnumerable<TableOperation> limitedBatchOperationList)
        {
            var limitedBatch = new TableBatchOperation();
            foreach (var limitedBatchOperation in limitedBatchOperationList)
            {
                limitedBatch.Add(limitedBatchOperation);
            }

            return limitedBatch;
        }
    }
}