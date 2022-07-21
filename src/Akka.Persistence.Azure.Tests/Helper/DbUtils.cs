using System.Threading.Tasks;
using Azure.Data.Tables;
using Azure.Storage.Blobs;

namespace Akka.Persistence.Azure.Tests.Helper
{
    public static class DbUtils
    {
        public static async Task CleanupCloudTable(string connectionString)
        {
            var tableClient = new TableServiceClient(connectionString);
            
            await foreach(var table in tableClient.QueryAsync())
            {
                await tableClient.DeleteTableAsync(table.Name);
            }
            
            var blobClient = new BlobServiceClient(connectionString);
            foreach (var blobContainer in blobClient.GetBlobContainers())
            {
                await blobClient.DeleteBlobContainerAsync(blobContainer.Name);
            }
        }
    }
}