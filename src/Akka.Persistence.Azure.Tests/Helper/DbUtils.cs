using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Microsoft.Azure.Cosmos.Table;
using Xunit.Abstractions;

namespace Akka.Persistence.Azure.Tests.Helper
{
    public static class DbUtils
    {
        public static async Task CleanupCloudTable(string connectionString)
        {
            var account = CloudStorageAccount.Parse(connectionString);
            var tableClient = account.CreateCloudTableClient();
            foreach (var cloudTable in tableClient.ListTables())
            {
                await cloudTable.DeleteIfExistsAsync();
            }

            var blobClient = new BlobServiceClient(connectionString);
            foreach (var blobContainer in blobClient.GetBlobContainers())
            {
                await blobClient.DeleteBlobContainerAsync(blobContainer.Name);
            }
        }
    }
}