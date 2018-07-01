using System.Threading.Tasks;
using Akka.Actor;
using Microsoft.WindowsAzure.Storage;

namespace Akka.Persistence.Azure.TestHelpers
{
    public class DbUtils
    {
        public static Task<bool> CleanupCloudTable(string connectionString, string tableName)
        {
            var account = CloudStorageAccount.Parse(connectionString);
            var table = account.CreateCloudTableClient().GetTableReference(tableName);
            return table.DeleteIfExistsAsync();
        }
    }
}