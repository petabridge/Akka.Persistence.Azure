using System.Threading.Tasks;
using Akka.Actor;
using Microsoft.Azure.Cosmos.Table;

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