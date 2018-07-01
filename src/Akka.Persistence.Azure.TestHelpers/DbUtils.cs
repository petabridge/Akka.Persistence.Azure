using System.Threading.Tasks;
using Akka.Actor;
using Microsoft.WindowsAzure.Storage;

namespace Akka.Persistence.Azure.TestHelpers
{
    public class DbUtils
    {
        public static Task<bool> CleanupCloudTable(ActorSystem sys, string tableName)
        {
            var connectionString = AzurePersistence.Get(sys).TableSettings.ConnectionString;
            var account = CloudStorageAccount.Parse(connectionString);
            var table = account.CreateCloudTableClient().GetTableReference(tableName);
            return table.DeleteIfExistsAsync();
        }
    }
}