using System.Threading.Tasks;
using Akka.Actor;
using Microsoft.Azure.Cosmos.Table;

namespace Akka.Persistence.Azure.TestHelpers
{
    public static class DbUtils
    {
        public static string ConnectionString { get; private set; }

        public static void Initialize(AzuriteEmulatorFixture fixture)
        {
            ConnectionString = fixture.ConnectionString;
        }
        public static Task<bool> CleanupCloudTable(string tableName)
        {
            var account = CloudStorageAccount.Parse(ConnectionString);
            var table = account.CreateCloudTableClient().GetTableReference(tableName);
            return table.DeleteIfExistsAsync();
        }
    }
}