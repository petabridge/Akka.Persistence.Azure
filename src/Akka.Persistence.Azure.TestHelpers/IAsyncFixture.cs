using System;
using System.Threading.Tasks;

namespace Akka.Persistence.Azure.TestHelpers
{
    /// <summary>
    /// Describes an asynchronous test fixture that we may use in both
    /// XUnit and NBench specs for integration testing purposes.
    /// </summary>
    public interface IAsyncFixture
    {
        Task Initialize();

        Task CleanUp();
    }

    public class WindowsAzureStorageEmulatorFixture : IAsyncFixture
    {
    }
}
