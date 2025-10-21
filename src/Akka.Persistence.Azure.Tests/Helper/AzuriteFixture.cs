// -----------------------------------------------------------------------
// <copyright file="AzuriteFixture.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2023 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Testcontainers.Azurite;
using Xunit;

namespace Akka.Persistence.Azure.Tests.Helper
{
    /// <summary>
    /// Provides a shared Azurite container for Azure persistence tests.
    /// The container is shared across all tests in the collection and automatically cleaned up.
    /// </summary>
    public class AzuriteFixture : IAsyncLifetime
    {
        private AzuriteContainer _container;

        /// <summary>
        /// The connection string for the running Azurite container.
        /// Use this to configure Azure persistence in tests.
        /// </summary>
        public string ConnectionString { get; private set; }

        public async Task InitializeAsync()
        {
            _container = new AzuriteBuilder()
                .WithImage("mcr.microsoft.com/azure-storage/azurite:latest")
                .Build();

            await _container.StartAsync();
            ConnectionString = _container.GetConnectionString();
        }

        public async Task DisposeAsync()
        {
            if (_container != null)
            {
                await _container.DisposeAsync();
            }
        }
    }

    /// <summary>
    /// Collection definition for Azure specs.
    /// All tests in this collection share a single Azurite container and run sequentially.
    /// </summary>
    [CollectionDefinition("AzureSpecs")]
    public class AzureSpecsCollection : ICollectionFixture<AzuriteFixture>
    {
        // This class is never instantiated. It exists only to define the collection.
    }
}
