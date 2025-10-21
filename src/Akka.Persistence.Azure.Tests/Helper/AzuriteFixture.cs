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
    /// Provides an isolated Azurite container for Azure persistence tests.
    /// Each test class gets a fresh container with automatic lifecycle management.
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
}
