// -----------------------------------------------------------------------
// <copyright file="WindowsAzureStorageEmulatorFixture.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2018 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Docker.DotNet;
using Docker.DotNet.Models;
using Xunit;

namespace Akka.Persistence.Azure.TestHelpers
{
    /// <summary>
    ///     Integration testing fixture using the Windows Azure CosmosDb Emulator
    ///     Docker image provided by Microsoft: https://hub.docker.com/r/microsoft/azure-cosmosdb-emulator/
    ///     Akka.Net: https://hub.docker.com/r/akkadotnet/azure-storage-emulator/
    /// </summary>
    public class EmulatorFixture : IAsyncLifetime
    {
        //does not work on linux
        /*
         cd C:\Program Files\Azure Cosmos DB Emulator 
         Microsoft.Azure.Cosmos.Emulator /EnableTableEndpoint
         */
        private const string _cosmosDbImageName = "microsoft/azure-cosmosdb-emulator";
        private const string _azuriteImageName = "mcr.microsoft.com/azure-storage/azurite";
        private readonly string _azuriteContainerName = $"azurite-{Guid.NewGuid():N}".Replace("-", "");
        private readonly string _cosmosContainerName = $"cosmos-{Guid.NewGuid():N}".Replace("-", "");
        private const string _storageImageName = "farmer1992/azure-storage-emulator";
        private readonly string _storageContainerName = $"azurestorage-{Guid.NewGuid():N}".Replace("-", "");
        private DockerClient _client;

        public static Dictionary<string, string> ConnectionStrings { get; private set; } = new Dictionary<string, string>();

        public async Task InitializeAsync()
        {
            DockerClientConfiguration config;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                config = new DockerClientConfiguration(new Uri("unix:///var/run/docker.sock"));
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                config = new DockerClientConfiguration(new Uri("npipe://./pipe/docker_engine"));
            else
                throw new NotSupportedException($"Unsupported OS [{RuntimeInformation.OSDescription}]");

            _client = config.CreateClient();

            //await InitializeCosmos();
            //await InitializeStorage();
            await InitializeAzurite();
            await Task.Delay(TimeSpan.FromSeconds(10));

        }
        private async Task InitializeCosmos()
        {
            var images =
                await _client.Images.ListImagesAsync(new ImagesListParameters { MatchName = _cosmosDbImageName });
            if (images.Count == 0)
                await _client.Images.CreateImageAsync(
                    new ImagesCreateParameters { FromImage = _cosmosDbImageName, Tag = "latest" }, null,
                    new Progress<JSONMessage>());


            var azureTablePort = 8081;

            // create the container
            await _client.Containers.CreateContainerAsync(new CreateContainerParameters
            {
                Image = _cosmosDbImageName,
                Name = _cosmosContainerName,
                Tty = true,
                HostConfig = new HostConfig
                {
                    PortBindings = new Dictionary<string, IList<PortBinding>>
                    {
                        {
                            "10250/tcp",
                            new List<PortBinding>
                            {
                                new PortBinding
                                {
                                    HostPort = "10250"
                                }
                            }
                        },
                        {
                            "10251/tcp",
                            new List<PortBinding>
                            {
                                new PortBinding
                                {
                                    HostPort = "10251"
                                }
                            }
                        },
                        {
                            "10252/tcp",
                            new List<PortBinding>
                            {
                                new PortBinding
                                {
                                    HostPort = "10252"
                                }
                            }
                        },
                        {
                            "10253/tcp",
                            new List<PortBinding>
                            {
                                new PortBinding
                                {
                                    HostPort = "10253"
                                }
                            }
                        },
                        {
                            "10254/tcp",
                            new List<PortBinding>
                            {
                                new PortBinding
                                {
                                    HostPort = "10254"
                                }
                            }
                        },
                        {
                            "10255/tcp",
                            new List<PortBinding>
                            {
                                new PortBinding
                                {
                                    HostPort = "10255"
                                }
                            }
                        },
                        {
                            "10350/tcp",
                            new List<PortBinding>
                            {
                                new PortBinding
                                {
                                    HostPort = "10350"
                                }
                            }
                        },
                        {
                            "8081/tcp",
                            new List<PortBinding>
                            {
                                new PortBinding
                                {
                                    HostPort = $"{azureTablePort}"
                                }
                            }
                        },
                        {
                            "8900/tcp",
                            new List<PortBinding>
                            {
                                new PortBinding
                                {
                                    HostPort = "8900"
                                }
                            }
                        },
                        {
                            "8901/tcp",
                            new List<PortBinding>
                            {
                                new PortBinding
                                {
                                    HostPort = "8901"
                                }
                            }
                        },
                        {
                            "8902/tcp",
                            new List<PortBinding>
                            {
                                new PortBinding
                                {
                                    HostPort = "8902"
                                }
                            }
                        }
                    }
                }
            });

            // start the container
            await _client.Containers.StartContainerAsync(_cosmosContainerName, new ContainerStartParameters());

            ConnectionStrings["Cosmos"] = CosmosConnStr();
        }

        private async Task InitializeStorage()
        {
            var images =
                await _client.Images.ListImagesAsync(new ImagesListParameters { MatchName = _storageImageName });
            if (images.Count == 0)
                await _client.Images.CreateImageAsync(
                    new ImagesCreateParameters { FromImage = _storageImageName, Tag = "latest" }, null,
                    new Progress<JSONMessage>());

            // create the container
            await _client.Containers.CreateContainerAsync(new CreateContainerParameters
            {
                Image = _storageImageName,
                Name = _storageContainerName,
                Tty = true,
                HostConfig = new HostConfig
                {
                    PortBindings = new Dictionary<string, IList<PortBinding>>
                    {
                        {
                            "10000/tcp",
                            new List<PortBinding>
                            {
                                new PortBinding
                                {
                                    HostPort = "10000"
                                }
                            }
                        },

                        {
                            "10001/tcp",
                            new List<PortBinding>
                            {
                                new PortBinding
                                {
                                    HostPort = "10001"
                                }
                            }
                        },

                        {
                            "10002/tcp",
                            new List<PortBinding>
                            {
                                new PortBinding
                                {
                                    HostPort = "10002"
                                }
                            }
                        }
                    }
                }
            });

            // start the container
            await _client.Containers.StartContainerAsync(_storageContainerName, new ContainerStartParameters());

            ConnectionStrings["Storage"] = StorageConnStr();
        }
        private async Task InitializeAzurite()
        {
            var images =
                await _client.Images.ListImagesAsync(new ImagesListParameters { MatchName = _azuriteImageName });
            if (images.Count == 0)
                await _client.Images.CreateImageAsync(
                    new ImagesCreateParameters { FromImage = _azuriteImageName, Tag = "latest" }, null,
                    new Progress<JSONMessage>());

            // create the container
            await _client.Containers.CreateContainerAsync(new CreateContainerParameters
            {
                Image = _azuriteImageName,
                Name = _azuriteContainerName,
                Tty = true,
                HostConfig = new HostConfig
                {
                    PortBindings = new Dictionary<string, IList<PortBinding>>
                    {
                        {
                            "10000/tcp",
                            new List<PortBinding>
                            {
                                new PortBinding
                                {
                                    HostPort = "10000"
                                }
                            }
                        },

                        {
                            "10001/tcp",
                            new List<PortBinding>
                            {
                                new PortBinding
                                {
                                    HostPort = "10001"
                                }
                            }
                        },

                        {
                            "10002/tcp",
                            new List<PortBinding>
                            {
                                new PortBinding
                                {
                                    HostPort = "10002"
                                }
                            }
                        }
                    }
                }
            });

            // start the container
            await _client.Containers.StartContainerAsync(_azuriteContainerName, new ContainerStartParameters());

            ConnectionStrings["Azurite"] = AzuriteConnStr();
        }
        public async Task DisposeAsync()
        {
            if (_client != null)
            {
                //await CleanUpCosmos();
                //await CleanUpStorage();
                await CleanUpAzurite();
                _client.Dispose();
            }

            await Task.CompletedTask;
        }
        public async Task CleanUpCosmos()
        {
            await _client.Containers.StopContainerAsync(_cosmosContainerName, new ContainerStopParameters());
            await _client.Containers.RemoveContainerAsync(_cosmosContainerName,
                new ContainerRemoveParameters { Force = true });
        }
        
        public async Task CleanUpStorage()
        {
            await _client.Containers.StopContainerAsync(_storageContainerName, new ContainerStopParameters());
            await _client.Containers.RemoveContainerAsync(_storageContainerName,
                new ContainerRemoveParameters { Force = true });
        }
        public async Task CleanUpAzurite()
        {
            await _client.Containers.StopContainerAsync(_azuriteContainerName, new ContainerStopParameters());
            await _client.Containers.RemoveContainerAsync(_azuriteContainerName,
                new ContainerRemoveParameters { Force = true });
        }
        public static string CosmosConnStr()
        {
            return $"DefaultEndpointsProtocol=https;AccountName=localhost;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==;TableEndpoint=https://localhost:8902/;";
        }
        public static string StorageConnStr(string ip = "127.0.0.1", int blobport = 10000,
            int queueport = 10001)
        {
            return
                $"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://{ip}:{blobport}/devstoreaccount1;QueueEndpoint=http://{ip}:{queueport}/devstoreaccount1;";
        }
        public static string AzuriteConnStr(string ip = "127.0.0.1", int blobport = 10000,
            int queueport = 10001, int tableport = 10002)
        {
            return
                $"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://{ip}:{blobport}/devstoreaccount1;QueueEndpoint=http://{ip}:{queueport}/devstoreaccount1;";
        }
    }
}