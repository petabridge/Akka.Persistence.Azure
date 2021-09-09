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

namespace Akka.Persistence.Azure.TestHelpers
{
    /// <summary>
    ///     Integration testing fixture using the Windows Azure Storage Emulator
    ///     Docker image provided by Microsoft: https://hub.docker.com/r/microsoft/azure-storage-emulator/
    /// </summary>
    public class WindowsAzureStorageEmulatorFixture : IAsyncFixture
    {
        private const string AzureStorageImageName = "microsoft/azure-storage-emulator";
        private readonly string _azureStorageContainerName = $"azurestorage-{Guid.NewGuid():N}";
        private DockerClient _client;

        public string ConnectionString { get; private set; }

        public async Task Initialize()
        {
            DockerClientConfiguration config;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                config = new DockerClientConfiguration(new Uri("unix:///var/run/docker.sock"));
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                config = new DockerClientConfiguration(new Uri("npipe://./pipe/docker_engine"));
            else
                throw new NotSupportedException($"Unsupported OS [{RuntimeInformation.OSDescription}]");

            _client = config.CreateClient();

            var images =
                await _client.Images.ListImagesAsync(new ImagesListParameters
                {
                    Filters = new Dictionary<string, IDictionary<string, bool>>
                    {
                        ["reference"] = new Dictionary<string, bool>
                        {
                            [AzureStorageImageName] = true
                        }
                    }
                });
            if (images.Count == 0)
                await _client.Images.CreateImageAsync(
                    new ImagesCreateParameters {FromImage = AzureStorageImageName, Tag = "latest"}, null,
                    new Progress<JSONMessage>());

            var azureBlobPort = 10000;
            var azureQueuePort = 10001;
            var azureTablePort = 10002;

            // create the container
            await _client.Containers.CreateContainerAsync(new CreateContainerParameters
            {
                Image = AzureStorageImageName,
                Name = _azureStorageContainerName,
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
                                    HostPort = $"{azureBlobPort}"
                                }
                            }
                        },

                        {
                            "10001/tcp",
                            new List<PortBinding>
                            {
                                new PortBinding
                                {
                                    HostPort = $"{azureQueuePort}"
                                }
                            }
                        },

                        {
                            "10002/tcp",
                            new List<PortBinding>
                            {
                                new PortBinding
                                {
                                    HostPort = $"{azureTablePort}"
                                }
                            }
                        }
                    }
                }
            });

            // start the container
            await _client.Containers.StartContainerAsync(_azureStorageContainerName, new ContainerStartParameters());

            ConnectionString = GenerateConnStr();

            await Task.Delay(TimeSpan.FromSeconds(10));
        }

        public async Task CleanUp()
        {
            if (_client != null)
            {
                await _client.Containers.StopContainerAsync(_azureStorageContainerName, new ContainerStopParameters());
                await _client.Containers.RemoveContainerAsync(_azureStorageContainerName,
                    new ContainerRemoveParameters {Force = true});
                _client.Dispose();
            }
        }

        public static string GenerateConnStr(string ip = "127.0.0.1", int blobport = 10000, int queueport = 10001,
            int tableport = 10002)
        {
            return
                $"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://{ip}:{blobport}/devstoreaccount1;TableEndpoint=http://{ip}:{tableport}/devstoreaccount1;QueueEndpoint=http://{ip}:{queueport}/devstoreaccount1;";
        }
    }
}