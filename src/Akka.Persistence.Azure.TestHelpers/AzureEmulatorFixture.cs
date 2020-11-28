// -----------------------------------------------------------------------
// <copyright file="WindowsAzureStorageEmulatorFixture.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2018 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Akka.Util;
using Docker.DotNet;
using Docker.DotNet.Models;
using Xunit;

namespace Akka.Persistence.Azure.TestHelpers
{
    /// <summary>
    ///     Integration testing fixture using the Windows Azure Storage Emulator
    ///     Docker image provided by Microsoft: https://hub.docker.com/r/microsoft/azure-storage-emulator/
    /// </summary>
    public class AzureEmulatorFixture : IAsyncLifetime
    {
        //private const string AzureStorageImageName = "farmer1992/azure-storage-emulator:latest";
        //private const string AzureStorageImageName = "eabanonu/legacy-azurite:latest";
        private const string AzureStorageImageName = "akkadotnet/azure-storage-emulator:ltsc2019";
        private readonly string _azureStorageContainerName = $"azurestorage-{Guid.NewGuid():N}";
        private DockerClient _client;

        public static string ConnectionString { get; private set; }
        public AzureEmulatorFixture()
        {

            DockerClientConfiguration config;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                config = new DockerClientConfiguration(new Uri("unix:///var/run/docker.sock"));
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                config = new DockerClientConfiguration(new Uri("npipe://./pipe/docker_engine"));
            else
                throw new NotSupportedException($"Unsupported OS [{RuntimeInformation.OSDescription}]");

            _client = config.CreateClient();
        }
        public async Task InitializeAsync()
        {
            var images = await _client.Images.ListImagesAsync(new ImagesListParameters { MatchName = AzureStorageImageName });
            if (images.Count == 0)
                await _client.Images.CreateImageAsync(
                    new ImagesCreateParameters { FromImage = AzureStorageImageName, Tag = "latest" }, null,
                    new Progress<JSONMessage>(message =>
                    {
                        Console.WriteLine(!string.IsNullOrEmpty(message.ErrorMessage)
                            ? message.ErrorMessage
                            : $"{message.ID} {message.Status} {message.ProgressMessage}");
                    }));

            var azureBlobPort = ThreadLocalRandom.Current.Next(9000, 10000);
            var azureQueuePort = ThreadLocalRandom.Current.Next(9000, 10000);
            var azureTablePort = ThreadLocalRandom.Current.Next(9000, 10000);

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

            ConnectionString = GenerateConnStr(blobport: azureBlobPort, queueport: azureQueuePort, tableport: azureTablePort);

            await Task.Delay(TimeSpan.FromSeconds(10));
        }

        public async Task DisposeAsync()
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