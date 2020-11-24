﻿// -----------------------------------------------------------------------
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
    ///     Integration testing fixture using the Windows Azure CosmosDb Emulator
    ///     Docker image provided by Microsoft: https://hub.docker.com/r/microsoft/azure-cosmosdb-emulator/
    ///     Image is not supported on Linux Container
    /// </summary>
    public class AzureCosmosDbEmulatorFixture : IAsyncFixture
    {
        //does not work on linux
        private const string AzureCosmosDbImageName = "microsoft/azure-cosmosdb-emulator";
        private readonly string _azureStorageContainerName = $"cosmos-{Guid.NewGuid():N}".Replace("-", "");
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
                await _client.Images.ListImagesAsync(new ImagesListParameters { MatchName = AzureCosmosDbImageName });
            if (images.Count == 0)
                await _client.Images.CreateImageAsync(
                    new ImagesCreateParameters { FromImage = AzureCosmosDbImageName, Tag = "latest" }, null,
                    new Progress<JSONMessage>());


            var azureTablePort = 8081;

            // create the container
            await _client.Containers.CreateContainerAsync(new CreateContainerParameters
            {
                Image = AzureCosmosDbImageName,
                Name = _azureStorageContainerName,
                Tty = true,
                HostConfig = new HostConfig
                {
                    PortBindings = new Dictionary<string, IList<PortBinding>>
                    {
                        {
                            "8081/tcp",
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
                    new ContainerRemoveParameters { Force = true });
                _client.Dispose();
            }
        }

        public static string GenerateConnStr(string ip = "127.0.0.1", int tableport = 8081)
        {
            return
                $"AccountEndpoint=https://{ip}:{tableport}/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
        }
    }
}