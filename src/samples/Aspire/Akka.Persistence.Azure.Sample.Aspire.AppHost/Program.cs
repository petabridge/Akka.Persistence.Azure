// -----------------------------------------------------------------------
// <copyright file="Program.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2025 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

var builder = DistributedApplication.CreateBuilder(args);

// Configure Azure Storage emulator (Azurite) for local development
var azureStorage = builder.AddAzureStorage("storage")
    .RunAsEmulator();

// Add blob storage resource for snapshot store
var blobStorage = azureStorage.AddBlobs("persistence-blobs");

// Add table storage resource for journal
var tableStorage = azureStorage.AddTables("persistence-tables");

// Add the Akka.NET sample project with references to storage resources
builder.AddProject<Projects.Akka_Persistence_Azure_Sample_Aspire>("akka-sample")
    .WithReference(blobStorage)
    .WithReference(tableStorage)
    .WaitFor(azureStorage);

builder.Build().Run();
