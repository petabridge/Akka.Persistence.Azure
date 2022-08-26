// -----------------------------------------------------------------------
// <copyright file="AzureBlobSnapshotStoreSpec.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2022 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Configuration;
using Akka.Persistence.Azure.Tests.Helper;
using Akka.Persistence.TCK.Snapshot;
using Xunit;
using Xunit.Abstractions;
using static Akka.Persistence.Azure.Tests.Helper.AzureStorageConfigHelper;

namespace Akka.Persistence.Azure.Tests
{
    [Collection("AzureSpecs")]
    public class AzureBlobSnapshotStoreSpec : SnapshotStoreSpec
    {
        public AzureBlobSnapshotStoreSpec(ITestOutputHelper output) 
            : base(AzureConfig(), nameof(AzureBlobSnapshotStoreSpec), output)
        {
            AzurePersistence.Get(Sys);
            Initialize();
        }
    }
}