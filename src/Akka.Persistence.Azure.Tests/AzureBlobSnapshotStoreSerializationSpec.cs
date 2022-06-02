// -----------------------------------------------------------------------
// <copyright file="AzureBlobSnapshotStoreSerializationSpec.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2019 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Configuration;
using Akka.Persistence.Azure.Tests.Helper;
using Akka.Persistence.TCK.Serialization;
using Xunit;
using Xunit.Abstractions;
using static Akka.Persistence.Azure.Tests.Helper.AzureStorageConfigHelper;

namespace Akka.Persistence.Azure.Tests
{
    [Collection("AzureSpecs")]
    public class AzureBlobSnapshotStoreSerializationSpec : SnapshotStoreSerializationSpec    
    {
        public AzureBlobSnapshotStoreSerializationSpec(ITestOutputHelper output) 
            : base(AzureConfig(), nameof(AzureBlobSnapshotStoreSerializationSpec), output)
        {
            AzurePersistence.Get(Sys);
        }
    }
}