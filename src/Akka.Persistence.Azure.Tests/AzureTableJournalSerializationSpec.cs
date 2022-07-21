// -----------------------------------------------------------------------
// <copyright file="AzureTableJournalSerializationSpec.cs" company="Petabridge, LLC">
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
    public class AzureTableJournalSerializationSpec : JournalSerializationSpec
    {
        public AzureTableJournalSerializationSpec(ITestOutputHelper output)
            : base(AzureConfig(), nameof(AzureTableJournalSerializationSpec), output)
        {
            AzurePersistence.Get(Sys);
        }

        [Fact(Skip= "https://github.com/akkadotnet/akka.net/issues/3965")]
        public override void Journal_should_serialize_Persistent_with_EventAdapter_manifest()
        {

        }
    }
}