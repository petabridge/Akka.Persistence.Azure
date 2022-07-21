// -----------------------------------------------------------------------
// <copyright file="AzureTableJournalSpec.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2018 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using Akka.Configuration;
using Akka.Persistence.TCK.Journal;
using System;
using Akka.Persistence.Azure.Tests.Helper;
using Xunit;
using Xunit.Abstractions;
using static Akka.Persistence.Azure.Tests.Helper.AzureStorageConfigHelper;

namespace Akka.Persistence.Azure.Tests
{
    [Collection("AzureSpecs")]
    public abstract class AzureTableJournalSpec : JournalSpec
    {
        public AzureTableJournalSpec(ITestOutputHelper output)
            : base(AzureConfig(), nameof(AzureTableJournalSpec), output)
        {
            AzurePersistence.Get(Sys);
            Initialize();
        }
    }
}