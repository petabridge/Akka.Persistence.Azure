using System;
using Akka.Configuration;

namespace Akka.Persistence.Azure.Tests.Helper
{
    public static class AzureStorageConfigHelper
    {
        public static Config AzureConfig(string cosmosConnectionString, string blobConnectionString)
        {
            var tableName = "t" + Guid.NewGuid().ToString().Replace("-", "");
            //var containerName = "testcontainer" + Guid.NewGuid(); => System.ArgumentException : Invalid table name. Check MSDN for more information about valid table naming.

            var containerName = "test" + Guid.NewGuid().ToString().Replace("-", "");

            return ConfigurationFactory.ParseString(
                    @"
akka {
    loglevel = DEBUG
    log-config-on-start = off
    test.single-expect-default = 30s

    persistence {
        publish-plugin-commands = on

        journal {
            plugin = ""akka.persistence.journal.azure-table""

            azure-table {
                connection-string=""" + cosmosConnectionString + @"""
                connect-timeout = 3s
                request-timeout = 3s
                verbose-logging = on

                event-adapters {
                    color-tagger = ""Akka.Persistence.TCK.Query.ColorFruitTagger, Akka.Persistence.TCK""
                }
                event-adapter-bindings = {
                    ""System.String"" = color-tagger
                }
            }
        }

        query {
            journal {
                azure-table {
                    write-plugin = ""akka.persistence.journal.azure-table""
                    refresh-interval = 1s
		            max-buffer-size = 150
                }
            }
        }

        snapshot-store {
            plugin = ""akka.persistence.snapshot-store.azure-blob-store""
            
            azure-blob-store {
                connection-string=""" + blobConnectionString + @"""
                container-name=""defaultContainer""
                table-name = ""defaultTable""
                request -timeout = 3s
            }
        }
    }
}")
                .WithFallback("akka.persistence.journal.azure-table.table-name=" + tableName);
                //.WithFallback("akka.persistence.snapshot-store.azure-blob-store.table-name=" + containerName);
                //.WithFallback("akka.persistence.snapshot-store.azure-blob-store.container-name=" + containerName);
        }

    }
}