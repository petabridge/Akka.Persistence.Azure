using Akka.Configuration;
using System;
using Hocon;

namespace Akka.Persistence.Azure.Query
{
    public sealed class AzureTableStorageQuerySettings
    {
        private AzureTableStorageQuerySettings(
            string @class,
            string writePlugin,
            string maxBufferSize,
            TimeSpan refreshInterval)
        {
            Class = @class;
            MaxBufferSize = maxBufferSize;
            RefreshInterval = refreshInterval;
            WritePlugin = writePlugin;
        }

        public string Class { get; }

        public string MaxBufferSize { get; }

        public TimeSpan RefreshInterval { get; }

        public string WritePlugin { get; }

        public static AzureTableStorageQuerySettings Create(Config config)
        {
            var @class = config.GetString("class");
            var writePlugin = config.GetString("write-plugin");
            var maxBufferSize = config.GetString("max-buffer-size");
            var refreshInterval = config.GetTimeSpan("refresh-interval");

            return new AzureTableStorageQuerySettings(
                @class,
                writePlugin,
                maxBufferSize,
                refreshInterval);
        }
    }
}