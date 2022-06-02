using System;
using Xunit;

namespace Akka.Persistence.Azure.Tests.Helper
{
    public class WindowsFactAttribute: FactAttribute
    {
        private string _skip;

        /// <inheritdoc />
        public override string Skip
        {
            get
            {
                var isUnix = Environment.OSVersion.Platform == PlatformID.Unix;
                return isUnix ? SkipUnixReason ?? "Skipped under Unix platforms" : _skip;
            }
            set => _skip = value;
        }
        
        public string SkipUnixReason { get; set; }        
    }
}