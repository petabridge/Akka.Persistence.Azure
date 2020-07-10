using Akka.Actor;
using Akka.Persistence.Azure.Util;
using FluentAssertions;
using Xunit;

namespace Akka.Persistence.Azure.Tests
{
    public class PartitionKeyEscapeHelperSpecs
    {
        [Theory]
        [InlineData("/system/sharding/areaCoordinator/singleton/coordinator")]
        [InlineData("/system/sha$rding/areaCoordinator/single$ton/coordinator$")]
        [InlineData("/system/sha$$$rding/areaCoord/inator$$/single$ton/coord$inator$")]
        public void Should_escape_correctly(string partitionKey)
        {
            var escapedKey = PartitionKeyEscapeHelper.Escape(partitionKey);
            escapedKey.Should().NotContain("/");
            var originalKey = PartitionKeyEscapeHelper.Unescape(escapedKey);
            originalKey.Should().Be(partitionKey);
        }
    }
    
    class A : ReceiveActor{ }
}