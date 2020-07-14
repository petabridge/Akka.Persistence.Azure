using System;
using Xunit;

namespace Akka.Persistence.Azure.Tests
{
    public class FailingSpec
    {
        [Fact]
        public void Fail()
        {
            throw new Exception("This spec should fail");
        }
    }
}